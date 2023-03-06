{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Handlers where

import           Control.Concurrent             (killThread, newEmptyMVar,
                                                 putMVar, readMVar, takeMVar,
                                                 tryPutMVar)
import           Control.Concurrent.STM         (atomically, check, dupTChan,
                                                 flushTQueue, modifyTVar,
                                                 modifyTVar', newTVarIO,
                                                 peekTQueue, readTVar,
                                                 readTVarIO, stateTVar,
                                                 tryPutTMVar, writeTQueue,
                                                 writeTVar)
import           Control.Concurrent.STM.TChan   (readTChan)
import           Control.Exception              (finally)
import           Control.Monad                  (forever, join, replicateM,
                                                 unless, void, when)
import           Data.Bifunctor                 (bimap)
import qualified Data.ByteString.Lazy           as BL
import qualified Data.IntMap.Strict             as IM
import           Data.IORef                     (newIORef, readIORef,
                                                 writeIORef)
import qualified Data.Map.Strict                as Map
import qualified Data.Vector                    as V
import qualified Proto3.Suite                   as PT
import qualified SlaveThread

import           HStream.Base                   (throwIOError)
import qualified HStream.Common.GrpcHaskell     as GRPC
import           HStream.Gossip.Gossip          (gossip)
import           HStream.Gossip.HStreamGossip   (ServerList (..))
import           HStream.Gossip.Probe           (doPing, pingReq, pingReqPing)
import           HStream.Gossip.Types           (EventMessage (EventMessage),
                                                 EventName, EventPayload,
                                                 GossipContext (..),
                                                 InitType (Gossip),
                                                 RequestAction (..),
                                                 ServerState (OK, Suspicious),
                                                 ServerStatus (..),
                                                 StateMessage (..))
import qualified HStream.Gossip.Types           as T
import           HStream.Gossip.Utils           (broadcast, broadcastMessage,
                                                 cleanStateMessages,
                                                 eventNameINIT, eventNameINITED,
                                                 getMemberListWithEpochSTM,
                                                 getMsgInc, incrementTVar,
                                                 mkGRPCClientConf,
                                                 updateLamportTime,
                                                 updateStatus)
import           HStream.Gossip.Worker          (addToServerList)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

runStateHandler :: GossipContext -> IO ()
runStateHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue statePool
    flushTQueue statePool
  unless (null newMsgs) $ do
    handleStateMessages gc $ cleanStateMessages newMsgs

handleStateMessages :: GossipContext -> [StateMessage] -> IO ()
handleStateMessages = mapM_ . handleStateMessage

handleStateMessage :: GossipContext -> StateMessage -> IO ()
handleStateMessage gc@GossipContext{..} msg@(T.GJoin node@I.ServerNode{..}) = unless (node == serverSelf) $ do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling " <> show node <> " joining cluster"
  sMap <- snd <$> readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing -> do
      addToServerList gc node msg OK False
      atomically $ do
        modifyTVar' deadServers $ Map.delete serverNodeId
        modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
      Log.info . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] " <> show node <> " has joined the cluster"
    Just ServerStatus{..} -> unless (serverInfo == node) $
      -- TODO: vote to resolve conflict
      Log.warning . Log.buildString $ "Node won't be added to the list to conflict of server id"
handleStateMessage GossipContext{..} msg@(T.GConfirm _inc node@I.ServerNode{..} _node)= do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling " <> show node <> " leaving cluster"
  sMap <- snd <$> readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing               -> pure ()
    Just ServerStatus{..} -> do
      mWorker <- atomically $ do
        modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
        writeTVar latestMessage msg
        stateTVar workers (Map.updateLookupWithKey (\_ _ -> Nothing) serverNodeId)
      case mWorker of
        Nothing -> pure ()
        Just  a -> do
          Log.info . Log.buildString $ "Stopping Worker" <> show serverNodeId
          killThread a
          Log.info . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] " <> show node <> " left cluster"
      atomically $ do
        modifyTVar' serverList $ bimap succ (Map.delete serverNodeId)
        modifyTVar' deadServers $ Map.insert serverNodeId serverInfo
handleStateMessage GossipContext{..} msg@(T.GSuspect inc node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  join . atomically $ if node == serverSelf
    then writeTQueue statePool (T.GAlive (succ inc) node serverSelf) >> return (pure ())
    else do
      sMap <- snd <$> readTVar serverList
      case Map.lookup serverNodeId sMap of
        Just ss -> do
          updated <- updateStatus ss msg Suspicious
          when updated $ modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
          return (pure ())
        Nothing -> return $ Log.debug "Suspected node not found in the server list"
          -- addToServerList gc node msg Suspicious
handleStateMessage gc@GossipContext{..} msg@(T.GAlive _inc node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  unless (node == serverSelf) $ do
    sMap <- snd <$> readTVarIO serverList
    case Map.lookup serverNodeId sMap of
      Just ss -> atomically $ do
        updated <- updateStatus ss msg OK
        when updated $ modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
      Nothing -> addToServerList gc node msg OK False
handleStateMessage _ _ = throwIOError "illegal state message"

runEventHandler :: GossipContext -> IO ()
runEventHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue eventPool
    flushTQueue eventPool
  unless (null newMsgs) $ do
    handleEventMessages gc newMsgs

handleEventMessages :: GossipContext -> [EventMessage] -> IO ()
handleEventMessages = mapM_ . handleEventMessage

handleEventMessage :: GossipContext -> EventMessage -> IO ()
handleEventMessage gc@GossipContext{..} msg@(EventMessage eName lpTime bs) = do
  Log.debug . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
                            <> "] Received Custom Event" <> show eName <> " with lamport " <> show lpTime
  join . atomically $ do
    currentTime <- fromIntegral <$> updateLamportTime eventLpTime lpTime
    seen <- readTVar seenEvents
    -- FIXME: max length should be a setting for seen buffer
    let len = max 10 (IM.size seen)
        lpInt = fromIntegral lpTime
    if currentTime > len && lpInt < currentTime - len then return $ pure ()
      else case IM.lookup lpInt seen of
        Nothing -> handleNewEvent lpInt
        Just events -> if event `elem` events
          then return $ pure ()
          else handleNewEvent lpInt
   where
     event = (eName, bs)
     handleNewEvent lpInt = do
        modifyTVar seenEvents $ IM.insertWith (++) lpInt [event]
        modifyTVar broadcastPool $ broadcastMessage (T.GEvent msg)
        return $ case Map.lookup eName eventHandlers of
          Nothing     -> if eName == eventNameINIT
            then do
              Log.info . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
                                        <> "] Handling Internal Event" <> show eName <> " with lamport " <> show lpInt
              (isSeed, _, wasIDead) <- readMVar seedsInfo
              when (isSeed && not wasIDead) $ handleINITEvent gc bs
            else Log.info $ "Action dealing with event " <> Log.buildString' eName <> " not found"
          Just action -> do
            action bs

handleINITEvent :: GossipContext -> EventPayload -> IO ()
handleINITEvent gc@GossipContext{..} payload = do
  case PT.fromByteString payload of
    Left err -> Log.warning $ Log.buildString' err
    Right ServerList{..} -> do
      initGossip gc $ V.toList serverListNodes
      void $ tryPutMVar clusterInited Gossip
      atomically $ do
        mWorkers <- readTVar workers
        check $ (Map.size mWorkers + 1) == length seeds
      broadCastUserEvent gc eventNameINITED (BL.toStrict $ PT.toLazyByteString serverSelf)

broadCastUserEvent :: GossipContext -> EventName -> EventPayload -> IO ()
broadCastUserEvent gc@GossipContext {..} userEventName userEventPayload= do
  lpTime <- atomically $ incrementTVar eventLpTime
  let eventMessage = EventMessage userEventName lpTime userEventPayload
  handleEventMessage gc eventMessage

initGossip :: GossipContext -> [I.ServerNode] -> IO ()
initGossip gc = mapM_ (\x -> addToServerList gc x (T.GJoin x) OK True)
