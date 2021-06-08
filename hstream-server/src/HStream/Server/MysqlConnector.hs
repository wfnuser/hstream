{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}


module HStream.Server.MysqlConnector
  (
    mysqlSinkConnector,
  )
where

import           HStream.Processing.Connector
import           HStream.Processing.Type          as HPT
import           HStream.Server.Utils
import           HStream.Store
import           HStream.Store.Internal.LogDevice
import           RIO
import qualified RIO.Map                          as M
import qualified Z.Data.CBytes                    as ZCB
import qualified Z.Data.JSON                      as JSON
import qualified Z.IO.Logger                      as Log

import           HStream.Processing.Util
import           HStream.SQL.Codegen
import           System.IO
import           System.IO.Unsafe

import           Data.Aeson
import           Data.Aeson.Types
import qualified Data.ByteString.Lazy             as L
import qualified Data.ByteString.Lazy.Char8       as C
import qualified Data.HashMap.Strict              as HM
import           Data.Scientific
import qualified Data.Text                        as T


import           Database.MySQL.Base

import           HStream.SQL.AST
import           HStream.Server.Converter

mysqlSinkConnector :: MySQLConn -> SinkConnector
mysqlSinkConnector conn = SinkConnector {
  writeRecord = writeRecordToMysql conn
}

toMysqlValue :: Value -> String
toMysqlValue (String s) = "'" ++  show s ++ "'"
toMysqlValue (Bool b) = if b then "True" else "False"
toMysqlValue (Number sci) = do
  case floatingOrInteger sci of
    Left r  -> show r
    Right i -> show i

writeRecordToMysql :: MySQLConn -> SinkRecord -> IO ()
writeRecordToMysql conn SinkRecord{..} = do
    let insertMap = (decode snkValue) :: Maybe (HM.HashMap T.Text Value)
    case insertMap of
      Just l -> do
        let flattened = flatten l
        let keys = HM.keys flattened
        let elems = HM.elems flattened
        execute_ conn $ Query . C.pack $ ("INSERT INTO " <> (T.unpack snkStream) <> " " <> keysAsSql (map T.unpack keys) <> " VALUES " <> (keysAsSql (toMysqlValue <$> elems)))
        return ()
      _ -> do
        Log.warning "Invalid SinK Value"
  where
    keysAsSql :: [String] -> String
    keysAsSql ss = '(' : helper' ss
      where
        helper' (a:b:ss) = a ++ (',':helper' (b:ss))
        helper' (a:ss)   = a ++ (helper' ss)
        helper' []       = ")"
