{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}

module HStream.SQL.Codegen.V1New.Boilerplate where

#ifdef HStreamEnableSchema
import           Data.Aeson
import qualified Data.Aeson                            as Aeson
import qualified Data.Binary                           as B
import           Data.Binary.Get
import qualified Data.ByteString.Builder               as BB
import qualified Data.ByteString.Lazy                  as BL
import qualified Data.HashMap.Strict                   as HM
import qualified Data.IntMap                           as IntMap
import           Data.Maybe                            (fromJust)
import           Data.Scientific                       (Scientific (coefficient),
                                                        coefficient, scientific)
import qualified Data.Text                             as T
import qualified Data.Text.Lazy                        as TL
import qualified Data.Text.Lazy.Encoding               as TLE
import qualified Data.Time                             as Time
import qualified Data.Time.Clock.POSIX                 as Time
import           Data.Time.Format.ISO8601              (iso8601ParseM,
                                                        iso8601Show)
import           HStream.Processing.Encoding
import           HStream.Processing.Stream.TimeWindows
import           HStream.SQL.Binder
import           HStream.SQL.Exception
import           HStream.SQL.Rts
import qualified HStream.Utils.Aeson                   as HsAeson
import           RIO                                   (Int64, Void)

instance Serialized FlowObject where
  compose (fo1, fo2) =
    let schema = extractFlowObjectSchema (fo1 <> fo2)
        o = compose ( flowObjectToJsonObject fo1
                    , flowObjectToJsonObject fo2
                    )
     in jsonObjectToFlowObject schema o
  separate fo =
    let (o1, o2) = separate (flowObjectToJsonObject fo)
        schema1 = extractJsonObjectSchema o1
        schema2 = extractJsonObjectSchema o2
     in ( jsonObjectToFlowObject schema1 o1
        , jsonObjectToFlowObject schema2 o2
        )

textSerde :: Serde TL.Text BL.ByteString
textSerde =
  Serde
  { serializer   = Serializer   TLE.encodeUtf8
  , deserializer = Deserializer TLE.decodeUtf8
  }

objectSerde :: Serde Object BL.ByteString
objectSerde =
  Serde
  { serializer   = Serializer   encode
  , deserializer = Deserializer $ fromJust . decode
  }

flowObjectSerde :: Serde FlowObject BL.ByteString
flowObjectSerde =
  Serde
  { serializer = Serializer encode
  , deserializer = Deserializer $ fromJust . decode
  }

intSerde :: Serde Int BL.ByteString
intSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }

voidSerde :: Serde Void BL.ByteString
voidSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }

objectObjectSerde :: Serde Object Object
objectObjectSerde =
  Serde
  { serializer = Serializer id
  , deserializer = Deserializer id
  }

flowObjectFlowObjectSerde :: Serde FlowObject FlowObject
flowObjectFlowObjectSerde =
  Serde
  { serializer = Serializer id
  , deserializer = Deserializer id
  }

timeWindowSerde :: Int64 -> Serde TimeWindow BL.ByteString
timeWindowSerde windowSize =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStartBuilder = BB.int64BE tWindowStart
          blankBuilder = BB.int64BE 0
       in BB.toLazyByteString $ winStartBuilder <> blankBuilder
  , deserializer = Deserializer $ runGet decodeTimeWindow
  }
  where
    decodeTimeWindow = do
      startTs <- getInt64be
      _       <- getInt64be
      return TimeWindow {tWindowStart = startTs, tWindowEnd = startTs + windowSize}

sessionWindowSerde :: Serde TimeWindow BL.ByteString
sessionWindowSerde =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStartBuilder = BB.int64BE tWindowStart
          winEndBuilder   = BB.int64BE tWindowEnd
       in BB.toLazyByteString $ winStartBuilder <> winEndBuilder
  , deserializer = Deserializer $ runGet decodeTimeWindow
  }
  where
    decodeTimeWindow = do
      startTs <- getInt64be
      endTs   <- getInt64be
      return TimeWindow {tWindowStart = startTs, tWindowEnd = endTs}

timeWindowObjectSerde :: Serde TimeWindow Object
timeWindowObjectSerde =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let startTime = Time.utcToZonedTime Time.utc (Time.posixSecondsToUTCTime $ realToFrac (fromIntegral tWindowStart * 0.001))
          endTime   = Time.utcToZonedTime Time.utc (Time.posixSecondsToUTCTime $ realToFrac (fromIntegral tWindowEnd   * 0.001))
          winStart  = [( HsAeson.fromText winStartText
                       , Aeson.String (T.pack $ iso8601Show startTime)
                       )]
          winEnd    = [( HsAeson.fromText winEndText
                       , Aeson.String (T.pack $ iso8601Show endTime)
                       )]
       in HsAeson.fromList $ winStart ++ winEnd
  , deserializer = Deserializer $ \obj ->
      case do
        Aeson.String s1 <- HsAeson.lookup (HsAeson.fromText winStartText) obj
        Aeson.String s2 <- HsAeson.lookup (HsAeson.fromText winEndText  ) obj
        (tsStart :: Time.ZonedTime) <- iso8601ParseM (T.unpack s1)
        (tsEnd   :: Time.ZonedTime) <- iso8601ParseM (T.unpack s2)
        let startTime = fromIntegral . floor . (1000 *) . Time.utcTimeToPOSIXSeconds . Time.zonedTimeToUTC $ tsStart
            endTime   = fromIntegral . floor . (1000 *) . Time.utcTimeToPOSIXSeconds . Time.zonedTimeToUTC $ tsEnd
        return (startTime, endTime) of
         Nothing -> throwSQLException CodegenException Nothing ("Error when deserializing timewindow " <> show obj)
         Just (startTime, endTime) -> TimeWindow { tWindowStart = startTime
                                                 , tWindowEnd   = endTime
                                                 }
  }

timeWindowFlowObjectSerde :: Serde TimeWindow FlowObject
timeWindowFlowObjectSerde =
  Serde
  { serializer = Serializer $ \tw -> (jsonObjectToFlowObject timewindowSchema) $ (runSer . serializer $ timeWindowObjectSerde) tw
  , deserializer = Deserializer $ \fo -> (runDeser . deserializer $ timeWindowObjectSerde) (flowObjectToJsonObject fo)
  }

sessionWindowFlowObjectSerde :: Serde TimeWindow FlowObject
sessionWindowFlowObjectSerde =
  Serde
  { serializer = Serializer $ \tw -> (jsonObjectToFlowObject timewindowSchema) $ (runSer . serializer $ timeWindowObjectSerde) tw
  , deserializer = Deserializer $ \fo -> (runDeser . deserializer $ timeWindowObjectSerde) (flowObjectToJsonObject fo)
  }

timewindowSchema :: Schema
timewindowSchema = Schema
  { schemaOwner   = ""
  , schemaColumns =
      IntMap.fromList [ (0, ColumnCatalog { columnId = 0
                                          , columnName = "window_start"
                                          , columnStreamId = 0
                                          , columnStream = ""
                                          , columnType = BTypeTimestamp
                                          , columnIsNullable = True
                                          , columnIsHidden = True
                                          }
                        )
                      , (1, ColumnCatalog { columnId = 1
                                          , columnName = "window_end"
                                          , columnStreamId = 0
                                          , columnStream = ""
                                          , columnType = BTypeTimestamp
                                          , columnIsNullable = True
                                          , columnIsHidden = True
                                          }
                        )
                      ]
  }
#endif
