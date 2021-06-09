{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Connector.Mysql
  ( mysqlSinkConnector
  ) where

import           Control.Monad                   (join, void)
import qualified Data.Aeson                      as Aeson
import           Data.Bifunctor                  (first)
import qualified Data.ByteString.Lazy.Char8      as DBCL
import qualified Data.HashMap.Strict             as HM
import           Data.List                       (intercalate)
import           Data.Scientific                 (floatingOrInteger)
import           Data.Text                       (Text)
import qualified Data.Text                       as Text
import           Database.MySQL.Base             as MY

import qualified Z.IO.Logger                     as Log

import           HStream.Processing.Connector    (SinkConnector (..))
import           HStream.Processing.Type         (SinkRecord (..))

mysqlSinkConnector :: MY.MySQLConn -> SinkConnector
mysqlSinkConnector myClient =
  SinkConnector { writeRecord = writeRecordToMysql myClient }

toMysqlValue :: Aeson.Value -> String
toMysqlValue (Aeson.String s) = "'" ++  show s ++ "'"
toMysqlValue (Aeson.Bool b) = if b then "True" else "False"
toMysqlValue (Aeson.Number sci) = do
  case floatingOrInteger sci of
    Left r  -> show r
    Right i -> show i
toMysqlValue _ = error "Not implemented"

writeRecordToMysql :: MY.MySQLConn -> SinkRecord -> IO ()
writeRecordToMysql myClient SinkRecord{..} = do
  let insertMap = Aeson.decode snkValue :: Maybe (HM.HashMap Text.Text Aeson.Value)
  case insertMap of
    Just l -> do
      let !flattened = flatten l
      let keys = "(" <> (intercalate "," . map Text.unpack $ HM.keys flattened) <> ")"
          elems = "(" <> (intercalate "," . map toMysqlValue $ HM.elems flattened) <> ")" 
      void $ execute_ myClient $ Query $ DBCL.pack ("INSERT INTO " ++ show snkStream ++ " " ++ keys ++ " VALUES " ++ elems)
    _ -> do
      Log.warning "Invalid Sink Value"

-- | Flatten all JSON structures.
--
-- >>> flatten (HM.fromList [("a", Aeson.Object $ HM.fromList [("b", Aeson.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flatten :: HM.HashMap Text Aeson.Value -> HM.HashMap Text Aeson.Value
flatten jsonMap =
  let flattened = join $ map (flatten' "." Text.empty) (HM.toList jsonMap)
   in HM.fromList $ map (first Text.tail) flattened

flatten' :: Text -> Text -> (Text, Aeson.Value) -> [(Text, Aeson.Value)]
flatten' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    Aeson.Object o -> join $ map (flatten' splitor (prefix <> splitor <> k)) (HM.toList o)
    _              -> [(prefix <> splitor <> k, v)]
