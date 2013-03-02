{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
module Main (main) where

import           Control.Monad         (forM_)
import           Data.Char             (ord)
import qualified Data.Text.Lazy        as T
import           Prelude               hiding (FilePath)
import           Shelly
import           Text.Shakespeare.Text (lt)

csv2levelDB, queryLevelDB, csv2indexLevelDB, queryIndexLevelDB :: FilePath
csv2levelDB = "./csv2leveldb"
queryLevelDB = "./query_leveldb"
csv2indexLevelDB = "./csv2idx_leveldb"
queryIndexLevelDB = "./query_idx_leveldb"

csv, db :: T.Text
csv = "in.csv"
db = "./out.db"

dbPath :: FilePath
dbPath = "./out.db"

printTimes :: FilePath -> FilePath -> Sh ()
printTimes csv2db querydb = do
    whenM (test_d dbPath) $ do
        echo_err "Removing existing database."
        rm_rf dbPath

    echo_err "Generating database."
    print_stdout False $ run_ csv2db [csv, db]

    echo_n_err "Querying: "
    forM_ ['A'..'Z'] $ \c -> do
        echo_n_err $ T.singleton c
        let charIndex = T.pack . show $ ord c - ord 'A'
        t <- print_stdout False $
             T.strip <$>
             run querydb [db, "AA", T.pack [c, 'Z']] -|-
             run "tail" ["-n1"]
        unless (isValidTimeOutput t) $
            errorExit [lt|Error while querying for #{T.singleton c}: #{t}|]
        let runTime = head . tail . T.words $ t
        echo [lt|#{charIndex},#{runTime}|]

    echo_err ""

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

main :: IO ()
main = shelly $ do
    echo "LevelDB Database:"
    printTimes csv2levelDB queryLevelDB

    echo ""
    echo "LevelDB Index:"
    printTimes csv2indexLevelDB queryIndexLevelDB

