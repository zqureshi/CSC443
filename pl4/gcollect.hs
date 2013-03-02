{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE QuasiQuotes          #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module Main (main) where

import           Control.Monad         (forM_, void)
import           Data.Char             (ord)
import qualified Data.Text.Lazy        as T
import qualified Data.Text.Lazy.IO     as TIO
import           Prelude               hiding (FilePath)
import           Shelly
import           System.Environment    (getArgs, getProgName)
import           System.Exit           (exitFailure)
import           Text.Shakespeare.Text (lt)

default (T.Text)

printTimes :: FilePath -> FilePath -> Sh ()
printTimes csv2db querydb = do
    echo_err "Generating database."
    void $ print_stdout False
         $ escaping False
         $ cmd csv2db

    echo_n_err "Querying: "
    forM_ ['A'..'Z'] $ \c -> do
        echo_n_err $ T.singleton c

        let charIndex = T.pack . show $ ord c - ord 'A'
        t <-    print_stdout False
             $  escaping False
             $  T.strip
            <$> cmd querydb "AA" (T.pack [c, 'Z']) -|- run "tail" ["-n1"]

        unless (isValidTimeOutput t) $
            errorExit [lt|Error while querying for #{T.singleton c}: #{t}|]
        let runTime = getTime t
        echo [lt|#{charIndex},#{runTime}|]

    echo_err ""

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

getTime :: T.Text -> T.Text
getTime = head . tail . T.words

main :: IO ()
main = do
    args <- map T.pack <$> getArgs
    unless (length args == 2) $ do
        progName <- T.pack <$> getProgName
        TIO.putStrLn [lt|USAGE: #{progName} <csv2db command> <querydb command>|]
        exitFailure

    let csv2db = fromText $ head args
        querydb = fromText $ args !! 1
    shelly $ printTimes csv2db querydb
