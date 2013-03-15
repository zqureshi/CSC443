{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Main (main) where

import           Control.Monad         (forM_)
import qualified Data.Text.Lazy        as T
import           Prelude               hiding (FilePath)
import           Shelly
import           Text.Shakespeare.Text (lt)

default (T.Text)

msort :: FilePath
msort  = "./msort"

unsorted, sorted :: T.Text
unsorted =  "unsorted.medium.txt"
sorted = "sorted.medium.txt"

k :: Int
k = 2048

printTimes :: Sh ()
printTimes = do
    let kstr = T.pack (show k)

    echo_n_err "Sorting with mem = "
    forM_ ([15..25] :: [Int]) $ \mem -> do
        let realMem = 2 ^ mem :: Int
            memstr = T.pack (show realMem)

        echo_n_err [lt|#{memstr} |]

        t <- print_stdout False 
             $  T.strip
            <$> run msort [unsorted, sorted, memstr, kstr]
            -|- run "tail" ["-n1"]

        unless (isValidTimeOutput t) $
            errorExit [lt|Error while querying for #{memstr}: #{t}|]
        let runTime = getTime t
        echo [lt|#{memstr},#{runTime}|]
    echo_err ""

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

getTime :: T.Text -> T.Text
getTime = head . tail . T.words

main :: IO ()
main = shelly printTimes
