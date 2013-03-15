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

pageSize :: Int
pageSize = 10485760 -- 10 MB

printTimes :: Sh ()
printTimes = do
    let page = T.pack (show pageSize)

    echo_n_err "Sorting with k = "
    forM_ ([17..20] :: [Int]) $ \k -> do
        let realK = 2 ^ k :: Int
            kstr = T.pack (show realK)

        echo_n_err [lt|#{kstr} |]

        t <- print_stdout False 
             $  T.strip
            <$> run msort [unsorted, sorted, page, kstr]
            -|- run "tail" ["-n1"]

        unless (isValidTimeOutput t) $
            errorExit [lt|Error while querying for #{kstr}: #{t}|]
        let runTime = getTime t
        echo [lt|#{kstr},#{runTime}|]
    echo_err ""

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

getTime :: T.Text -> T.Text
getTime = head . tail . T.words

main :: IO ()
main = shelly printTimes
