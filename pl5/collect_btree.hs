{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Main (main) where

import           Control.Monad         (forM_)
import qualified Data.Text.Lazy        as T
import           Prelude               hiding (FilePath)
import           Shelly
import           Text.Shakespeare.Text (lt)

default (T.Text)

bsort, msort, mkunsorted :: FilePath
bsort  = "./bsort"
msort  = "./msort"
mkunsorted = "./mkunsorted"

unsorted, sorted, db :: T.Text
unsorted = "unsorted.txt"
sorted   = "sorted.txt"
db = "out.db"

dbPath :: FilePath
dbPath = "out.db"

goodMemSize :: T.Text
goodMemSize = "8388608"
goodK :: T.Text
goodK = "2048"

badMemSize :: T.Text
badMemSize = "32768"
badK :: T.Text
badK = "2"

printTimes :: Sh ()
printTimes =
    forM_ ([10..22] :: [Int]) $ \count -> do
        let realCount = 2^count :: Int
            countstr = T.pack (show realCount)

        echo_n_err [lt|Record count: #{countstr}; |]

        (b, good, bad) <- print_stdout False $ do
                whenM (test_d dbPath) $ rm_rf dbPath

                escaping False $
                    cmd mkunsorted countstr (">" :: T.Text) unsorted

                echo_n_err "bsort; "
                bSort <- T.strip
                      <$> run bsort [unsorted, db]
                      -|- run "tail" ["-n1"]

                unless (isValidTimeOutput bSort) $
                    errorExit [lt|Error getting time from #{bSort}|]
                let bSortTime = getTime bSort

                echo_n_err "good msort; "
                goodMsort <- T.strip
                      <$> run msort [unsorted, sorted, goodMemSize, goodK]
                      -|- run "tail" ["-n1"]

                unless (isValidTimeOutput goodMsort) $
                    errorExit [lt|Error getting time from #{goodMsort}|]
                let goodMsortTime = getTime goodMsort

                echo_n_err "bad msort; "
                badMsort <- T.strip
                      <$> run msort [unsorted, sorted, badMemSize, badK]
                      -|- run "tail" ["-n1"]

                unless (isValidTimeOutput badMsort) $
                    errorExit [lt|Error getting time from #{badMsort}|]
                let badMsortTime = getTime badMsort

                echo_err ""

                return (bSortTime, goodMsortTime, badMsortTime)

        echo [lt|#{countstr},#{b},#{good},#{bad}|]

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

getTime :: T.Text -> T.Text
getTime = head . tail . T.words

main :: IO ()
main = shelly printTimes
