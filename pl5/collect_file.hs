{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Main (main) where

import           Control.Monad         (forM_)
import qualified Data.Text.Lazy        as T
import           Prelude               hiding (FilePath)
import           Shelly
import           Text.Shakespeare.Text (lt)

default (T.Text)

msort, mkunsorted :: FilePath
msort  = "./msort"
mkunsorted = "./mkunsorted"

memSize :: Int
memSize = 8388608

unsorted, sorted :: T.Text
unsorted = "unsorted.txt"
sorted   = "sorted.txt"

k :: Int
k = 2048

printTimes :: Sh ()
printTimes = do
    let kstr = T.pack (show k)
        memstr = T.pack (show memSize)

    echo_n_err "Sorting file with records = "
    forM_ ([10..22] :: [Int]) $ \count -> do
        let realCount = 2^count :: Int
            countstr = T.pack (show realCount)

        echo_n_err [lt|#{countstr} |]

        t <- print_stdout False $ do
                escaping False $
                    cmd mkunsorted countstr (">" :: T.Text) unsorted

                T.strip
                  <$> run msort [unsorted, sorted, memstr, kstr]
                  -|- run "tail" ["-n1"]
            
        unless (isValidTimeOutput t) $
            errorExit [lt|Error while querying for #{memstr}: #{t}|]
        let runTime = getTime t
        echo [lt|#{countstr},#{runTime}|]
    echo_err ""

isValidTimeOutput :: T.Text -> Bool
isValidTimeOutput t = "TIME:" `T.isPrefixOf` t
                   && "milliseconds" `T.isSuffixOf` t

getTime :: T.Text -> T.Text
getTime = head . tail . T.words

main :: IO ()
main = shelly printTimes
