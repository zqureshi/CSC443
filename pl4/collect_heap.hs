{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

module Main (main) where

import           Control.Monad         (forM_)
import           Data.Char             (ord)
import qualified Data.Text.Lazy        as T
import           Prelude               hiding (FilePath)
import           Shelly
import           Text.Shakespeare.Text (lt)

default (T.Text)

csv2heap, queryHeap, csv2denseHeap, queryDenseHeap :: FilePath

csv2heap  = "../pl2-3/csv2heapfile"
queryHeap = "../pl2-3/query"

csv2denseHeap  = "../pl2-3/csv2idxfile"
queryDenseHeap = "../pl2-3/query_idx_heapfile"

csv, heap :: T.Text
csv  = "./in.csv"
heap = "./out.heap"

heapPath :: FilePath
heapPath = "./out.heap"

pageSize :: Int
pageSize = 65536

printTimes :: FilePath -> FilePath -> Sh ()
printTimes csv2db querydb = do
    let page = T.pack (show pageSize)

    whenM (test_f heapPath) $ do
        echo_err "Removing existing heap."
        rm_f heapPath

    echo_err "Generating database."
    print_stdout False
         $ run_ csv2db [csv, heap, page]

    echo_n_err "Querying: "
    forM_ ['A'..'Z'] $ \c -> do
        echo_n_err $ T.singleton c

        let charIndex = T.pack . show $ ord c - ord 'A'
        t <-    print_stdout False
             $  T.strip
            <$> run querydb [heap, "AA", T.pack [c, 'Z'], page]
            -|- run "tail" ["-n1"]

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
main = shelly $ do
    echo "Heap file:"
    printTimes csv2heap queryHeap

    echo ""
    echo "Dense Heap:"
    printTimes csv2denseHeap queryDenseHeap

