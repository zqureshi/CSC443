{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main (main) where

import           Control.Monad
import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
import           System.Environment    (getArgs, getProgName)
import           System.Exit           (exitFailure)
import           System.Random

attributeLength :: Int
attributeLength = 8

main :: IO ()
main = do
    args <- getArgs

    when (null args) $ do
        progName <- getProgName
        putStrLn $ "USAGE: " ++ progName ++ " <number of entries>"
        putStrLn "Pass -1 to print entries strings forever."
        exitFailure

    let count = read (head args) :: Int

    (if count > 0
     then replicateM_ count
     else forever) putRandomString
  where
    putRandomString = getStdRandom (randomByteString attributeLength)
                  >>= BS.putStrLn

randomByteString :: forall g. RandomGen g => Int -> g -> (ByteString, g)
randomByteString len rand =
    let (bs, Just rand') = BS.unfoldrN len step rand
    in (bs, rand')
  where
    step :: g -> Maybe (Char, g)
    step g = Just $ choice choices g

choices :: ByteString
choices = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

choice :: RandomGen g => ByteString -> g -> (Char, g)
choice l g = let (i, g') = randomR (0, BS.length l -  1) g
             in (l `BS.index` i, g')
