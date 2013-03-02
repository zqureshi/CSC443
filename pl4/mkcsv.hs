{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main (main) where

import           Control.Monad         (void, when)
import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as BS
import           System.Environment    (getArgs, getProgName)
import           System.Exit           (exitFailure)
import           System.Random

main :: IO ()
main = do
    args <- getArgs

    when (null args) $ do
        prog <- getProgName
        putStrLn $ "USAGE: " ++ prog ++ " <tuple count>"
        exitFailure
    let tupleCount = read (head args) :: Integer
    void $ times tupleCount $
        generateTuple 100 >>= BS.putStrLn . BS.intercalate ","

    return ()

choices :: ByteString
choices = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

choice :: RandomGen g => ByteString -> g -> (Char, g)
choice l g = let (i, g') = randomR (0, BS.length l -  1) g
             in (l `BS.index` i, g')

randomByteString :: forall g. RandomGen g => Int -> g -> (ByteString, g)
randomByteString len rand =
    let (bs, Just rand') = BS.unfoldrN len step rand
    in (bs, rand')
  where
    step :: g -> Maybe (Char, g)
    step g = Just $ choice choices g

generateAttr :: Int -> IO ByteString
generateAttr len = getStdRandom (randomByteString len)

generateTuple :: Integer -> IO [ByteString]
generateTuple count = times count $ generateAttr 9

times :: Monad m => Integer -> m a -> m [a]
times c m = mapM (const m) [1..c]
