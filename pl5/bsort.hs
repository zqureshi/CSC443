{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import           Control.Applicative      ((<$>))
import           Control.Monad
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Conduit
import qualified Data.Conduit.List        as CL
import           Data.Default             (def)
import           Data.Time.Clock.POSIX    (getPOSIXTime)
import           Data.Word                (Word8)
import qualified Database.LevelDB         as LDB
import           Foreign.ForeignPtr       (ForeignPtr, finalizeForeignPtr,
                                           withForeignPtr)
import           Prelude                  hiding (lines)
import           System.Environment       (getArgs, getProgName)
import           System.Exit              (exitFailure)
import qualified System.IO                as IO
import           Text.Printf              (printf)

------------------------------------------------------------------------------
-- I/O
------------------------------------------------------------------------------

-- | Similar to @System.IO.hGetBufSome@. Reads a ByteString into an existing
-- buffer and returns a ByteString referencing that buffer.
hGetBufSome :: IO.Handle -> ForeignPtr Word8 -> Int -> IO BS.ByteString
hGetBufSome handle ptr count = do
    bytesRead <- withForeignPtr ptr $ \p ->
                 IO.hGetBufSome handle p count
    return $! BSI.PS ptr 0 bytesRead -- <- Unsafe

-- | Stream the contents of the given file as binary data. Use the given block
-- size.
--
-- This is similar to @Conduit.Binary.sourceFile@ except that the same block
-- of memory is re-used. So, only one block is accessible at a time. Trying to
-- group multiple blocks returned by this without copying them will only
-- result in pain and misery.
sourceFile :: MonadResource m => Int -> FilePath -> Producer m BS.ByteString
sourceFile bsize fp =
    -- Open the file and allocate the block. Ensure they're both freed once
    -- we're done here.
    -- Open the file and ensure it is closed after we are done.
    bracketP (IO.openBinaryFile fp IO.ReadMode) IO.hClose $
    sourceFileHandle bsize

-- | Similar to @sourceFile@ except that this can operate on an existing
-- handle.
sourceFileHandle
    :: MonadResource m
    => Int          -- ^ Size of the buffer used to read blocks
    -> IO.Handle    -- ^ File Handle
    -> Producer m BS.ByteString
sourceFileHandle bsize h =
    -- Allocate the block and ensure it is freed after we are done.
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr $ \ptr ->
    loop ptr
  where
    loop ptr = do
        bs <- liftIO $ hGetBufSome h ptr bsize
        unless (BS.null bs) $
            yield bs >> loop ptr

-- | Split incoming chunks of @ByteString@s on line breaks. Empty strings will
-- be discarded.
lines :: Monad m => Conduit BS.ByteString m [BS.ByteString]
lines = CL.map $ filter (not . BS.null) . BS.split 0x0a

-- | A sink that will simply add all bytestrings to the given LevelDB database
-- as keys with empty values.
levelDBIndexSink
    :: MonadResource m
    => LDB.DB
    -> Sink BS.ByteString m ()
levelDBIndexSink db = awaitForever $ \bs -> LDB.put db def bs ""

-- | A Conduit that takes incoming lists and yields all their elements.
splat :: Monad m => Conduit [a] m a
splat = awaitForever $ mapM_ yield

-- | Get the current system time in milliseconds.
now :: IO Int
now = truncate . (* 1000) <$> getPOSIXTime

recordSize :: Int
recordSize = 9

main :: IO ()
main = do
    args <- getArgs

    when (length args < 2) $ do
        progName <- getProgName
        putStrLn $ "USAGE: " ++ progName ++ " <input file> <out index> [count]"
        exitFailure

    let inputFile = head args
        outIndex  = args !! 1
        bufSize = if length args > 2
                  then read (args !! 2) * recordSize
                  else 500 * recordSize

    startTime <- now

    runResourceT $ do
        db <- LDB.open outIndex
                       (LDB.defaultOptions { LDB.createIfMissing = True
                                           , LDB.errorIfExists   = True })
        sourceFile bufSize inputFile
            $= lines $= splat
            $$ levelDBIndexSink db

    endTime <- now

    putStrLn $ printf "TIME: %d milliseconds" (endTime - startTime)
