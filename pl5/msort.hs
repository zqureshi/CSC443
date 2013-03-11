module Main where

import           Control.Monad
import           Control.Monad.IO.Class   (liftIO)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Char                (ord)
import           Data.Conduit
import           Data.Word                (Word8)
import           Foreign.ForeignPtr       (ForeignPtr, finalizeForeignPtr,
                                           withForeignPtr)
import           Prelude                  hiding (lines)
import           System.Environment       (getArgs, getProgName)
import           System.Exit              (exitFailure)
import qualified System.IO                as IO

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
-- This is similar to @sourceFile@ except that the same block of memory is
-- re-used. So, only one block is accessible at a time. Trying to group the
-- bytestrings will result in nothing but pain and misery.
sourceBlocks
    :: MonadResource m
    => Int              -- ^ Size of the buffer used while reading the file.
    -> FilePath         -- ^ Path to the file.
    -> Producer m BS.ByteString
sourceBlocks bsize fp =
    -- The following two lines open the file and allocate the block.
    -- @bracketP@ ensures that these resources will be freed as soon as their
    -- use is finished.
    bracketP (IO.openBinaryFile fp IO.ReadMode) IO.hClose $ \h ->
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr $ \ptr ->
    loop h ptr
  where
    loop h ptr = do
        bs <- liftIO $ hGetBufSome h ptr bsize
        unless (BS.null bs) $
            yield bs >> loop h ptr

-- | Split incoming bytestrings at newline. If a bytestring does not contain a
-- newline, this will yield it anyway.
--
-- So, this really only works correctly if block sizes are aligned with line
-- sizes. That is, if the block size is a multiple of line size (incl.
-- newline).
lines :: Monad m => Conduit BS.ByteString m BS.ByteString
lines = awaitForever $ \bs ->
    case BS.elemIndex newline bs of
        Nothing -> yield bs
        Just i  -> yield (BS.take (i-1) bs)
                >> yield (BS.drop i bs)
  where newline = fromIntegral (ord '\n')


-- | @makeRuns input output length@ makes runs of length @length@ in @output@.
makeRuns :: FilePath -> FilePath -> Integer -> IO ()
makeRuns = undefined

main :: IO ()
main = do
    args <- getArgs

    when (length args /= 4) $ do
        progName <- getProgName
        putStrLn $ progName ++
                   " <input file> <output file> <memory capacity> <k>"
        exitFailure

    let inFile:outFile:rest = args
        memCapacity         = read (head rest) :: Int
        k                   = read (last rest) :: Int

    runResourceT
        $  sourceBlocks memCapacity inFile
        $= lines
        $$ awaitForever (liftIO . BS.hPut IO.stdout)
