module Main where

import           Control.Monad
import           Control.Monad.IO.Class   (liftIO)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Conduit
import qualified Data.Conduit.List        as CL
import           Data.List                (sort)
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
-- re-used. So, only one block is accessible at a time. Trying to group
-- multiple blocks returned by this without copying them will only result in
-- pain and misery.
sourceBlocks
    :: MonadResource m
    => Int              -- ^ Size of the buffer used while reading the file.
    -> FilePath         -- ^ Path to the file.
    -> Producer m BS.ByteString
sourceBlocks bsize fp =
    -- Open the file and allocate the block. Ensure they're both freed once
    -- we're done here.
    bracketP (IO.openBinaryFile fp IO.ReadMode) IO.hClose $ \h ->
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr $ \ptr ->
    loop h ptr
  where
    loop h ptr = do
        bs <- liftIO $ hGetBufSome h ptr bsize
        unless (BS.null bs) $
            yield bs >> loop h ptr

-- | Split incoming chunks of @ByteString@s on line breaks.
-- Empty strings will be discarded.
splitLines :: Monad m => Conduit BS.ByteString m [BS.ByteString]
splitLines = CL.map $ filter (not . BS.null) . BS.split 0x0a

-- | Write lists of byte strings out to the given file, adding a new line
-- after each.
writeLines :: MonadResource m => FilePath -> Sink [BS.ByteString] m ()
writeLines fp = bracketP (IO.openBinaryFile fp IO.WriteMode) IO.hClose
              $ CL.mapM_ . mapM_ . puts
  where
    puts h s = liftIO $ BS.hPut h s >> BS.hPut h newline
    newline = BS.singleton 0x0a

-- Size of records being sorted in bytes. This includes the new line.
recordSize :: Int
recordSize = 9

-- | @makeRuns input output length@ makes runs of length @length@ in @output@.
--
-- @length@ specifies the number of records, not the number of bytes.
makeRuns :: FilePath -> FilePath -> Int -> IO ()
makeRuns inFile outFile runLength = runResourceT    $
       sourceBlocks (runLength * recordSize) inFile $=
       splitLines =$= awaitForever (yield . sort)   $$
       writeLines outFile

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

    makeRuns inFile outFile (memCapacity `quot` recordSize)
