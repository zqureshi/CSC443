module Main (main) where

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

------------------------------------------------------------------------------
-- Sources, Conduits, etc.
------------------------------------------------------------------------------

-- | A Conduit that takes incoming lists and yields all their elements.
splat :: Monad m => Conduit [a] m a
splat = awaitForever $ mapM_ yield


-- | Similar to @sourceBlocks@ except that this can operate on an existing
-- handle.
sourceBlocksHandle
    :: MonadResource m
    => Int          -- ^ Size of the buffer used to read blocks
    -> IO.Handle    -- ^ File Handle
    -> Producer m BS.ByteString
sourceBlocksHandle bsize h =
    -- Allocate the block and ensure it is freed after we are done.
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr $ \ptr ->
    loop ptr
  where
    loop ptr = do
        bs <- liftIO $ hGetBufSome h ptr bsize
        unless (BS.null bs) $
            yield bs >> loop ptr


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
    -- Open the file and ensure it is closed after we are done.
    bracketP (IO.openBinaryFile fp IO.ReadMode) IO.hClose $
    sourceBlocksHandle bsize


-- | Split incoming chunks of @ByteString@s on line breaks.
-- Empty strings will be discarded.
splitLines :: Monad m => Conduit BS.ByteString m [BS.ByteString]
splitLines = CL.map $ filter (not . BS.null) . BS.split 0x0a


-- | @sortedRunSourceHandle h start runLen bufSize@ iterates through the
-- sorted run of size @runLen@ in the file @h@ starting at offset @start@. A
-- buffer of size @bufSize@ is used to read the data.
sortedRunSource
    :: MonadResource m
    => IO.Handle   -- ^ Handle to the file
    -> Int         -- ^ Position at which the run starts
    -> Int         -- ^ Number of records in the run
    -> Int         -- ^ Size of the buffer used to read the data
    -> Source m BS.ByteString
sortedRunSource h start runLength bufSize = do
    liftIO $ IO.hSeek h IO.AbsoluteSeek (fromIntegral start)
    sourceBlocksHandle realBufSize h
        $= splitLines
        $= splat
        $= CL.isolate runLength
  where
    -- Change buffer size to the closest multiple of the recordSize that is
    -- less than the original buffer size.
    realBufSize = (bufSize `quot` recordSize) * recordSize


------------------------------------------------------------------------------
-- msort
------------------------------------------------------------------------------

-- Size of records being sorted in bytes. This includes the new line.
recordSize :: Int
recordSize = 9

-- | @makeRuns input output length@ makes runs of length @length@ in @output@.
--
-- @length@ specifies the number of records, not the number of bytes.
makeRuns :: FilePath -> FilePath -> Int -> IO ()
makeRuns inFile outFile runLength = runResourceT $
    sourceBlocks (runLength * recordSize) inFile $=
    splitLines =$= CL.map sort =$= splat         $$
    bracketP (IO.openBinaryFile outFile IO.WriteMode) IO.hClose
             (CL.mapM_ . puts)
  where
    puts h s = liftIO $ BS.hPut h s >> BS.hPut h newline
    newline = BS.singleton 0x0a

-- @mergeRuns f runLen bufSize runs@ merges runs starting at the given
-- positions into the given file.
mergeRuns :: FilePath -> Int -> Int -> [Int] -> IO ()
mergeRuns fp runLen bsize pos = undefined -- TODO
-- Algorithm:
--      buf[i] = run[i].next for all i
--      do
--          min, i = min(buf)
--          yield min
--          buf[i] = run[i].next
--      until (buf[i] = Nothing for all i)

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
        runLength           = memCapacity `quot` recordSize

    makeRuns inFile outFile runLength
