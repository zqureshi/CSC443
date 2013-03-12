{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Main (main) where

import           Control.Applicative       ((<$>))
import           Control.Monad
import           Control.Monad.IO.Class    (liftIO)
import           Control.Monad.Trans.Class (lift)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Internal  as BSI
import           Data.Conduit
import qualified Data.Conduit.List         as CL
import           Data.Function             (on)
import qualified Data.List                 as List
import           Data.Maybe                (fromJust, isNothing)
import qualified Data.Vector               as V
import           Data.Word                 (Word8)
import           Foreign.ForeignPtr        (ForeignPtr, finalizeForeignPtr,
                                            withForeignPtr)
import           Prelude                   hiding (lines)
import           System.Environment        (getArgs, getProgName)
import           System.Exit               (exitFailure)
import qualified System.IO                 as IO

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


-- | Split incoming chunks of @ByteString@s on line breaks. Empty strings will
-- be discarded.
lines :: Monad m => Conduit BS.ByteString m [BS.ByteString]
lines = CL.map $ filter (not . BS.null) . BS.split 0x0a


-- | @sourceSortedRun h start runLen bufSize@ iterates through the sorted run
-- of size @runLen@ in the file @h@ starting at offset @start@. A buffer of
-- size @bufSize@ is used to read the data.
sourceSortedRun
    :: MonadResource m
    => IO.Handle   -- ^ Handle to the file
    -> Int         -- ^ Position at which the run starts
    -> Int         -- ^ Number of records in the run
    -> Int         -- ^ Size of the buffer used to read the data
    -> Source m BS.ByteString
sourceSortedRun h start runLength bufSize = do
    liftIO $ IO.hSeek h IO.AbsoluteSeek (fromIntegral start)
    sourceFileHandle realBufSize h
        $= lines
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
-- @length@ specifies the number of records, not the number of bytes.
makeRuns :: FilePath -> FilePath -> Int -> IO ()
makeRuns inFile outFile runLength = runResourceT $
    sourceFile blockSize inFile $=
    lines $= CL.map List.sort   $= splat $$
    bracketP openFile IO.hClose (CL.mapM_ . puts)
  where
    -- Number of bytes consumed by @runLength@ records.
    blockSize = runLength * recordSize
    openFile = IO.openBinaryFile outFile IO.WriteMode
    -- Write the given bytestring to the handle and add a newline.
    puts h s = liftIO $ BS.hPut h s >> BS.hPut h newline
    newline = BS.singleton 0x0a

-- @mergeRuns fin fout runLen bufSize runs@ merges runs starting at the given
-- positions in @fin@ into @fout@.
mergeRuns
    :: forall m. MonadResource m
    => FilePath
    -> Int
    -> Int
    -> [Int]
    -> Source m BS.ByteString
mergeRuns fin runLen bsize pos =
    bracketP (IO.openBinaryFile fin IO.ReadMode) IO.hClose $ \h -> do
    let srcs = map (\p -> sourceSortedRun h p runLen bsize) pos
    (srcs, vals) <- lift $ unzip <$> mapM ($$+ CL.head) srcs
    loop (V.fromList $ zip3 ([0..] :: [Int]) (map Just srcs) vals)
  where
    cmp Nothing Nothing   = EQ
    cmp Nothing _         = GT
    cmp _       Nothing   = LT
    cmp (Just a) (Just b) = a `compare` b

    thrd (_, _, a) = a

    loop
        :: V.Vector (Int, Maybe (ResumableSource m BS.ByteString),
                     Maybe BS.ByteString)
        -> Source m BS.ByteString
    loop l = do
        closedSources <- filter (/= -1) . V.toList <$> V.mapM closeExpired l
        let (idx, src, val) = V.minimumBy (cmp `on` thrd) l
        unless (isNothing val) $ do
            yield (fromJust val)
            (newsrc, newval) <- lift (fromJust src $$++ CL.head)
            let updates = map (\i -> (i, (i, Nothing, Nothing))) closedSources
                newl = l V.// ((idx, (idx, Just newsrc, newval)):updates)
            loop newl

    -- Close expired resumable streams. Output @Nothing@ if a stream was
    -- closed, or the stream otherwise.
    closeExpired (i, Just r, Nothing) = lift (r $$+- CL.sinkNull)
                                     >> return i
    closeExpired _ = return (-1)

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
