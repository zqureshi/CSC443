{-# LANGUAGE TupleSections #-}
module Main (main) where

import           Control.Arrow                (first)
import           Control.Monad
import           Control.Monad.IO.Class       (MonadIO (liftIO))
import           Control.Monad.Trans.Class    (lift)
import           Control.Monad.Trans.Resource as Res
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Internal     as BSI
import           Data.Conduit
import qualified Data.Conduit.List            as CL
import           Data.Function                (on)
import qualified Data.List                    as List
import           Data.Maybe                   (fromJust, isNothing)
import qualified Data.Vector                  as V
import           Data.Word                    (Word8)
import           Foreign.ForeignPtr           (ForeignPtr, finalizeForeignPtr,
                                               withForeignPtr)
import           Prelude                      hiding (lines)
import           System.Environment           (getArgs, getProgName)
import           System.Exit                  (exitFailure)
import qualified System.IO                    as IO

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
    -> Integer     -- ^ Position at which the run starts
    -> Int         -- ^ Number of records in the run
    -> Int         -- ^ Size of the buffer used to read the data
    -> Source m BS.ByteString
sourceSortedRun h start runLength bufSize = do
    liftIO $ IO.hSeek h IO.AbsoluteSeek start
    sourceFileHandle realBufSize h
        $= lines
        $= splat
        $= CL.isolate runLength
  where
    -- Change buffer size to the closest multiple of the recordSize that is
    -- less than the original buffer size.
    realBufSize = (bufSize `quot` recordSize) * recordSize

-- | A conduit that writes lists of ByteStrings separated by newlines to the
-- given file and outputs the offsets in the file at which each block was
-- written.
fileWriteBlock
    :: MonadResource m
    => FilePath
    -> Conduit [BS.ByteString] m Integer
fileWriteBlock fp =
    bracketP (IO.openBinaryFile fp IO.WriteMode) IO.hClose handleWriteBlock

-- | Version of @fileWriteBlock@ that writes to an existing Handle.
handleWriteBlock
    :: MonadIO m
    => IO.Handle
    -> Conduit [BS.ByteString] m Integer
handleWriteBlock h = awaitForever $ \bs -> do
    pos <- liftIO $ IO.hTell h
    mapM_ putLn bs
    yield pos
  where
    newline = BS.singleton 0x0a
    putLn s = liftIO $ BS.hPut h s >> BS.hPut h newline


-- | Group @n@ elements from upstream and send the list downstream.
group :: Monad m => Int -> Conduit a m [a]
group n = loop
    where loop = do l <- CL.take n
                    unless (null l) $ yield l >> loop


------------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------------

-- Return the minimum value and its index.
imin :: (a -> a -> Ordering) -> V.Vector a -> (Int, a)
imin cmpr vec = V.ifoldr' cmp' (0, vec V.! 0) vec
    where cmp' ai a (bi, b) = case a `cmpr` b of
                                LT -> (ai, a)
                                _  -> (bi, b)

------------------------------------------------------------------------------
-- msort
------------------------------------------------------------------------------

-- Size of records being sorted in bytes. This includes the new line.
recordSize :: Int
recordSize = 9

-- | @makeRuns input output length@ makes runs of length @length@ in @output@.
-- @length@ specifies the number of records, not the number of bytes.
--
-- This is pass 0 of the external merge sort algorithm.
makeRuns
    :: MonadResource m
    => FilePath     -- ^ Path to the file containing the unsorted data
    -> FilePath     -- ^ Target file to which sorted runs will be written
    -> Int          -- ^ Number of records in each sorted run
    -> Source m Integer
makeRuns inFile outFile runLength = sourceFile blockSize inFile
                                 $= lines $= CL.map List.sort
                                 $= fileWriteBlock outFile
  -- Number of bytes consumed by @runLength@ records.
  where blockSize = runLength * recordSize

-- @mergeRuns fin runLength bufSize positions@ merges K runs (where K is the
-- length of @positions@) of @runLength@ records each using a @bufSize@ byte
-- buffer.
--
-- The file is assumed to be @runLen@-sorted. The output will be @runLen * K@
-- sorted.
--
-- Total memory used by buffers will be @K * bufSize@.
mergeRuns
    :: MonadResource m
    => FilePath     -- ^ Path to the file containing the sorted runs
    -> Int          -- ^ Number of records in each sorted run
    -> Int          -- ^ Buffer size to use to read from each sorted run
    -> [Integer]    -- ^ List of starting positions of each run
    -> Source m BS.ByteString
mergeRuns fin runLen bsize positions =
    bracketP (IO.openBinaryFile fin IO.ReadMode) IO.hClose $ \h -> do
    let sources = map (runIterator h) positions
    sourcesAndValues <- lift $! mapM ($$+ CL.head) sources
    loop (V.fromList $ map (first Just) sourcesAndValues)
  where
    -- Source that yields elements of the given run.
    runIterator h pos = sourceSortedRun h pos runLen bsize

    cmp Nothing Nothing   = EQ
    cmp Nothing _         = GT
    cmp _       Nothing   = LT
    cmp (Just a) (Just b) = a `compare` b

    loop l = do
        closedSources <- V.foldM' closeExpired [] (V.indexed l)
        let (idx, (src, val)) = imin (cmp `on` snd) l
        unless (isNothing val) $ do
            yield (fromJust val)
            (newsrc, newval) <- lift $! fromJust src $$++ CL.head
            let updates = (idx, (Just newsrc, newval)):
                          map (, (Nothing, Nothing)) closedSources
            loop $! l V.// updates

    -- Close expired resumable streams and accumulate a list of indexes of
    -- those streams.
    closeExpired l (i, (Just r, Nothing)) = lift (r $$+- CL.sinkNull)
                                         >> return (i:l)
    closeExpired l _ = return l


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

    runPositions <- runResourceT $
        makeRuns inFile outFile runLength $$ CL.consume
    -- Now have @length runPositions@ runs -- each is @runLength@-sorted.

    let bufSize = memCapacity `quot` k

    runResourceT $ do
        (key, h) <- Res.allocate (IO.openBinaryFile "tmp.txt" IO.WriteMode)
                                  IO.hClose
        CL.sourceList runPositions $= group k $$ awaitForever $ \pos ->
            mergeRuns outFile runLength bufSize pos $$ sinkHandle h
        release key

sinkHandle :: MonadResource m => IO.Handle -> Sink BS.ByteString m ()
sinkHandle h = CL.mapM_ puts
  where
    puts s  = liftIO $ BS.hPut h s >> BS.hPut h newline
    newline = BS.singleton 0x0a
