{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections         #-}
module Main (main) where

import           Control.Applicative          ((<$>))
import           Control.Monad
import           Control.Monad.Primitive      (PrimMonad, PrimState)
import           Control.Monad.State
import           Control.Monad.Trans.Resource as Res
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Internal     as BSI
import           Data.Conduit
import qualified Data.Conduit.Binary          as CB
import qualified Data.Conduit.Internal        as CI
import qualified Data.Conduit.List            as CL
import           Data.Function                (on)
import           Data.Maybe                   (fromJust, isNothing)
import           Data.Time.Clock.POSIX        (getPOSIXTime)
import qualified Data.Vector                  as V
import qualified Data.Vector.Algorithms.Intro as Intro
import qualified Data.Vector.Mutable          as VM
import           Data.Void                    (absurd)
import           Data.Word                    (Word8)
import           Foreign.ForeignPtr           (ForeignPtr, finalizeForeignPtr,
                                               withForeignPtr)
import           Prelude                      hiding (lines)
import           System.Directory             (doesFileExist, removeFile,
                                               renameFile)
import           System.Environment           (getArgs, getProgName)
import           System.Exit                  (exitFailure)
import qualified System.IO                    as IO
import           Text.Printf                  (printf)

------------------------------------------------------------------------------
-- I/O
------------------------------------------------------------------------------

-- | Similar to @System.IO.hGetBuf@. Reads a ByteString into an existing
-- buffer and returns a ByteString referencing that buffer.
hGetBuf :: IO.Handle -> ForeignPtr Word8 -> Int -> IO BS.ByteString
hGetBuf handle !ptr !count = do
    -- Originally used hGetBufSome here but it caused some weird line break
    -- bug where when it had to read the last 90 bytes of an 8100 byte file,
    -- it would read only 86, reading the next 4 in the next iteration.
    bytesRead <- withForeignPtr ptr $ \p -> IO.hGetBuf handle p count
    return $! BSI.PS ptr 0 bytesRead

------------------------------------------------------------------------------
-- Sources, Conduits, etc.
------------------------------------------------------------------------------

countSunk :: Monad m => Sink i m a -> Sink i m (a, Int)
countSunk (CI.ConduitM pipe) = CI.ConduitM $ loop 0 pipe
  where
    loop !i p =
        case p of
            CI.HaveOutput _ _ o -> absurd o
            CI.NeedInput rp rc  -> CI.NeedInput (loop (i + 1) . rp)
                                                (\u -> (,i) <$> rc u)
            CI.Done a           -> return (a, i)
            CI.PipeM mp         -> lift mp >>= loop i
            CI.Leftover p' _    -> loop i p'
{-# INLINE countSunk #-}

-- | Read mutable vectors from upstream and yield their elements downstream.
splatMV :: PrimMonad m => Conduit (VM.MVector (PrimState m) a) m a
splatMV = awaitForever $ \v -> loop (VM.length v) v 0
  where
    loop size v i | i == size = return ()
                  | otherwise = do lift (VM.read v i) >>= yield
                                   loop size v $! i + 1
{-# INLINE splatMV #-}

-- | Similar to @sourceFile@ except that this can operate on an existing
-- handle.
sourceHandle
    :: MonadResource m
    => Int          -- ^ Size of the buffer used to read blocks
    -> IO.Handle    -- ^ File Handle
    -> Producer m BS.ByteString
sourceHandle bsize h =
    -- Allocate the block and ensure it is freed after we are done.
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr $ \ptr ->
    loop ptr
  where
    loop !ptr = do
        bs <- liftIO $ hGetBuf h ptr bsize
        unless (BS.null bs) $
            yield bs >> loop ptr
{-# INLINE sourceHandle #-}

-- | @sourceFile bsize fp@ sends blocks of bytestrings of size @bsize@
-- downstream.
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
    sourceHandle bsize

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
sourceSortedRun h !start !runLength bufSize =
    bracketP (BSI.mallocByteString realBufSize) finalizeForeignPtr $ \ptr ->
    loop ptr start runLength
  where
    loop  _    _    0 = return ()
    loop !ptr !pos !count = do
        block <- liftIO $ IO.hSeek h IO.AbsoluteSeek pos
                       >> hGetBuf h ptr realBufSize

        unless (BS.null block) $ do
            let records = toRecords block
                toYield = V.take count records
                newCount = count - V.length toYield
            V.mapM_ yield toYield
            loop ptr (pos + fromIntegral realBufSize) newCount

    -- Change buffer size to the closest multiple of the recordSize that is
    -- less than the original buffer size.
    realBufSize = (bufSize `quot` recordSize) * recordSize
{-# INLINE sourceSortedRun #-}

-- | @takeV n@ takes @n@ values from upstream and puts them into a vector.
takeV :: (Monad m, PrimMonad m) => Int -> Consumer a m (V.Vector a)
takeV !n = (lift $! VM.new n) >>= go
  where
    go v = loop 0
      where
        loop i
            | i == n    = lift $! V.unsafeFreeze v
            | otherwise =
                    do  x <- await
                        case x of
                            Just x' -> do lift $! VM.write v i x'
                                          loop $! i + 1
                            Nothing -> lift $! V.unsafeFreeze (VM.take i v)
{-# INLINE takeV #-}

-- | @groupV n@ takes values from upstream and puts them in groups of @n@ as
-- vectors.
groupV :: (Monad m, PrimMonad m) => Int -> Conduit a m (V.Vector a)
groupV !n = loop
  where loop = do v <- takeV n
                  unless (V.null v) $! yield v >> loop
{-# INLINE groupV #-}

-- | @range start step step@ produces numbers from from @start@ to @stop@
-- inclusive. @step@ specifies the number to add at each step.
range :: (Num a, Ord a, Monad m) => a -> a -> a -> Source m a
range start step stop = loop start
  where
    loop i
        | i <= stop = yield i >> loop (i + step)
        | otherwise = return ()
{-# INLINE range #-}

------------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------------

-- | Get the current system time in milliseconds.
now :: IO Int
now = truncate . (* 1000) <$> getPOSIXTime
{-# INLINE now #-}

-- | Get the third element of the tuple.
thrd :: (a, b, c) -> c
thrd (_, _, c) = c
{-# INLINE thrd #-}

-- | Allocate a temporary file under a Resource monad. They file will be
-- deleted when the key is released.
allocateTempFile
    :: MonadResource m
    => String
    -> m (Res.ReleaseKey, FilePath, IO.Handle)
allocateTempFile pat = do
    (k, (p, h)) <- Res.allocate (IO.openBinaryTempFile "." pat) clean
    return (k, p, h)
  where
    clean (p, h) = do IO.hClose h
                      exists <- doesFileExist p
                      when exists (removeFile p)

-- | Same as @toRecords@ except that a mutable vector is returned.
toRecordsMV
    :: PrimMonad m
    => BS.ByteString
    -> m (VM.MVector (PrimState m) BS.ByteString)
toRecordsMV s = VM.new count >>= go
  where
    count = BS.length s `quot` recordSize
    go !v  = loop 0
      where
        loop i | i == count = return v
               | otherwise  = do
                      let !x = BS.take recordSize
                             $ BS.drop (i * recordSize) s
                      VM.write v i x
                      loop $! i + 1
{-# INLINE toRecordsMV #-}

-- | Groups the given bytestrings into records. Each record is assumed to be
-- @recordSize@ bytes. The given ByteString's length must be a multiple of
-- @recordSize@.
toRecords :: BS.ByteString -> V.Vector BS.ByteString
toRecords !s = V.create $ toRecordsMV s
{-# INLINE toRecords #-}

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
--
-- The total number of records written is returned
makeRuns
    :: FilePath     -- ^ Path to the file containing the unsorted data
    -> IO.Handle    -- ^ Target file to which sorted runs will be written
    -> Int          -- ^ Number of records in each sorted run
    -> IO Int
makeRuns inFile outHandle runLength = runResourceT . fmap snd
    $  sourceFile blockSize inFile
    $= transPipe liftIO (CL.mapM sortRecords =$= splatMV)
    $$ countSunk
     $ CB.sinkHandle outHandle
  -- Number of bytes consumed by @runLength@ records.
  where blockSize   = runLength * recordSize

        sortRecords
            :: PrimMonad m
            => BS.ByteString
            -> m (VM.MVector (PrimState m) BS.ByteString)
        sortRecords !s = do recs <- toRecordsMV s
                            Intro.sort recs
                            return recs
{-# INLINE makeRuns #-}

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
    => IO.Handle        -- ^ File containing the sorted runs
    -> Int              -- ^ Number of records in each sorted run
    -> Int              -- ^ Buffer size to use to read from each sorted run
    -> V.Vector Integer -- ^ List of starting positions of each run
    -> Source m BS.ByteString
mergeRuns h runLen bsize positions = do
    let sources = V.map runIterator positions
    sourcesAndValues <- lift $! V.forM sources $ \s -> do
                                    (s', v) <- s $$+ CL.head
                                    return (Just s', v)
    loop sourcesAndValues
  where
    -- Source that yields elements of the given run.
    runIterator !pos = sourceSortedRun h pos runLen bsize

    cmp Nothing Nothing   = EQ
    cmp Nothing _         = GT
    cmp _       Nothing   = LT
    cmp (Just a) (Just b) = a `compare` b

    loop !l = do
        closedSources <- V.foldM' closeExpired [] (V.indexed l)
        let !idx = V.minIndexBy (cmp `on` snd) l
            !(src, val) = l V.! idx
        unless (isNothing val) $ do
            yield (fromJust val)
            (newsrc, newval) <- lift $! fromJust src $$++ CL.head
            let updates = (idx, (Just newsrc, newval)):
                          map (, (Nothing, Nothing)) closedSources
            loop $! l V.// updates

    -- Close expired resumable streams and accumulate a list of indexes of
    -- those streams.
    closeExpired l (i, (Just r, Nothing)) = lift (r $$+- return ())
                                         >> return (i:l)
    closeExpired l _ = return l
{-# INLINE mergeRuns #-}


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
        bufSize             = ((memCapacity `quot` k) `quot` recordSize)
                                * recordSize

    unless (bufSize > 0) $ do
        putStrLn $ printf "Not enough memory to perform %d-way merge." k
        putStrLn $ printf "Need at least %d bytes." (k * recordSize)
        exitFailure

    putStrLn $ printf "%d-way merge; buffer size: %d; run length %d records"
                      k bufSize runLength

    runResourceT $ do
        runFile@(_, _, rfile) <- allocateTempFile "run.txt"

        startTime <- liftIO now

        -- Pass 0
        totalRecords <- liftIO $ makeRuns inFile rfile runLength
        -- File is now @runLength@-sorted.

        liftIO $ putStrLn $ printf "Pass 0: %d records" totalRecords

        let totalSize = totalRecords * recordSize

            loop
                :: MonadResource m
                => Int
                -> (Res.ReleaseKey, FilePath, IO.Handle)
                -> (Res.ReleaseKey, FilePath, IO.Handle)
                -> Int
                -> m (Res.ReleaseKey, FilePath, IO.Handle)
            loop !pass input output n | n >= totalRecords = return input
                                      | otherwise = do
                let positions = range 0 (fromIntegral $ n * recordSize)
                                        (fromIntegral $ totalSize - 1)
                    groups = positions $= transPipe liftIO (groupV k)

                liftIO $ putStrLn $ printf "Pass %d" pass

                -- k-way merge
                groups $$ CL.mapM_ $ \pos ->
                  mergeRuns (thrd input) n bufSize pos
                       $$ CB.sinkHandle (thrd output)

                -- Now have (n * k)-sorted
                let newInput = output
                newOutput <- allocateTempFile "run.txt"

                loop (pass + 1) newInput newOutput (n * k)

        targetFile <- allocateTempFile "run.txt"
        (_, sortedp, sortedh) <- loop 1 runFile targetFile runLength

        liftIO $ IO.hClose sortedh
              >> renameFile sortedp outFile

        endTime <- liftIO now
        liftIO . putStrLn $ printf "TIME: %d milliseconds" (endTime - startTime)

