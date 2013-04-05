{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
module Main (main) where

import           Control.Applicative          ((<$>))
import qualified Control.Concurrent           as Conc
import           Control.Monad
import           Control.Monad.Primitive      (PrimMonad, PrimState, RealWorld)
import           Control.Monad.State
import           Control.Monad.Trans.Resource as Res
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Internal     as BSI
import           Data.Conduit
import qualified Data.Conduit.Binary          as CB
import qualified Data.Conduit.List            as CL
import           Data.Function                (on)
import           Data.Time.Clock.POSIX        (getPOSIXTime)
import qualified Data.Vector                  as V
import qualified Data.Vector.Algorithms.Intro as Intro
import qualified Data.Vector.Mutable          as VM
import           Data.Word                    (Word8)
import           Foreign.ForeignPtr           (ForeignPtr, finalizeForeignPtr,
                                               withForeignPtr)
import           Foreign.Ptr                  as Ptr
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
{-# INLINE hGetBuf #-}

------------------------------------------------------------------------------
-- Sources, Conduits, etc.
------------------------------------------------------------------------------

-- | Reads mutable vectors from upstream and yields their elements downstream.
splatMV :: PrimMonad m => Conduit (VM.MVector (PrimState m) a) m a
splatMV = awaitForever $ \v -> loop (VM.length v) v 0
  where
    loop size v i | i == size = return ()
                  | otherwise = do lift (VM.read v i) >>= yield
                                   loop size v $! i + 1
{-# INLINE splatMV #-}

-- | @sourceHandle total buf h@ reads a total of @total@ bytes from @h@ in
-- blocks of @buf@ bytes.
sourceHandle
    :: MonadResource m
    => Int          -- ^ Total number of bytes to read
    -> Int          -- ^ Size of buffer to which bytes will be read
    -> IO.Handle    -- ^ Handle to input file
    -> Source m BS.ByteString
sourceHandle total bsize h = do
    liftIO $ IO.hSetBuffering h (IO.BlockBuffering (Just bsize))
    bracketP (BSI.mallocByteString bsize) finalizeForeignPtr (loop 0)
  where
    loop readb ptr
        | readb >= total = return ()
        | otherwise     = do
            bs <- liftIO $ hGetBuf h ptr (min bsize (total - readb))
            unless (BS.null bs) $ do
                yield bs
                loop (readb + bsize) ptr
{-# INLINE sourceHandle #-}

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
    -- The same buffer and vector will be re-used in the loop.
    bracketP (BSI.mallocByteString realBufSize) finalizeForeignPtr $ \ptr -> do
    v <- liftIO $ VM.new recordCapacity
    loop v ptr start runLength
  where
    loop  _  _    _    0 = return ()
    loop !v !ptr !pos !count = do
        block <- liftIO $ IO.hSeek h IO.AbsoluteSeek pos
                       >> hGetBuf h ptr realBufSize
        unless (BS.null block) $ do
            v' <- liftIO $ toRecords' v block

            -- We need only up to @count@ of those records.
            let toYield  = VM.take count v'
                newCount = count - VM.length toYield
                newPos = pos + fromIntegral realBufSize

            -- Yield everything and move on.
            yield toYield $= transPipe liftIO splatMV
                          $$ CL.mapM_ yield

            loop v ptr newPos newCount

    -- Use a buffer size that is a multiple of recordSize closest to but less
    -- than the requested buffer size.
    realBufSize = recordCapacity * recordSize
    {-# INLINE realBufSize #-}

    -- Maximum number of records that can be stored in the buffer.
    recordCapacity = bufSize `quot` recordSize
    {-# INLINE recordCapacity #-}
{-# INLINE sourceSortedRun #-}

-- | @groupV' i v@ returns a conduit that reads elements from upstream and
-- puts groups of @i@ elements in the given mutable vector before yielding the
-- vector.
groupV' :: PrimMonad m
        => Int
        -> VM.MVector (PrimState m) a
        -> Conduit a m (VM.MVector (PrimState m) a)
groupV' !n v = lift (VM.clear v) >> loop 0
  where
    loop i | i == n    = do yield v
                            lift (VM.clear v)
                            loop 0
           | otherwise = do x <- await
                            case x of
                                Nothing -> when (i > 0) $
                                              yield $! VM.take i v
                                Just x' -> do lift $! VM.write v i x'
                                              loop $! i + 1
{-# INLINE groupV' #-}

-- | @groupV n@ takes values from upstream and puts them in groups of @n@ as
-- vectors.
groupV :: PrimMonad m => Int -> Conduit a m (V.Vector a)
groupV !n = (lift (VM.new n) >>= groupV' n) =$= CL.mapM V.freeze
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

-- | Read bytestrings from upstream, concatenate them into batches of
-- bytestrings of the given size.
concatBS :: MonadResource m => Int -> Conduit BS.ByteString m BS.ByteString
concatBS size =
    bracketP (BSI.mallocByteString size) finalizeForeignPtr (loop 0)
  where
    loop i ptr
        | i >= size = do yield $! BSI.PS ptr 0 size
                         loop 0 ptr
        | otherwise = do
             ms <- await
             case ms of
                 Nothing -> yield $! BSI.PS ptr 0 i
                 Just bs ->
                     if BS.length bs >= available
                      then let (l, r) = BS.splitAt available bs
                           in do newI <- cpy i l
                                 leftover r
                                 loop newI ptr
                      else do newI <- cpy i bs
                              loop newI ptr
      where
        available = size - i
        {-# INLINE available #-}

        cpy :: MonadIO m => Int -> BS.ByteString -> m Int
        cpy idx (BSI.PS x s l) = liftIO $
            withForeignPtr ptr $ \target ->
            withForeignPtr x   $ \source -> do
            BSI.memcpy (target `Ptr.plusPtr` idx)
                       (source `Ptr.plusPtr` s)
                       (fromIntegral l)
            return $! idx + l
        {-# INLINE cpy #-}
{-# INLINE concatBS #-}

------------------------------------------------------------------------------
-- Utilities
------------------------------------------------------------------------------

-- | A strict version of @putMVar@ that accepts a monad operation that
-- produces the output.
putMVarM' :: MonadIO m => Conc.MVar a -> m a -> m ()
putMVarM' v m = do !x <- m
                   liftIO $! Conc.putMVar v x

-- | Get the current system time in milliseconds.
now :: IO Int
now = truncate . (* 1000) <$> getPOSIXTime
{-# INLINE now #-}

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

-- | @substr i n s@ returns a substring of @s@ starting at index @i@ of length
-- @n@.
substr :: Int -> Int -> BS.ByteString -> BS.ByteString
substr i n ps@(BSI.PS x s l)
    | i <= 0 && n >= l = ps
    | i >= l || l <= 0 = BS.empty
    | otherwise        = BSI.PS x (s + i) n
{-# INLINE substr #-}

-- | Read records from the given bytestring into the given mutable vector
-- starting at the given index.
--
-- Only the first @N@ records will be read, where @N@ is the number of slots
-- in the vector.
--
-- Returns the number of records read.
readRecords
    :: PrimMonad m
    => VM.MVector (PrimState m) BS.ByteString
    -> Int
    -> BS.ByteString
    -> m Int
readRecords v !idx s = loop idx 0
  where
    vlen = VM.length v
    slen = BS.length s
    {-# INLINE vlen #-}
    {-# INLINE slen #-}

    sub i = substr i recordSize s
    {-# INLINE sub #-}

    loop !vi !si | vi == vlen = return vi
                 | si >= slen = return vi
                 | otherwise  = VM.write v vi (sub si) >>
                                loop (vi + 1) (si + recordSize)
{-# INLINE readRecords #-}

-- Size of records being sorted in bytes. This includes the new line.
recordSize :: Int
recordSize = 9
{-# INLINE recordSize #-}

toRecords' :: PrimMonad m
           => VM.MVector (PrimState m) BS.ByteString
           -> BS.ByteString
           -> m (VM.MVector (PrimState m) BS.ByteString)
toRecords' v s = do VM.clear v
                    c <- readRecords v 0 s
                    if c == VM.length v
                        then return v
                        else return $! VM.take c v
{-# INLINE toRecords' #-}

-- | Reads blocks of records from upstream and puts them into separate
-- bytestrings. Re-uses the same vector.
--
-- The argument specifies the maximum number of records a block can contain.
toRecords
    :: MonadIO m
    => Int
    -> Conduit BS.ByteString m (VM.MVector RealWorld BS.ByteString)
toRecords maxRecords = do v <- liftIO $ VM.new maxRecords
                          CL.mapM $ liftIO . toRecords' v
{-# INLINE toRecords #-}

------------------------------------------------------------------------------
-- msort
------------------------------------------------------------------------------

-- | @makeRuns input output length@ makes runs of length @length@ in @output@.
-- @length@ specifies the number of records, not the number of bytes.
--
-- This is pass 0 of the external merge sort algorithm.
--

-- | @makeRuns input totalRecords runLength@ makes runs of length @runLength@
-- with up to @totalRecords@ from @input@ into a temporary file.
--
-- A @RunInfo@ is returned with information about the run.
makeRuns
    :: MonadResource m
    => IO.Handle    -- ^ Handle to the input file
    -> Int          -- ^ Total number of records
    -> Int          -- ^ Number of records in each run
    -> m RunInfo
makeRuns inFile recordCount runLength = do
    (outKey, outPath, outHandle) <- allocateTempFile "run.txt"
    liftIO $ IO.hSetBuffering outHandle (IO.BlockBuffering (Just blockSize))
    sourceHandle totalSize blockSize inFile
        $= toRecords runLength
        $= transPipe liftIO (CL.iterM Intro.sort =$= splatMV)
        $= concatBS blockSize
        $$ CB.sinkHandle outHandle
    return $! RI outKey outPath outHandle recordCount
  where
    totalSize = recordCount * recordSize
    blockSize = runLength * recordSize
    {-# INLINE totalSize #-}
    {-# INLINE blockSize #-}
{-# INLINE makeRuns #-}

-- | Given a vector of sources that yield elemnts in-order, return a source
-- that combines and yields the elements of all the sources in-order.
sortSources
    :: (Monad m, Ord a)
    => V.Vector (Source m a)
    -> Source m a
sortSources sources = do
    sourcesAndValues <- lift $! V.forM sources $ \s -> do
                                    (s', v) <- s $$+ CL.head
                                    return (Just s', v)
    loop sourcesAndValues
  where
    cmp Nothing  Nothing  = EQ
    cmp Nothing  _        = GT
    cmp _        Nothing  = LT
    cmp (Just a) (Just b) = a `compare` b
    {-# INLINE cmp #-}

    loop !l = do
        closedSources <- V.foldM' closeExpired [] (V.indexed l)
        let idx        = V.minIndexBy (cmp `on` snd) l
        case l V.! idx of
          (_, Nothing) -> return ()
          (Just src,   Just val) -> do
            yield val
            (newsrc, newval) <- lift $! src $$++ CL.head
            let updates = (idx, (Just newsrc, newval)):
                          map (, (Nothing, Nothing)) closedSources
            loop $! l V.// updates

    closeExpired l (i, (Just r, Nothing)) = lift (r $$+- return ())
                                         >> return (i:l)
    closeExpired l _                      = return       l
    {-# INLINE closeExpired #-}
{-# INLINE sortSources #-}

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
mergeRuns h runLen bsize positions =
    sortSources $ V.map runIterator positions
  where
    -- Source that yields elements of the given run.
    runIterator !pos = sourceSortedRun h pos runLen bsize
    {-# INLINE runIterator #-}
{-# INLINE mergeRuns #-}

-- | Returns the number of records in the given file.
getRecordCount :: MonadIO m => IO.Handle -> m Int
getRecordCount h = liftIO $ do
    oldPosition <- IO.hTell h
    IO.hSeek h IO.SeekFromEnd 0
    newPosition <- fromInteger <$> IO.hTell h
    IO.hSeek h IO.AbsoluteSeek oldPosition
    return $! newPosition `quot` recordSize

data RunInfo = RI { riReleaseKey  :: Res.ReleaseKey
                  , riFilePath    :: IO.FilePath
                  , riHandle      :: IO.Handle
                  , riRecordCount :: Int
                  }

-- | Finish sorting the given run.
msort :: forall m. MonadResource m
      => RunInfo    -- ^ Information about the run
      -> Int        -- ^ @n@ where the file is @n@-sorted
      -> Int        -- ^ Buffer size used while sorting
      -> Int        -- ^ @k@-way merge
      -> m RunInfo
msort runInfo runLength bufSize k = loop 1 runInfo runLength
  where
    totalSize    = totalRecords * recordSize
    totalRecords = riRecordCount runInfo
    {-# INLINE totalSize    #-}
    {-# INLINE totalRecords #-}

    loop :: Int -> RunInfo -> Int -> m RunInfo
    loop !pass input n
        | n >= totalRecords = return input
        | otherwise         = do
            (outKey, outPath, outHandle) <- allocateTempFile "run.txt"
            liftIO $ do
                putStrLn $ printf "Pass %d" pass
                IO.hSetBuffering inHandle
                                 (IO.BlockBuffering (Just bufSize))
                IO.hSetBuffering outHandle
                                 (IO.BlockBuffering (Just bufSize))

            let positions = range 0 (fromIntegral $ n * recordSize)
                                    (fromIntegral $  totalSize - 1)
                groups    = positions $= transPipe liftIO (groupV k)

            -- Perform a k-way merge
            groups $$ CL.mapM_ $ \pos ->
                mergeRuns inHandle n bufSize pos
                   $$ concatBS bufSize
                   =$ CB.sinkHandle outHandle

            Res.release $ riReleaseKey input

            -- Now have (n * k)-sorted.
            let newInput = input { riReleaseKey = outKey
                                 , riFilePath   = outPath
                                 , riHandle     = outHandle
                                 }
            loop (pass + 1) newInput (n * k)
          where
            inHandle = riHandle input
            {-# INLINE inHandle #-}

main :: IO ()
main = do
    args <- getArgs

    when (length args /= 4) $ do
        progName <- getProgName
        putStrLn $ progName ++
                   " <input file> <output file> <memory capacity> <k>"
        exitFailure

    numCapabilities <- Conc.getNumCapabilities
    when (numCapabilities > 1) $
        putStrLn $ printf "Will run %d instances concurrently."
                          numCapabilities

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
        (inputKey, inputHandle)
                     <- Res.allocate (IO.openBinaryFile inFile IO.ReadMode)
                                      IO.hClose
        totalRecords <- getRecordCount inputHandle
        liftIO $ putStrLn $ printf "Sorting %d records." totalRecords
        startTime <- liftIO now

        -- We are going to run @numCapabilities@ concurrent sorters. So, the
        -- records are divied up equally amongst all sorters except the last
        -- one which will take the extra ones as well.
        let (recEach, recExtra) = totalRecords `quotRem` numCapabilities
            procRecordCounts = replicate (numCapabilities - 1) recEach
                                 ++ [recEach + recExtra]

        -- Pass 0: Make @runLength@-sorted runs for each instance.
        sortedRuns <- forM procRecordCounts $ \recordCount ->
            makeRuns inputHandle recordCount runLength
        Res.release inputKey

        -- We now have @runLength@-sorted runs for each instance which can
        -- sort it on its own.

        -- Sort each run concurrently and get the final run info
        !finalRunMVars <- forM sortedRuns $ \runInfo -> do
            final <- liftIO Conc.newEmptyMVar
            Res.resourceForkIO $ putMVarM' final $
                msort runInfo runLength bufSize k
            return final

        !finalRuns <- mapM (liftIO . Conc.takeMVar) finalRunMVars

        -- Now have @numCapabilities@ files that are fully sorted. Can merge
        -- them all in a single pass.

        let runIterator RI{..} = sourceSortedRun riHandle 0
                                                 riRecordCount bufSize
            finalSources = V.fromList $ map runIterator finalRuns

        -- When there were multiple concurrent sorters, merge the final
        -- outputs in one pass and rename the final temporary file to the
        -- output file.
        when (numCapabilities > 1) $ do
            (_, outPath, outHandle) <- allocateTempFile "run.txt"
            sortSources finalSources $$ concatBS bufSize
                                     =$ CB.sinkHandle outHandle
            liftIO $ IO.hClose outHandle >> renameFile outPath outFile

        -- If there was only one sorter, use its result.
        unless (numCapabilities > 1) $ do
            let RI{..} = head finalRuns
            liftIO $ IO.hClose riHandle >> renameFile riFilePath outFile

        endTime <- liftIO now
        liftIO $ putStrLn $ printf "TIME: %d milliseconds"
                                   (endTime - startTime)
