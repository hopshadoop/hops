/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.erasure_coding;

import io.hops.erasure_coding.BaseEncodingManager.LOGTYPES;
import io.hops.erasure_coding.StripeReader.LocationPair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Progressable;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

/**
 * Represents a generic decoder that can be used to read a file with
 * corrupt blocks by using the parity file.
 */
public class Decoder {
  public static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.raid.Decoder");
  private final Log DECODER_METRICS_LOG = LogFactory.getLog("RaidMetrics");
  public static final int DEFAULT_PARALLELISM = 4;
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;
  private int numMissingBlocksInStripe;
  private long numReadBytes;

  public static class BlockChecksumException extends IOException {
    public BlockChecksumException() {
    }

    public BlockChecksumException(String message) {
      super(message);
    }

    public BlockChecksumException(String message, Throwable cause) {
      super(message, cause);
    }

    public BlockChecksumException(Throwable cause) {
      super(cause);
    }
  }

  public Decoder(Configuration conf, Codec codec) {
    this.conf = conf;
    this.parallelism =
        conf.getInt("raid.encoder.parallelism", DEFAULT_PARALLELISM);
    this.codec = codec;
    this.code = codec.createErasureCode(conf);
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.decoder.bufsize", 1024 * 1024);
    this.writeBufs = new byte[codec.parityLength][];
    this.readBufs = new byte[codec.parityLength + codec.stripeLength][];
    //allocateBuffers();
    //writeBufs will be allocated when the writeBufs.length is known and
    //writeBufs array can be initialized.
  }
  
  public int getNumMissingBlocksInStripe() {
    return numMissingBlocksInStripe;
  }

  public long getNumReadBytes() {
    return numReadBytes;
  }
  
  private void allocateBuffers() {
    for (int i = 0; i < writeBufs.length; i++) {
      writeBufs[i] = new byte[bufSize];
    }
  }

  private void configureBuffers(long blockSize) {
    if ((long) bufSize > blockSize) {
      bufSize = (int) blockSize;
      allocateBuffers();
    } else if (blockSize % bufSize != 0) {
      bufSize = (int) (blockSize / 256L); // heuristic.
      if (bufSize == 0) {
        bufSize = 1024;
      }
      bufSize = Math.min(bufSize, 1024 * 1024);
      allocateBuffers();
    }
  }
  
  public void recoverParityBlockToFile(FileSystem srcFs, Path srcPath,
      FileSystem parityFs, Path parityPath, long blockSize, long blockOffset,
      File localBlockFile, Context context)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) srcFs;
    long crc32 = dfs.getClient().getBlockChecksum(parityPath.toUri().toString(),
        (int) (blockOffset / blockSize));
    OutputStream out = null;
    try {
      out = new FileOutputStream(localBlockFile);
      fixErasedBlock(srcFs, srcPath, parityFs, parityPath, false, blockSize,
          blockOffset, blockSize, false, out, context, false, crc32);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Recovers a corrupt block to local file.
   *
   * @param srcFs
   *     The filesystem containing the source file.
   * @param srcPath
   *     The damaged source file.
   * @param parityPath
   *     The filesystem containing the parity file. This could be
   *     different from fs in case the parity file is part of a HAR archive.
   * @param blockSize
   *     The block size of the file.
   * @param blockOffset
   *     Known location of error in the source file. There could
   *     be additional errors in the source file that are discovered during
   *     the decode process.
   * @param localBlockFile
   *     The file to write the block to.
   * @param limit
   *     The maximum number of bytes to be written out.
   *     This is to prevent writing beyond the end of the file.
   */
  public void recoverBlockToFile(FileSystem srcFs, Path srcPath,
      FileSystem parityFs, Path parityPath, long blockSize, long blockOffset,
      File localBlockFile, long limit, Context context)
      throws IOException, InterruptedException {
    DistributedFileSystem dfs = (DistributedFileSystem) srcFs;
    long crc32 = dfs.getClient().getBlockChecksum(srcPath.toUri().getPath(),
        (int) (blockOffset / blockSize));
    OutputStream out = null;
    try {
      out = new FileOutputStream(localBlockFile);
      fixErasedBlock(srcFs, srcPath, parityFs, parityPath, true, blockSize,
          blockOffset, limit, false, out, context, false, crc32);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  DecoderInputStream generateAlternateStream(FileSystem srcFs, Path srcFile,
      FileSystem parityFs, Path parityFile, long blockSize, long errorOffset,
      long limit, Context context) {
    configureBuffers(blockSize);
    Progressable reporter = context;
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }
    
    DecoderInputStream decoderInputStream =
        new DecoderInputStream(reporter, limit, blockSize, errorOffset, srcFs,
            srcFile, parityFs, parityFile);
    
    return decoderInputStream;
  }

  /**
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void fixErasedBlock(FileSystem srcFs, Path srcFile, FileSystem parityFs,
      Path parityFile, boolean fixSource, long blockSize, long errorOffset,
      long limit, boolean partial, OutputStream out, Mapper.Context context,
      boolean skipVerify, long oldCrc)
      throws IOException, InterruptedException {

    // TODO This causes a NullPointerException and it didn't seem to be required
    //    configureBuffers(blockSize);
    Progressable reporter = context;
    if (reporter == null) {
      reporter = RaidUtils.NULL_PROGRESSABLE;
    }

    CRC32 crc = new CRC32();
    fixErasedBlockImpl(srcFs, srcFile, parityFs, parityFile, fixSource,
        blockSize, errorOffset, limit, partial, out, reporter, crc);
    if (crc.getValue() != oldCrc) {
      throw new BlockChecksumException(String.format(
          "Repair of %s at offset %d failed. Checksum differs from stored checksum",
          fixSource ? srcFile : parityFile, errorOffset));
    }
  }

  long fixErasedBlockImpl(FileSystem srcFs, Path srcFile, FileSystem parityFs,
      Path parityFile, boolean fixSource, long blockSize, long errorOffset,
      long limit, boolean partial, OutputStream out, Progressable reporter,
      CRC32 crc) throws IOException {
    long startTime = System.currentTimeMillis();
    if (crc != null) {
      crc.reset();
    }
    int blockIdx = (int) (errorOffset / blockSize);
    LocationPair lp = null;
    int erasedLocationToFix;
    if (fixSource) {
      lp = StripeReader.getBlockLocation(codec, blockIdx);
      erasedLocationToFix = codec.parityLength + lp.getBlockIdxInStripe();
    } else {
      lp = StripeReader.getParityBlockLocation(codec, blockIdx);
      erasedLocationToFix = lp.getBlockIdxInStripe();
    }

    FileStatus srcStat = srcFs.getFileStatus(srcFile);
    FileStatus parityStat = parityFs.getFileStatus(parityFile);

    InputStream[] inputs = null;
    List<Integer> erasedLocations = new ArrayList<Integer>();
    // Start off with one erased location.
    erasedLocations.add(erasedLocationToFix);
    List<Integer> locationsToRead =
        new ArrayList<Integer>(codec.parityLength + codec.stripeLength);

    int boundedBufferCapacity = 2;
    ParallelStreamReader parallelReader = null;
    LOG.info("Need to write " + limit +
        " bytes for erased location index " + erasedLocationToFix);
    
    long startOffsetInBlock = 0;
    if (partial) {
      startOffsetInBlock = errorOffset % blockSize;
    }

    // will be resized later
    int[] erasedLocationsArray = new int[0];
    int[] locationsToReadArray = new int[0];
    int[] locationsNotToReadArray = new int[0];

    try {
      numReadBytes = 0;
      long written;
      // Loop while the number of written bytes is less than the max.
      for (written = 0; written < limit; ) {
        try {
          if (parallelReader == null) {
            long offsetInBlock = written + startOffsetInBlock;
            StripeReader sReader = StripeReader
                .getStripeReader(codec, conf, blockSize, srcFs,
                    lp.getStripeIdx(), srcStat);
            inputs = sReader
                .buildInputs(srcFs, srcFile, srcStat, parityFs, parityFile,
                    parityStat, lp.getStripeIdx(), offsetInBlock,
                    erasedLocations, locationsToRead, code);
            
            /*
             * locationsToRead have now been populated and erasedLocations
             * might have been updated with more erased locations.
             */
            LOG.info("Erased locations: " + erasedLocations.toString() +
                "\nLocations to Read for repair:" +
                locationsToRead.toString());

            /*
             * Initialize erasedLocationsArray with erasedLocations.
             */
            int i = 0;
            erasedLocationsArray = new int[erasedLocations.size()];
            for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
                 loc++) {
              if (erasedLocations.indexOf(loc) >= 0) {
                erasedLocationsArray[i] = loc;
                i++;
              }
            }

            /*
             * Initialize locationsToReadArray with locationsToRead.
             */
            i = 0;
            locationsToReadArray = new int[locationsToRead.size()];
            for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
                 loc++) {
              if (locationsToRead.indexOf(loc) >= 0) {
                locationsToReadArray[i] = loc;
                i++;
              }
            }

            i = 0;
            locationsNotToReadArray =
                new int[codec.stripeLength + codec.parityLength -
                    locationsToRead.size()];

            for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
                 loc++) {
              if (locationsToRead.indexOf(loc) == -1 ||
                  erasedLocations.indexOf(loc) != -1) {
                locationsNotToReadArray[i] = loc;
                i++;
              }
            }

            this.writeBufs = new byte[erasedLocations.size()][];
            allocateBuffers();

            assert (parallelReader == null);
            parallelReader = new ParallelStreamReader(reporter, inputs,
                (int) Math.min(bufSize, limit), parallelism,
                boundedBufferCapacity, Math.min(limit, blockSize));
            parallelReader.start();
          }
          ParallelStreamReader.ReadResult readResult =
              readFromInputs(erasedLocations, limit, reporter, parallelReader);
          
          code.decodeBulk(readResult.readBufs, writeBufs, erasedLocationsArray,
              locationsToReadArray, locationsNotToReadArray);
          
          // get the number of bytes read through hdfs.
          for (int readNum : readResult.numRead) {
            numReadBytes += readNum;
          }

          int toWrite = (int) Math.min((long) bufSize, limit - written);
          for (int i = 0; i < erasedLocationsArray.length; i++) {
            if (erasedLocationsArray[i] == erasedLocationToFix) {
              if (out != null) {
                out.write(writeBufs[i], 0, toWrite);
              }
              if (crc != null) {
                crc.update(writeBufs[i], 0, toWrite);
              }
              written += toWrite;
              break;
            }
          }
        } catch (IOException e) {
          if (e instanceof TooManyErasedLocations) {
            logRaidReconstructionMetrics("FAILURE", 0, codec,
                System.currentTimeMillis() - startTime, erasedLocations.size(),
                numReadBytes, srcFile, errorOffset,
                LOGTYPES.OFFLINE_RECONSTRUCTION, srcFs);
            throw e;
          }
          // Re-create inputs from the new erased locations.
          if (parallelReader != null) {
            parallelReader.shutdown();
            parallelReader = null;
          }
          RaidUtils.closeStreams(inputs);
        }
      }
      logRaidReconstructionMetrics("SUCCESS", written, codec,
          System.currentTimeMillis() - startTime, erasedLocations.size(),
          numReadBytes, srcFile, errorOffset, LOGTYPES.OFFLINE_RECONSTRUCTION,
          srcFs);
      return written;
    } finally {
      numMissingBlocksInStripe = erasedLocations.size();
      if (parallelReader != null) {
        parallelReader.shutdown();
      }
      RaidUtils.closeStreams(inputs);
    }
  }

  ParallelStreamReader.ReadResult readFromInputs(List<Integer> erasedLocations,
      long limit, Progressable reporter, ParallelStreamReader parallelReader)
      throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      long start = System.currentTimeMillis();
      readResult = parallelReader.getReadResult();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    IOException exceptionToThrow = null;
    // Process io errors, we can tolerate upto codec.parityLength errors.
    for (int i = 0; i < readResult.ioExceptions.length; i++) {
      IOException e = readResult.ioExceptions[i];
      if (e == null) {
        continue;
      }
      if (e instanceof BlockMissingException) {
        LOG.warn("Encountered BlockMissingException in stream " + i);
      } else if (e instanceof ChecksumException) {
        LOG.warn("Encountered ChecksumException in stream " + i);
      } else {
        throw e;
      }
      int newErasedLocation = i;
      erasedLocations.add(newErasedLocation);
      exceptionToThrow = e;
    }
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return readResult;
  }
  
  public void logRaidReconstructionMetrics(String result, long bytes,
      Codec codec, long delay, int numMissingBlocks, long numReadBytes,
      Path srcFile, long errorOffset, LOGTYPES type, FileSystem fs) {

    try {
      JSONObject json = new JSONObject();
      json.put("result", result);
      json.put("constructedbytes", bytes);
      json.put("code", codec.id);
      json.put("delay", delay);
      json.put("missingblocks", numMissingBlocks);
      json.put("readbytes", numReadBytes);
      json.put("file", srcFile.toString());
      json.put("offset", errorOffset);
      json.put("type", type.name());
      json.put("cluster", fs.getUri().getAuthority());
      DECODER_METRICS_LOG.info(json.toString());

    } catch (JSONException e) {
      LOG.warn("Exception when logging the Raid metrics: " + e.getMessage(), e);
    }
  }
  
  public class DecoderInputStream extends InputStream {
    
    private long limit;
    private ParallelStreamReader parallelReader = null;
    private byte[] buffer;
    private long bufferLen;
    private int position;
    private long streamOffset = 0;
    
    private final Progressable reporter;
    private InputStream[] inputs;
    private final int boundedBufferCapacity = 2;
    
    private final long blockSize;
    private final long errorOffset;
    private long startOffsetInBlock;
    
    private final FileSystem srcFs;
    private final Path srcFile;
    private final FileSystem parityFs;
    private final Path parityFile;
    
    private int blockIdx;
    private int erasedLocationToFix;
    private LocationPair locationPair;
    
    private long currentOffset;
    private long dfsNumRead = 0;

    private final List<Integer> locationsToRead = new ArrayList<Integer>();
    private final List<Integer> erasedLocations = new ArrayList<Integer>();
    int[] erasedLocationsArray;
    int[] locationsToReadArray;
    int[] locationsNotToReadArray;

    public DecoderInputStream(final Progressable reporter, final long limit,
        final long blockSize, final long errorOffset, final FileSystem srcFs,
        final Path srcFile, final FileSystem parityFs, final Path parityFile) {
      
      this.reporter = reporter;
      this.limit = limit;
      
      this.blockSize = blockSize;
      this.errorOffset = errorOffset;
      
      this.srcFile = srcFile;
      this.srcFs = srcFs;
      this.parityFile = parityFile;
      this.parityFs = parityFs;
      
      this.blockIdx = (int) (errorOffset / blockSize);
      this.startOffsetInBlock = errorOffset % blockSize;
      this.currentOffset = errorOffset;
    }

    public long getCurrentOffset() {
      return currentOffset;
    }
    
    public long getAvailable() {
      return limit - streamOffset;
    }
    
    /**
     * Will init the required objects, start the parallel reader, and
     * put the decoding result in buffer in this method.
     *
     * @throws java.io.IOException
     */
    private void init() throws IOException {
      if (streamOffset >= limit) {
        buffer = null;
        return;
      }

      if (null == locationPair) {
        locationPair = StripeReader.getBlockLocation(codec, blockIdx);
        erasedLocationToFix =
            codec.parityLength + locationPair.getBlockIdxInStripe();
        erasedLocations.add(erasedLocationToFix);
      }

      if (null == parallelReader) {

        long offsetInBlock = streamOffset + startOffsetInBlock;
        FileStatus srcStat = srcFs.getFileStatus(srcFile);
        FileStatus parityStat = parityFs.getFileStatus(parityFile);
        StripeReader sReader = StripeReader
            .getStripeReader(codec, conf, blockSize, srcFs,
                locationPair.getStripeIdx(), srcStat);

        inputs = sReader
            .buildInputs(srcFs, srcFile, srcStat, parityFs, parityFile,
                parityStat, locationPair.getStripeIdx(), offsetInBlock,
                erasedLocations, locationsToRead, code);

        /*
         * locationsToRead have now been populated and erasedLocations
         * might have been updated with more erased locations.
         */
        LOG.info("Erased locations: " + erasedLocations.toString() +
            "\nLocations to Read for repair:" +
            locationsToRead.toString());

        /*
         * Initialize erasedLocationsArray with the erasedLocations.
         */
        int i = 0;
        erasedLocationsArray = new int[erasedLocations.size()];
        for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
             loc++) {
          if (erasedLocations.indexOf(loc) >= 0) {
            erasedLocationsArray[i] = loc;
            i++;
          }
        }
        /*
         * Initialize locationsToReadArray with the locationsToRead.
         */
        i = 0;
        locationsToReadArray = new int[locationsToRead.size()];
        for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
             loc++) {
          if (locationsToRead.indexOf(loc) >= 0) {
            locationsToReadArray[i] = loc;
            i++;
          }
        }

        /*
         * Initialize locationsNotToReadArray with the locations that are
         * either erased or not supposed to be read.
         */
        i = 0;
        locationsNotToReadArray =
            new int[codec.stripeLength + codec.parityLength -
                locationsToRead.size()];

        for (int loc = 0; loc < codec.stripeLength + codec.parityLength;
             loc++) {
          if (locationsToRead.indexOf(loc) == -1 ||
              erasedLocations.indexOf(loc) != -1) {
            locationsNotToReadArray[i] = loc;
            i++;
          }
        }

        writeBufs = new byte[erasedLocations.size()][];
        allocateBuffers();

        assert (parallelReader == null);
        parallelReader = new ParallelStreamReader(reporter, inputs,
            (int) Math.min(bufSize, limit), parallelism, boundedBufferCapacity,
            limit);
        parallelReader.start();
      }

      if (null != buffer && position == bufferLen) {
        buffer = null;
      }

      if (null == buffer) {
        ParallelStreamReader.ReadResult readResult =
            readFromInputs(erasedLocations, limit, reporter, parallelReader);

        // get the number of bytes read through hdfs.
        for (int readNum : readResult.numRead) {
          dfsNumRead += readNum;
        }
        code.decodeBulk(readResult.readBufs, writeBufs, erasedLocationsArray,
            locationsToReadArray, locationsNotToReadArray);

        for (int i = 0; i < erasedLocationsArray.length; i++) {
          if (erasedLocationsArray[i] == erasedLocationToFix) {
            buffer = writeBufs[i];
            validateBlockChecksum(srcFile.toUri().getPath(), blockIdx, buffer);
            bufferLen = Math.min(bufSize, limit - streamOffset);
            position = 0;
            break;
          }
        }
      }
    }

    private void validateBlockChecksum(String src, int blockIndex,
        byte[] buffer) throws IOException {
      long checksum = ((DistributedFileSystem) srcFs).getClient()
          .getBlockChecksum(src, blockIndex);
      CRC32 crc = new CRC32();
      crc.update(buffer);
      if (checksum != crc.getValue()) {
        throw new BlockChecksumException(
            "Repair failed. Checksum of repaired block differs from original");
      }
    }

    /**
     * make sure we have the correct decoding data in the buffer.
     *
     * @throws java.io.IOException
     */
    private void checkBuffer() throws IOException {
      while (streamOffset <= limit) {
        try {
          init();
          break;
        } catch (IOException e) {
          if (e instanceof TooManyErasedLocations ||
              e instanceof BlockChecksumException) {
            throw e;
          }
          // Re-create inputs from the new erased locations.
          if (parallelReader != null) {
            parallelReader.shutdown();
            parallelReader = null;
          }
          if (inputs != null) {
            RaidUtils.closeStreams(inputs);
          }
        }
      }
    }

    @Override
    public int read() throws IOException {
      
      checkBuffer();
      if (null == buffer) {
        return -1;
      }

      int result = buffer[position] & 0xff;
      position++;
      streamOffset++;
      currentOffset++;

      return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }


    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      long startTime = System.currentTimeMillis();
      dfsNumRead = 0;
      
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      
      int numRead = 0;
      while (numRead < len) {
        try {
          checkBuffer();
        } catch (IOException e) {
          long delay = System.currentTimeMillis() - startTime;
          logRaidReconstructionMetrics("FAILURE", 0, codec, delay,
              erasedLocations.size(), dfsNumRead, srcFile, errorOffset,
              LOGTYPES.ONLINE_RECONSTRUCTION, srcFs);
          throw e;
        }

        if (null == buffer) {
          if (numRead > 0) {
            logRaidReconstructionMetrics("SUCCESS", (int) numRead, codec,
                System.currentTimeMillis() - startTime, erasedLocations.size(),
                dfsNumRead, srcFile, errorOffset,
                LOGTYPES.ONLINE_RECONSTRUCTION, srcFs);
            return (int) numRead;
          }
          return -1;
        }
        
        int numBytesToCopy =
            (int) Math.min(bufferLen - position, len - numRead);
        System.arraycopy(buffer, position, b, off, numBytesToCopy);
        position += numBytesToCopy;
        currentOffset += numBytesToCopy;
        streamOffset += numBytesToCopy;
        off += numBytesToCopy;
        numRead += numBytesToCopy;
      }

      if (numRead > 0) {
        logRaidReconstructionMetrics("SUCCESS", numRead, codec,
            System.currentTimeMillis() - startTime, erasedLocations.size(),
            dfsNumRead, srcFile, errorOffset, LOGTYPES.ONLINE_RECONSTRUCTION,
            srcFs);
      }
      return (int) numRead;
    }
    
    @Override
    public void close() throws IOException {
      if (parallelReader != null) {
        parallelReader.shutdown();
        parallelReader = null;
      }
      if (inputs != null) {
        RaidUtils.closeStreams(inputs);
      }
      super.close();
    }
  }
}


