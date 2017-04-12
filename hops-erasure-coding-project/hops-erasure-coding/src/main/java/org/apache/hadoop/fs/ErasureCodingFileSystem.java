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

package org.apache.hadoop.fs;

import io.hops.erasure_coding.BaseEncodingManager;
import io.hops.erasure_coding.Codec;
import io.hops.erasure_coding.Decoder;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 * This is an implementation of the Hadoop Erasure Coding Filesystem. This
 * FileSystem
 * wraps an instance of the DistributedFileSystem.
 * If a file is corrupted, this FileSystem uses the parity blocks to
 * regenerate the bad block.
 */

public class ErasureCodingFileSystem extends FilterFileSystem {
  public static final int SKIP_BUF_SIZE = 2048;

  Configuration conf;

  ErasureCodingFileSystem() throws IOException {
  }

  ErasureCodingFileSystem(FileSystem fs) throws IOException {
    super(fs);
  }

  /* Initialize a Raid FileSystem
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    this.conf = conf;
    
    // init the codec from conf.
    Codec.initializeCodecs(conf);

    Class<?> clazz =
        conf.getClass("fs.raid.underlyingfs.impl", DistributedFileSystem.class);
    if (clazz == null) {
      throw new IOException("No FileSystem for fs.raid.underlyingfs.impl.");
    }

    this.fs = (FileSystem) ReflectionUtils.newInstance(clazz, null);
    super.initialize(name, conf);
  }

  /*
   * Returns the underlying filesystem
   */
  public FileSystem getFileSystem() throws IOException {
    return fs;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // We want to use RAID logic only on instance of DFS.
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem underlyingDfs = (DistributedFileSystem) fs;
      FileStatus fileStatus = underlyingDfs.getFileStatus(f);
      EncodingStatus encodingStatus =
          underlyingDfs.getEncodingStatus(f.toUri().getPath());
      if (encodingStatus.isEncoded()) {
        // Use underlying filesystem if file length is 0.
        final long fileSize = fileStatus.getLen();
        if (fileSize > 0) {
          System.out.println("Using recovery stream");
          return new ExtFSDataInputStream(conf, this, f, fileSize,
              fileStatus.getBlockSize(), bufferSize);
        }
        System.out.println("Wrong file size " + fileSize);
      }
      System.out.println("Wrong encoding status " + encodingStatus.getStatus());
    } else {
      System.out.println("Wrong file system");
    }
    return fs.open(f, bufferSize);
  }

  public void close() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch (IOException ie) {
        //this might already be closed, ignore
      }
    }
    super.close();
  }

  /**
   * Layered filesystem input stream. This input stream tries reading
   * from alternate locations if it encoumters read errors in the primary
   * location.
   */
  private static class ExtFSDataInputStream extends FSDataInputStream {

    private static class UnderlyingBlock {
      // File that holds this block. Need not be the same as outer file.
      public Path path;
      // Offset within path where this block starts.
      public long actualFileOffset;
      // Offset within the outer file where this block starts.
      public long originalFileOffset;
      // Length of the block (length <= blk sz of outer file).
      public long length;

      public UnderlyingBlock(Path path, long actualFileOffset,
          long originalFileOffset, long length) {
        this.path = path;
        this.actualFileOffset = actualFileOffset;
        this.originalFileOffset = originalFileOffset;
        this.length = length;
      }
    }

    /**
     * Create an input stream that wraps all the reads/positions/seeking.
     */
    private static class ExtFsInputStream extends FSInputStream {

      // Extents of "good" underlying data that can be read.
      private UnderlyingBlock[] underlyingBlocks;
      private long currentOffset;
      private FSDataInputStream currentStream;
      private Decoder.DecoderInputStream recoveryStream;
      private boolean useRecoveryStream;
      private UnderlyingBlock currentBlock;
      private byte[] oneBytebuff = new byte[1];
      private byte[] skipbuf = new byte[SKIP_BUF_SIZE];
      private int nextLocation;
      private ErasureCodingFileSystem lfs;
      private Path path;
      private final long fileSize;
      private final long blockSize;
      private final int buffersize;
      private final Configuration conf;
      private Configuration innerConf;

      ExtFsInputStream(Configuration conf, ErasureCodingFileSystem lfs,
          Path path, long fileSize, long blockSize, int buffersize)
          throws IOException {
        this.path = path;
        this.nextLocation = 0;
        // Construct array of blocks in file.
        this.blockSize = blockSize;
        this.fileSize = fileSize;
        long numBlocks = (this.fileSize % this.blockSize == 0) ?
            this.fileSize / this.blockSize : 1 + this.fileSize / this.blockSize;
        this.underlyingBlocks = new UnderlyingBlock[(int) numBlocks];
        for (int i = 0; i < numBlocks; i++) {
          long actualFileOffset = i * blockSize;
          long originalFileOffset = i * blockSize;
          long length = Math.min(blockSize, fileSize - originalFileOffset);
          this.underlyingBlocks[i] =
              new UnderlyingBlock(path, actualFileOffset, originalFileOffset,
                  length);
        }
        this.currentOffset = 0;
        this.currentBlock = null;
        this.buffersize = buffersize;
        this.conf = conf;
        this.lfs = lfs;
        
        // Initialize the "inner" conf, and cache this for all future uses.
        //Make sure we use DFS and not DistributedRaidFileSystem for unRaid.
        this.innerConf = new Configuration(conf);
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
            DistributedFileSystem.class);
        this.innerConf.set("fs.hdfs.impl", clazz.getName());
        // Disable caching so that a previously cached RaidDfs is not used.
        this.innerConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        
        // Open a stream to the first block.
        openCurrentStream();
      }

      private void closeCurrentStream() throws IOException {
        if (currentStream != null) {
          currentStream.close();
          currentStream = null;
        }
      }
      
      private void closeRecoveryStream() throws IOException {
        if (null != recoveryStream) {
          recoveryStream.close();
          recoveryStream = null;
        }
      }

      /**
       * Open a stream to the file containing the current block
       * and seek to the appropriate offset
       */
      private void openCurrentStream() throws IOException {
        
        if (recoveryStream != null &&
            recoveryStream.getCurrentOffset() == currentOffset &&
            recoveryStream.getAvailable() > 0) {
          useRecoveryStream = true;
          closeCurrentStream();
          return;
        } else {
          useRecoveryStream = false;
          closeRecoveryStream();
        }

        //if seek to the filelen + 1, block should be the last block
        int blockIdx =
            (currentOffset < fileSize) ? (int) (currentOffset / blockSize) :
                underlyingBlocks.length - 1;
        UnderlyingBlock block = underlyingBlocks[blockIdx];
        // If the current path is the same as we want.
        if (currentBlock == block ||
            currentBlock != null && currentBlock.path == block.path) {
          // If we have a valid stream, nothing to do.
          if (currentStream != null) {
            currentBlock = block;
            return;
          }
        } else {
          closeCurrentStream();
        }

        currentBlock = block;
        currentStream = lfs.fs.open(currentBlock.path, buffersize);
        long offset =
            block.actualFileOffset + (currentOffset - block.originalFileOffset);
        currentStream.seek(offset);
      }

      /**
       * Returns the number of bytes available in the current block.
       */
      private int blockAvailable() {
        return (int) (currentBlock.length -
            (currentOffset - currentBlock.originalFileOffset));
      }

      @Override
      public synchronized int available() throws IOException {
        // Application should not assume that any bytes are buffered here.
        nextLocation = 0;
        return Math.min(blockAvailable(), currentStream.available());
      }

      @Override
      public synchronized void close() throws IOException {
        closeCurrentStream();
        closeRecoveryStream();
        super.close();
      }

      @Override
      public boolean markSupported() {
        return false;
      }

      @Override
      public void mark(int readLimit) {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public void reset() throws IOException {
        // Mark and reset are not supported.
        nextLocation = 0;
      }

      @Override
      public synchronized int read() throws IOException {
        int value = read(oneBytebuff, 0, 1);
        if (value < 0) {
          return value;
        } else {
          return 0xFF & oneBytebuff[0];
        }
      }

      @Override
      public synchronized int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
      }

      @Override
      public synchronized int read(byte[] b, int offset, int len)
          throws IOException {
        if (currentOffset >= fileSize) {
          return -1;
        }
        openCurrentStream();
        int limit = Math.min(blockAvailable(), len);
        int value;
        if (useRecoveryStream) {
          value = recoveryStream.read(b, offset, limit);
        } else {
          try {
            value = currentStream.read(b, offset, limit);
          } catch (BlockMissingException e) {
            value = readViaCodec(b, offset, limit, blockAvailable(), e);
          } catch (ChecksumException e) {
            value = readViaCodec(b, offset, limit, blockAvailable(), e);
          }
        }

        currentOffset += value;
        nextLocation = 0;
        return value;
      }
      
      @Override
      public synchronized int read(long position, byte[] b, int offset, int len)
          throws IOException {
        long oldPos = currentOffset;
        seek(position);
        try {
          if (currentOffset >= fileSize) {
            return -1;
          }
          
          openCurrentStream();
          int limit = Math.min(blockAvailable(), len);
          int value;
          if (useRecoveryStream) {
            value = recoveryStream.read(b, offset, limit);
          } else {
            try {
              value = currentStream.read(b, offset, limit);
            } catch (BlockMissingException e) {
              value = readViaCodec(b, offset, limit, limit, e);
            } catch (ChecksumException e) {
              value = readViaCodec(b, offset, limit, limit, e);
            }
          }
          currentOffset += value;
          nextLocation = 0;
          return value;
        } finally {
          seek(oldPos);
        }
      }
      
      private int readViaCodec(byte[] b, int offset, int len, int streamLimit,
          IOException e) throws IOException {
        
        // generate the DecoderInputStream
        if (null == recoveryStream ||
            recoveryStream.getCurrentOffset() != currentOffset) {
          recoveryStream =
              getAlternateInputStream(e, currentOffset, streamLimit);
        }
        if (null == recoveryStream) {
          throw e;
        }
        
        try {
          int value = recoveryStream.read(b, offset, len);
          closeCurrentStream();
          return value;
        } catch (IOException ex) {
          throw e;
        }
      }

      @Override
      public synchronized long skip(long n) throws IOException {
        long skipped = 0;
        long startPos = getPos();
        while (skipped < n) {
          int toSkip = (int) Math.min(SKIP_BUF_SIZE, n - skipped);
          int val = read(skipbuf, 0, toSkip);
          if (val < 0) {
            break;
          }
          skipped += val;
        }
        nextLocation = 0;
        long newPos = getPos();
        if (newPos - startPos > n) {
          throw new IOException(
              "skip(" + n + ") went from " + startPos + " to " + newPos);
        }
        if (skipped != newPos - startPos) {
          throw new IOException(
              "skip(" + n + ") went from " + startPos + " to " + newPos +
                  " but skipped=" + skipped);
        }
        return skipped;
      }

      @Override
      public synchronized long getPos() throws IOException {
        nextLocation = 0;
        return currentOffset;
      }

      @Override
      public synchronized void seek(long pos) throws IOException {
        if (pos > fileSize) {
          throw new EOFException(
              "Cannot seek to " + pos + ", file length is " + fileSize);
        }
        if (pos != currentOffset) {
          closeCurrentStream();
          currentOffset = pos;
          openCurrentStream();
        }
        nextLocation = 0;
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        seek(targetPos);
        boolean value = currentStream.seekToNewSource(currentStream.getPos());
        nextLocation = 0;
        return value;
      }

      /**
       * position readable again.
       */
      @Override
      public void readFully(final long pos, byte[] b, int offset, int length)
          throws IOException {
        long oldPos = currentOffset;
        try {
          while (true) {
            // This loop retries reading until successful. Unrecoverable errors
            // cause exceptions.
            // currentOffset is changed by read().
            long curPos = pos;
            while (length > 0) {
              int n = read(curPos, b, offset, length);
              if (n < 0) {
                throw new IOException("Premature EOF");
              }
              offset += n;
              length -= n;
              curPos += n;
            }
            nextLocation = 0;
            return;
          }
        } finally {
          seek(oldPos);
        }
      }

      @Override
      public void readFully(long pos, byte[] b) throws IOException {
        readFully(pos, b, 0, b.length);
        nextLocation = 0;
      }

      /**
       * Extract good data from RAID
       *
       * @throws java.io.IOException
       *     if all alternate locations are exhausted
       */
      private Decoder.DecoderInputStream getAlternateInputStream(
          IOException curexp, long offset, final long readLimit)
          throws IOException {

        // Start offset of block.
        long corruptOffset = (offset / blockSize) * blockSize;


        long fileLen = this.lfs.getFileStatus(path).getLen();
        long limit = Math.min(readLimit, blockSize - (offset - corruptOffset));
        limit = Math.min(limit, fileLen);

        while (nextLocation < Codec.getCodecs().size()) {

          try {
            int idx = nextLocation++;
            Codec codec = Codec.getCodecs().get(idx);

            Decoder.DecoderInputStream recoveryStream = BaseEncodingManager
                .unRaidCorruptInputStream(innerConf, path, blockSize, offset,
                    limit);

            if (null != recoveryStream) {
              return recoveryStream;
            }

          } catch (Exception e) {
            LOG.info("Ignoring error in using alternate path " + path, e);
          }
        }
        LOG.warn("Could not reconstruct block " + path + ":" + offset);
        throw curexp;
      }

      /**
       * The name of the file system that is immediately below the
       * DistributedRaidFileSystem. This is specified by the
       * configuration parameter called fs.raid.underlyingfs.impl.
       * If this parameter is not specified in the configuration, then
       * the default class DistributedFileSystem is returned.
       *
       * @param conf
       *     the configuration object
       * @return the filesystem object immediately below DistributedRaidFileSystem
       * @throws java.io.IOException
       *     if all alternate locations are exhausted
       */
      private FileSystem getUnderlyingFileSystem(Configuration conf) {
        Class<?> clazz = conf.getClass("fs.raid.underlyingfs.impl",
            DistributedFileSystem.class);
        FileSystem fs = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
        return fs;
      }
    }

    /**
     * constructor for ext input stream.
     *
     * @param lfs
     *     the underlying filesystem
     * @param p
     *     the path in the underlying file system
     * @param buffersize
     *     the size of IO
     * @throws java.io.IOException
     */
    public ExtFSDataInputStream(Configuration conf, ErasureCodingFileSystem lfs,
        Path p, long fileSize, long blockSize, int buffersize)
        throws IOException {
      super(
          new ExtFsInputStream(conf, lfs, p, fileSize, blockSize, buffersize));
    }
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f,
      final PathFilter filter) throws FileNotFoundException, IOException {
    return fs.listLocatedStatus(f, filter);
  }
}
