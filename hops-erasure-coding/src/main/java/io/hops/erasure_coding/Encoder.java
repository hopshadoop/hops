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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ErasureCodingFileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Represents a generic encoder that can generate a parity file for a source
 * file.
 */
public class Encoder {
  public static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.raid.Encoder");
  public static final int DEFAULT_PARALLELISM = 4;
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected byte[][] writeBufs;

  /**
   * A class that acts as a sink for data, similar to /dev/null.
   */
  static class NullOutputStream extends OutputStream {
    public void write(byte[] b) throws IOException {
    }

    public void write(int b) throws IOException {
    }

    public void write(byte[] b, int off, int len) throws IOException {
    }
  }

  Encoder(Configuration conf, Codec codec) {
    this.conf = conf;
    this.parallelism =
        conf.getInt("raid.encoder.parallelism", DEFAULT_PARALLELISM);
    this.codec = codec;
    this.code = codec.createErasureCode(conf);
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.encoder.bufsize", 1024 * 1024);
    this.writeBufs = new byte[codec.parityLength][];
    allocateBuffers();
  }

  private void allocateBuffers() {
    for (int i = 0; i < codec.parityLength; i++) {
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
  
  /**
   * The interface to use to generate a parity file.
   * This method can be called multiple times with the same Encoder object,
   * thus allowing reuse of the buffers allocated by the Encoder object.
   *
   * @param fs
   *     The filesystem containing the source file.
   * @param srcFile
   *     The source file.
   * @param parityFile
   *     The parity file to be generated.
   */
  public void encodeFile(Configuration jobConf, FileSystem fs, Path srcFile,
      FileSystem parityFs, Path parityFile, short parityRepl, long numStripes,
      long blockSize, Progressable reporter, StripeReader sReader,
      Path copyPath, FSDataOutputStream copy)
      throws IOException {
    long expectedParityBlocks = numStripes * codec.parityLength;
    long expectedParityFileSize = numStripes * blockSize * codec.parityLength;

    if (!parityFs.mkdirs(parityFile.getParent())) {
      throw new IOException(
          "Could not create parent dir " + parityFile.getParent());
    }
    // delete destination if exists
    if (parityFs.exists(parityFile)) {
      parityFs.delete(parityFile, false);
    }

    // Writing out a large parity file at replication 1 is difficult since
    // some datanode could die and we would not be able to close() the file.
    // So write at replication 2 and then reduce it after close() succeeds.
    short tmpRepl = parityRepl;
    if (expectedParityBlocks >=
        conf.getInt("raid.encoder.largeparity.blocks", 20)) {
      if (parityRepl == 1) {
        tmpRepl = 2;
      }
    }
    FSDataOutputStream out = parityFs.create(parityFile, true,
        conf.getInt("io.file.buffer.size", 64 * 1024), tmpRepl, blockSize);

    DFSOutputStream dfsOut = (DFSOutputStream) out.getWrappedStream();
    dfsOut.enableParityStream(codec.getStripeLength(), codec.getParityLength(),
        copy == null ? srcFile.toUri().getPath() : null);

    try {
      encodeFileToStream(fs, srcFile, parityFile, sReader, blockSize, out,
          reporter, copyPath, copy);
      out.close();
      out = null;
      LOG.info("Wrote parity file " + parityFile);
      FileStatus tmpStat = parityFs.getFileStatus(parityFile);
      if (tmpStat.getLen() != expectedParityFileSize) {
        throw new IOException("Expected parity size " + expectedParityFileSize +
            " does not match actual " + tmpStat.getLen());
      }
      if (tmpRepl > parityRepl) {
        parityFs.setReplication(parityFile, parityRepl);
      }
      LOG.info("Wrote parity file " + parityFile);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Recovers a corrupt block in a parity file to a local file.
   * <p/>
   * The encoder generates codec.parityLength parity blocks for a source file
   * stripe.
   * Since we want only one of the parity blocks, this function creates
   * null outputs for the blocks to be discarded.
   *
   * @param fs
   *     The filesystem in which both srcFile and parityFile reside.
   * @param srcStat
   *     The FileStatus of source file.
   * @param blockSize
   *     The block size for the parity files.
   * @param corruptOffset
   *     The location of corruption in the parity file.
   * @param localBlockFile
   *     The destination for the reovered block.
   * @param progress
   *     A reporter for progress.
   */
  public void recoverParityBlockToFile(FileSystem fs, FileStatus srcStat,
      long blockSize, Path parityFile, long corruptOffset, File localBlockFile,
      Progressable progress) throws IOException {
    OutputStream out = new FileOutputStream(localBlockFile);
    try {
      recoverParityBlockToStream(fs, srcStat, blockSize, parityFile,
          corruptOffset, out, progress);
    } finally {
      out.close();
    }
  }

  /**
   * Recovers a corrupt block in a parity file to a local file.
   * <p/>
   * The encoder generates codec.parityLength parity blocks for a source file
   * stripe.
   * Since we want only one of the parity blocks, this function creates
   * null outputs for the blocks to be discarded.
   *
   * @param fs
   *     The filesystem in which both srcFile and parityFile reside.
   * @param srcStat
   *     fileStatus of The source file.
   * @param blockSize
   *     The block size for the parity files.
   * @param corruptOffset
   *     The location of corruption in the parity file.
   * @param out
   *     The destination for the reovered block.
   * @param progress
   *     A reporter for progress.
   */
  public void recoverParityBlockToStream(FileSystem fs, FileStatus srcStat,
      long blockSize, Path parityFile, long corruptOffset, OutputStream out,
      Progressable progress) throws IOException {
    LOG.info("Recovering parity block" + parityFile + ":" + corruptOffset);
    Path srcFile = srcStat.getPath();
    // Get the start offset of the corrupt block.
    corruptOffset = (corruptOffset / blockSize) * blockSize;
    // Output streams to each block in the parity file stripe.
    OutputStream[] outs = new OutputStream[codec.parityLength];
    long indexOfCorruptBlockInParityStripe =
        (corruptOffset / blockSize) % codec.parityLength;
    LOG.info("Index of corrupt block in parity stripe: " +
        indexOfCorruptBlockInParityStripe);
    // Create a real output stream for the block we want to recover,
    // and create null streams for the rest.
    for (int i = 0; i < codec.parityLength; i++) {
      if (indexOfCorruptBlockInParityStripe == i) {
        outs[i] = out;
      } else {
        outs[i] = new NullOutputStream();
      }
    }
    // Get the stripe index and start offset of stripe.
    long stripeIdx = corruptOffset / (codec.parityLength * blockSize);
    StripeReader sReader = StripeReader
        .getStripeReader(codec, conf, blockSize, fs, stripeIdx, srcStat);
    // Get input streams to each block in the source file stripe.
    assert sReader.hasNext() == true;
    InputStream[] blocks = sReader.getNextStripeInputs();
    LOG.info("Starting recovery by using source stripe " +
        srcFile + ": stripe " + stripeIdx);
    try {
      // Read the data from the blocks and write to the parity file.
      encodeStripe(fs, srcFile, parityFile, blocks, blockSize, outs, progress);
    } finally {
      RaidUtils.closeStreams(blocks);
    }
  }

  /**
   * Recovers a corrupt block in a parity file to an output stream.
   * <p/>
   * The encoder generates codec.parityLength parity blocks for a source file
   * stripe.
   * Since there is only one output provided, some blocks are written out to
   * files before being written out to the output.
   *
   * @param blockSize
   *     The block size for the source/parity files.
   * @param out
   *     The destination for the reovered block.
   */
  private void encodeFileToStream(FileSystem fs, Path sourceFile,
      Path parityFile, StripeReader sReader, long blockSize,
      FSDataOutputStream out, Progressable reporter,
      Path copyPath, FSDataOutputStream copy) throws IOException {
    OutputStream[] tmpOuts = new OutputStream[codec.parityLength];
    // One parity block can be written directly to out, rest to local files.
    tmpOuts[0] = out;
    File[] tmpFiles = new File[codec.parityLength - 1];
    for (int i = 0; i < codec.parityLength - 1; i++) {
      tmpFiles[i] = File.createTempFile("parity", "_" + i);
      LOG.info("Created tmp file " + tmpFiles[i]);
      tmpFiles[i].deleteOnExit();
    }

    OutputStream[] copyOuts = null;
    File[] tmpCopyFiles = null;
    if (copy != null) {
      tmpCopyFiles = new File[codec.stripeLength];
      copyOuts = new OutputStream[codec.stripeLength];
      for (int i = 0; i < codec.stripeLength; i++) {
        tmpCopyFiles[i] = File.createTempFile("copy", "_" + i);
        LOG.info("Created copy file " + tmpCopyFiles[i]);
        tmpCopyFiles[i].deleteOnExit();
      }
    }

    try {
      // Loop over stripes
      int stripe = 0;
      while (sReader.hasNext()) {
        reporter.progress();
        // Create input streams for blocks in the stripe.
        InputStream[] blocks = sReader.getNextStripeInputs();
        try {
          // Create output streams to the temp files.
          for (int i = 0; i < codec.parityLength - 1; i++) {
            tmpOuts[i + 1] = new FileOutputStream(tmpFiles[i]);
          }
          if (copy != null) {
            for (int i = 0; i < codec.stripeLength; i++) {
              copyOuts[i] = new FileOutputStream(tmpCopyFiles[i]);
            }
          }
          // Call the implementation of encoding.
          encodeStripe(fs, sourceFile, parityFile, blocks, blockSize, tmpOuts,
              reporter, true, stripe, copyPath, copyOuts);
          stripe++;
        } finally {
          RaidUtils.closeStreams(blocks);
        }
        // Close output streams to the temp files and write the temp files
        // to the output provided.
        for (int i = 0; i < codec.parityLength - 1; i++) {
          tmpOuts[i + 1].close();
          tmpOuts[i + 1] = null;
          InputStream in = new FileInputStream(tmpFiles[i]);
          RaidUtils.copyBytes(in, out, writeBufs[i], blockSize);
          reporter.progress();
        }
        if (copy != null) {
          out.hflush();
          DFSOutputStream sourceOut = (DFSOutputStream) copy.getWrappedStream();
          DFSOutputStream parityOut = (DFSOutputStream) out.getWrappedStream();
          sourceOut.setParityStripeNodesForNextStripe(parityOut.getUsedNodes());

          for (int i = 0; i < codec.stripeLength; i++) {
            copyOuts[i].close();
            copyOuts[i] = null;
            InputStream in = new FileInputStream(tmpCopyFiles[i]);
            RaidUtils.copyBytes(in, copy, writeBufs[0], blockSize);
            reporter.progress();
          }
          copy.hflush();
        }
      }
    } finally {
      for (int i = 0; i < codec.parityLength - 1; i++) {
        if (tmpOuts[i + 1] != null) {
          tmpOuts[i + 1].close();
        }
        tmpFiles[i].delete();
        LOG.info("Deleted tmp file " + tmpFiles[i]);
      }
      if (copy != null) {
        for (int i = 0; i < codec.stripeLength; i++) {
          if (copyOuts[i] != null) {
            copyOuts[i].close();
          }
          tmpCopyFiles[i].delete();
          LOG.info("Deleted tmp file " + tmpCopyFiles[i]);
        }
      }
    }
  }

  void encodeStripe(FileSystem fs, Path sourceFile, Path parityFile,
      InputStream[] blocks, long blockSize, OutputStream[] outs,
      Progressable reporter) throws IOException {
    encodeStripe(fs, sourceFile, parityFile, blocks, blockSize, outs, reporter,
        false, 0, null, null);
  }

  /**
   * Wraps around encodeStripeImpl in order to configure buffers.
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void encodeStripe(FileSystem fs, Path sourceFile, Path parityFile,
      InputStream[] blocks, long blockSize, OutputStream[] outs,
      Progressable reporter, boolean computeBlockChecksum, int stripe,
      Path copyPath, OutputStream[] copyOuts) throws IOException {
    configureBuffers(blockSize);
    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader =
        new ParallelStreamReader(reporter, blocks, bufSize, parallelism,
            boundedBufferCapacity, blockSize);
    parallelReader.start();

    Checksum[] sourceChecksums = null;
    Checksum[] parityChecksums = null;
    if (computeBlockChecksum) {
      sourceChecksums = new Checksum[codec.stripeLength];
      for (int i = 0; i < sourceChecksums.length; i++) {
        sourceChecksums[i] = new CRC32();
      }
      parityChecksums = new Checksum[codec.parityLength];
      for (int i = 0; i < parityChecksums.length; i++) {
        parityChecksums[i] = new CRC32();
      }
    }
    try {
      for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
        ParallelStreamReader.ReadResult readResult = null;
        try {
          readResult = parallelReader.getReadResult();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting for read result");
        }
        // Cannot tolerate any IO errors.
        IOException readEx = readResult.getException();
        if (readEx != null) {
          throw readEx;
        }

        if (computeBlockChecksum) {
          updateChecksums(sourceChecksums, readResult.readBufs);
        }
        if (copyOuts != null) {
          for (int i = 0; i < readResult.readBufs.length; i++) {
            copyOuts[i].write(readResult.readBufs[i], 0, readResult.numRead[i]);
          }
        }
        code.encodeBulk(readResult.readBufs, writeBufs);
        reporter.progress();

        // Now that we have some data to write, send it to the temp files.
        for (int i = 0; i < codec.parityLength; i++) {
          outs[i].write(writeBufs[i], 0, bufSize);
          if (computeBlockChecksum) {
            parityChecksums[i].update(writeBufs[i], 0, bufSize);
          }
          reporter.progress();
        }
      }
      DistributedFileSystem dfs =(DistributedFileSystem)
          (fs instanceof ErasureCodingFileSystem ?
              ((ErasureCodingFileSystem) fs).getFileSystem() : fs);
      sendChecksums(dfs, copyPath == null ? sourceFile : copyPath,
          sourceChecksums, stripe, codec.stripeLength);
      sendChecksums(dfs, parityFile, parityChecksums, stripe,
          codec.parityLength);
    } finally {
      parallelReader.shutdown();
    }
  }

  private void updateChecksums(Checksum[] checksums, byte[][] buffs) {
    for (int i = 0; i < checksums.length; i++) {
      checksums[i].update(buffs[i], 0, buffs[0].length);
    }
  }

  private void sendChecksums(DistributedFileSystem dfs, Path file,
      Checksum[] checksums, int stripe, int length) throws IOException {
    if (checksums == null) {
      return;
    }
    DFSClient dfsClient = dfs.getClient();
    int firstBlockIndex = stripe * length;
    for (int i = 0; i < length; i++) {
      int blockIndex = firstBlockIndex + i;
      dfsClient.addBlockChecksum(file.toUri().getPath(), blockIndex,
          checksums[i].getValue());
    }
  }
}
