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
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class StripeReader {
  public static final Log LOG = LogFactory.getLog(BaseEncodingManager.class);
  Codec codec;
  Configuration conf;
  FileSystem fs;
  long stripeStartIdx;
  int bufferSize;
  
  public static class LocationPair {
    private int stripeIdx;
    private int blockIdxInStripe;

    public LocationPair(int stripeIdx, int blockIdxInStripe) {
      this.stripeIdx = stripeIdx;
      this.blockIdxInStripe = blockIdxInStripe;
    }
    
    int getStripeIdx() {
      return stripeIdx;
    }
    
    int getBlockIdxInStripe() {
      return blockIdxInStripe;
    }
  }
  
  public StripeReader(Configuration conf, Codec codec, FileSystem fs,
      long stripeStartIdx) {
    this.codec = codec;
    this.conf = conf;
    this.fs = fs;
    this.stripeStartIdx = stripeStartIdx;
    this.bufferSize = conf.getInt("io.file.buffer.size", 64 * 1024);
  }

  /**
   * Has next stripe to read?
   */
  public abstract boolean hasNext();
  
  /**
   * Get the input streams for the next stripe
   */
  public abstract InputStream[] getNextStripeInputs() throws IOException;
  
  /**
   * Builds (codec.stripeLength + codec.parityLength) inputs given some erased
   * locations.
   * Outputs:
   * - the array of input streams @param inputs
   * - the list of erased locations @param erasedLocations.
   * - the list of locations that are not read @param locationsToNotRead.
   */
  public InputStream[] buildInputs(FileSystem srcFs, Path srcFile,
      FileStatus srcStat, FileSystem parityFs, Path parityFile,
      FileStatus parityStat, int stripeIdx, long offsetInBlock,
      List<Integer> erasedLocations, List<Integer> locationsToRead,
      ErasureCode code) throws IOException {
    InputStream[] inputs =
        new InputStream[codec.stripeLength + codec.parityLength];
    boolean redo = false;
    do {
      /*
       * In the first iteration locationsToRead is empty.
       * It is populated according to locationsToReadForDecode.
       * In consecutive iterations (if a stream failed to open)
       * the list is cleared and re-populated.
       */
      locationsToRead.clear();
      locationsToRead.addAll(code.locationsToReadForDecode(erasedLocations));

      for (int i = 0; i < inputs.length; i++) {
        boolean isErased = (erasedLocations.indexOf(i) != -1);
        boolean shouldRead = (locationsToRead.indexOf(i) != -1);
        try {
          InputStream stm = null;
          if (isErased || !shouldRead) {
            if (isErased) {
              LOG.info("Location " + i + " is erased, using zeros");
            } else {
              LOG.info("Location " + i + " need not be read, using zeros");
            }

            stm = new RaidUtils.ZeroInputStream(srcStat.getBlockSize() *
                ((i < codec.parityLength) ? stripeIdx * codec.parityLength + i :
                    stripeIdx * codec.stripeLength + i - codec.parityLength));
          } else {
            stm = buildOneInput(i, offsetInBlock, srcFs, srcFile, srcStat,
                parityFs, parityFile, parityStat);
          }
          inputs[i] = stm;
        } catch (IOException e) {
          if (e instanceof BlockMissingException ||
              e instanceof ChecksumException) {
            erasedLocations.add(i);
            redo = true;
            RaidUtils.closeStreams(inputs);
            break;
          } else {
            throw e;
          }
        }
      }
    } while (redo);
    return inputs;
  }
  
  /**
   * Given a block in the file and specific codec, return the LocationPair
   * object which contains id of the stripe it belongs to and its
   * location in the stripe
   */
  public static LocationPair getBlockLocation(Codec codec, int blockIdxInFile)
      throws IOException {
    int stripeIdx;
    int blockIdxInStripe;
    int blockIdx = blockIdxInFile;
    stripeIdx = blockIdx / codec.stripeLength;
    blockIdxInStripe = blockIdx % codec.stripeLength;
    return new LocationPair(stripeIdx, blockIdxInStripe);
  }
  
  public static LocationPair getParityBlockLocation(Codec codec,
      final int blockIdxInFile) {
    
    int stripeIdx = blockIdxInFile / codec.parityLength;
    int blockIdxInStripe = blockIdxInFile % codec.parityLength;
    
    return new LocationPair(stripeIdx, blockIdxInStripe);
  }
  
  public static StripeReader getStripeReader(Codec codec, Configuration conf,
      long blockSize, FileSystem fs, long stripeIdx, FileStatus srcStat)
      throws IOException {
    return new FileStripeReader(conf, blockSize, codec, fs, stripeIdx,
        srcStat.getPath(), srcStat.getLen());
  }
  
  protected abstract InputStream buildOneInput(int locationIndex,
      long offsetInBlock, FileSystem srcFs, Path srcFile, FileStatus srcStat,
      FileSystem parityFs, Path parityFile, FileStatus parityStat)
      throws IOException;
  
  protected InputStream getParityFileInput(int locationIndex, Path parityFile,
      FileSystem parityFs, FileStatus parityStat, long offsetInBlock)
      throws IOException {
    // Dealing with a parity file here.
    int parityBlockIdx =
        (int) (codec.parityLength * stripeStartIdx + locationIndex);
    long offset = parityStat.getBlockSize() * parityBlockIdx + offsetInBlock;
    assert (offset < parityStat.getLen());
    LOG.info("Opening " + parityFile + ":" + offset +
        " for location " + locationIndex);
    FSDataInputStream s = parityFs
        .open(parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
    s.seek(offset);
    return s;
  }
}
