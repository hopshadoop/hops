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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ***********************************************
 * A Block is a Hadoop FS primitive, identified by a
 * long.
 * <p/>
 * ************************************************
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Block implements Writable, Comparable<Block> {
  public static final String BLOCK_FILE_PREFIX = "blk_";
  public static final String METADATA_EXTENSION = ".meta";

  static {                                      // register a ctor
    WritableFactories.setFactory(Block.class, new WritableFactory() {
          @Override
          public Writable newInstance() {
            return new Block();
          }
        });
  }

  public static final Pattern blockFilePattern =
      Pattern.compile(BLOCK_FILE_PREFIX + "(-??\\d++)$");
  public static final Pattern metaFilePattern = Pattern.compile(
      BLOCK_FILE_PREFIX + "(-??\\d++)_(\\d++)\\" + METADATA_EXTENSION + "$");

  public static boolean isBlockFilename(File f) {
    String name = f.getName();
    return blockFilePattern.matcher(name).matches();
  }

  public static long filename2id(String name) {
    Matcher m = blockFilePattern.matcher(name);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  public static boolean isMetaFilename(String name) {
    return metaFilePattern.matcher(name).matches();
  }

  /**
   * Get generation stamp from the name of the metafile name
   */
  public static long getGenerationStamp(String metaFile) {
    Matcher m = metaFilePattern.matcher(metaFile);
    return m.matches() ? Long.parseLong(m.group(2)) :
        GenerationStamp.GRANDFATHER_GENERATION_STAMP;
  }

  /**
   * Get the blockId from the name of the metafile name
   */
  public static long getBlockId(String metaFile) {
    Matcher m = metaFilePattern.matcher(metaFile);
    return m.matches() ? Long.parseLong(m.group(1)) : 0;
  }

  private static long NON_EXISTING_BLK_ID = Long.MIN_VALUE;
  private long blockId;
  private long numBytes;
  private long generationStamp;

  public Block() {
    this(NON_EXISTING_BLK_ID, 0, 0);
  }

  public Block(final long blkid, final long len, final long generationStamp) {
    setNoPersistance(blkid, len, generationStamp);
  }

  public Block(final long blkid) {
    this(blkid, 0, GenerationStamp.GRANDFATHER_GENERATION_STAMP);
  }

  public Block(Block blk) {
    this(blk.blockId, blk.numBytes, blk.generationStamp);
  }

  public void setNoPersistance(long blkid, long len, long genStamp) {
    this.blockId = blkid;
    this.numBytes = len;
    this.generationStamp = genStamp;
  }

  /**
   */
  public long getBlockId() {
    return blockId;
  }
  
  public void setBlockIdNoPersistance(long bid) {
    blockId = bid;
  }

  /**
   */
  public String getBlockName() {
    return BLOCK_FILE_PREFIX + String.valueOf(blockId);
  }

  /**
   */
  public long getNumBytes() {
    return numBytes;
  }

  public void setNumBytesNoPersistance(long len) {
    this.numBytes = len;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }
  
  public void setGenerationStampNoPersistance(long stamp) {
    generationStamp = stamp;
  }

  /**
   */
  @Override
  public String toString() {
    return getBlockName() + "_" + getGenerationStamp();
  }

  /////////////////////////////////////
  // Writable
  /////////////////////////////////////
  @Override // Writable
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override // Writable
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }
  
  final void writeHelper(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(numBytes);
    out.writeLong(generationStamp);
  }
  
  final void readHelper(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.numBytes = in.readLong();
    this.generationStamp = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected block size: " + numBytes);
    }
  }
  
  // write only the identifier part of the block
  public void writeId(DataOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeLong(generationStamp);
  }

  // Read only the identifier part of the block
  public void readId(DataInput in) throws IOException {
    this.blockId = in.readLong();
    this.generationStamp = in.readLong();
  }

  @Override // Comparable
  public int compareTo(Block b) {
    return blockId < b.blockId ? -1 : blockId > b.blockId ? 1 : 0;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Block)) {
      return false;
    }
    return compareTo((Block) o) == 0;
  }
  
  /**
   * @return true if the two blocks have the same block ID and the same
   * generation stamp, or if both blocks are null.
   */
  public static boolean matchingIdAndGenStamp(Block a, Block b) {
    if (a == b) {
      return true; // same block, or both null
    }
    if (a == null || b == null) {
      return false; // only one null
    }
    return a.blockId == b.blockId && a.generationStamp == b.generationStamp;
  }

  @Override // Object
  public int hashCode() {
    //GenerationStamp is IRRELEVANT and should not be used here
    return (int) (blockId ^ (blockId >>> 32));
  }
}
