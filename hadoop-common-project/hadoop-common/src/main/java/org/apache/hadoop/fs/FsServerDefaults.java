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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;

/****************************************************
 * Provides server default configuration values to clients.
 * 
 ****************************************************/
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FsServerDefaults implements Writable {

  static { // register a ctor
    WritableFactories.setFactory(FsServerDefaults.class, new WritableFactory() {
      @Override
      public Writable newInstance() {
        return new FsServerDefaults();
      }
    });
  }

  private long blockSize;
  private int bytesPerChecksum;
  private int writePacketSize;
  private short replication;
  private int fileBufferSize;
  private boolean encryptDataTransfer;
  private long trashInterval;
  private DataChecksum.Type checksumType;
  private String keyProviderUri;
  private byte storagepolicyId;
  private boolean quotaEnabled;

  public FsServerDefaults() {
  }

  public FsServerDefaults(long blockSize, int bytesPerChecksum,
      int writePacketSize, short replication, int fileBufferSize,
      boolean encryptDataTransfer, long trashInterval,
      DataChecksum.Type checksumType, boolean quotaEnabled) {
    this(blockSize, bytesPerChecksum, writePacketSize, replication,
        fileBufferSize, encryptDataTransfer, trashInterval, checksumType,
        null, (byte) 0, quotaEnabled);
  }

  public FsServerDefaults(long blockSize, int bytesPerChecksum,
      int writePacketSize, short replication, int fileBufferSize,
      boolean encryptDataTransfer, long trashInterval,
      DataChecksum.Type checksumType, String keyProviderUri, boolean quotaEnabled) {
    this(blockSize, bytesPerChecksum, writePacketSize, replication,
        fileBufferSize, encryptDataTransfer, trashInterval, checksumType,
        keyProviderUri, (byte) 0, quotaEnabled);
  }

  public FsServerDefaults(long blockSize, int bytesPerChecksum,
      int writePacketSize, short replication, int fileBufferSize,
      boolean encryptDataTransfer, long trashInterval,
      DataChecksum.Type checksumType,
      String keyProviderUri, byte storagepolicy, boolean quotaEnabled) {
    this.blockSize = blockSize;
    this.bytesPerChecksum = bytesPerChecksum;
    this.writePacketSize = writePacketSize;
    this.replication = replication;
    this.fileBufferSize = fileBufferSize;
    this.encryptDataTransfer = encryptDataTransfer;
    this.trashInterval = trashInterval;
    this.checksumType = checksumType;
    this.keyProviderUri = keyProviderUri;
    this.storagepolicyId = storagepolicy;
    this.quotaEnabled = quotaEnabled;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  public int getWritePacketSize() {
    return writePacketSize;
  }

  public short getReplication() {
    return replication;
  }

  public int getFileBufferSize() {
    return fileBufferSize;
  }
  
  public boolean getEncryptDataTransfer() {
    return encryptDataTransfer;
  }

  public long getTrashInterval() {
    return trashInterval;
  }

  public DataChecksum.Type getChecksumType() {
    return checksumType;
  }

  public boolean getQuotaEnabled(){
    return quotaEnabled;
  }
  
  /* null means old style namenode.
   * "" (empty string) means namenode is upgraded but EZ is not supported.
   * some string means that value is the key provider.
   */
  public String getKeyProviderUri() {
    return keyProviderUri;
  }

  public byte getDefaultStoragePolicyId() {
    return storagepolicyId;
  }

  // /////////////////////////////////////////
  // Writable
  // /////////////////////////////////////////
  @Override
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(blockSize);
    out.writeInt(bytesPerChecksum);
    out.writeInt(writePacketSize);
    out.writeShort(replication);
    out.writeInt(fileBufferSize);
    out.writeBoolean(quotaEnabled);
    WritableUtils.writeEnum(out, checksumType);
    out.writeByte(storagepolicyId);
  }

  @Override
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    blockSize = in.readLong();
    bytesPerChecksum = in.readInt();
    writePacketSize = in.readInt();
    replication = in.readShort();
    fileBufferSize = in.readInt();
    quotaEnabled = in.readBoolean();
    checksumType = WritableUtils.readEnum(in, DataChecksum.Type.class);
    storagepolicyId = in.readByte();
  }
}
