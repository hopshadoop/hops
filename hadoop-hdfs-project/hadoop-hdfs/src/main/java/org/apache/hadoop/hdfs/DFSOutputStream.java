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
package org.apache.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import io.hops.erasure_coding.Codec;
import io.hops.exception.OutOfDBExtentsException;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.TraceScope;


/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into the dataQueue of DataStreamer. DataStreamer is a
 * thread that picks up packets from the dataQueue and sends it to
 * the first datanode in the pipeline.
 *
 ****************************************************************/
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer
    implements Syncable, CanSetDropBehind, StreamCapabilities {

  public static final Log LOG = LogFactory.getLog(DFSOutputStream.class);
  
  /**
   * Number of times to retry creating a file when there are transient 
   * errors (typically related to encryption zones and KeyProvider operations).
   */
  @VisibleForTesting
  static final int CREATE_RETRY_COUNT = 10;
  @VisibleForTesting
  static CryptoProtocolVersion[] SUPPORTED_CRYPTO_VERSIONS =
      CryptoProtocolVersion.supported();
  
  protected final DFSClient dfsClient;
  protected final ByteArrayManager byteArrayManager;
  // closed is accessed by different threads under different locks.
  protected volatile boolean closed = false;

  protected final String src;
  protected final long fileId;
  protected final long blockSize;
  protected final DataChecksum checksum;

  protected DFSPacket currentPacket = null;
  protected DataStreamer streamer;
  protected int packetSize = 0; // write packet size, not including the header.
  protected int chunksPerPacket = 0;
  protected long lastFlushOffset = 0; // offset when flush was invoked
  private long initialFileSize = 0; // at time of file open
  private final short blockReplication; // replication factor of file
  protected boolean shouldSyncBlock = false; // force blocks to disk upon close
  protected final AtomicReference<CachingStrategy> cachingStrategy;
  private FileEncryptionInfo fileEncryptionInfo;
  
  private boolean singleBlock = false;

  private static BlockStoragePolicySuite policySuite = BlockStoragePolicySuite.createDefaultSuite();
  
  /** Use {@link ByteArrayManager} to create buffer for non-heartbeat packets.*/
  protected DFSPacket createPacket(int packetSize, int chunksPerPkt, long offsetInBlock,
      long seqno, boolean lastPacketInBlock) throws InterruptedIOException {
    final byte[] buf;
    final int bufferSize = PacketHeader.PKT_MAX_HEADER_LEN + packetSize;

    try {
      buf = byteArrayManager.newByteArray(bufferSize);
    } catch (InterruptedException ie) {
      final InterruptedIOException iioe = new InterruptedIOException(
          "seqno=" + seqno);
      iioe.initCause(ie);
      throw iioe;
    }

    return new DFSPacket(buf, chunksPerPkt, offsetInBlock, seqno,
                         checksum.getChecksumSize(), lastPacketInBlock);
  }

  @Override
  protected void checkClosed() throws IOException {
    if (isClosed()) {
      streamer.getLastException().throwException4Close();
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  @VisibleForTesting
  public synchronized DatanodeInfo[] getPipeline() {
    if (streamer.streamerClosed()) {
      return null;
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return null;
    }
    DatanodeInfo[] value = new DatanodeInfo[currentNodes.length];
    for (int i = 0; i < currentNodes.length; i++) {
      value[i] = currentNodes[i];
    }
    return value;
  }
 
  private DFSOutputStream(DFSClient dfsClient, String src, Progressable progress,
      HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    super(checksum);
    this.dfsClient = dfsClient;
    this.src = src;
    this.fileId = stat.getFileId();
    this.blockSize = stat.getBlockSize();
    this.blockReplication = stat.getReplication();
    this.fileEncryptionInfo = stat.getFileEncryptionInfo();
    this.cachingStrategy = new AtomicReference<CachingStrategy>(
        dfsClient.getDefaultWriteCachingStrategy());       
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug(
          "Set non-null progress callback on DFSOutputStream " + src);
    }
    
     final int bytesPerChecksum = checksum.getBytesPerChecksum();
    if (bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
              ") and blockSize(" + blockSize +
              ") do not match. " + "blockSize should be a " +
              "multiple of io.bytes.per.checksum");

    }
    this.checksum = checksum;
    this.byteArrayManager = dfsClient.getClientContext().getByteArrayManager();
  }

  /** Construct a new output stream for creating a file. */
  protected DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
      EnumSet<CreateFlag> flag, Progressable progress,
      DataChecksum checksum, String[] favoredNodes,
      EncodingPolicy policy, final int dbFileMaxSize, boolean forceClientToWriteSFToDisk) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

    computePacketChunkSize(dfsClient.getConf().getWritePacketSize(), checksum.getBytesPerChecksum());

    streamer = new DataStreamer(stat, null, dfsClient, src, progress, checksum,
        cachingStrategy, byteArrayManager, dbFileMaxSize, forceClientToWriteSFToDisk, favoredNodes);
    
    if (policy != null) {
      Codec codec = Codec.getCodec(policy.getCodec());
      if (codec == null) {
        throw new IOException("Unkown codec: " + policy.getCodec());
      }
      streamer.enableSourceStream(codec.getStripeLength());
    }
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
      FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, Progressable progress, int buffersize,
      DataChecksum checksum, String[] favoredNodes,
      EncodingPolicy policy, final int dbFileMaxSize, boolean forceClientToWriteSFToDisk) throws IOException {
    TraceScope scope =
      dfsClient.newPathTraceScope("newStreamForCreate", src);
    try{
      HdfsFileStatus stat = null;

      // Retry the create if we get a RetryStartFileException up to a maximum
      // number of times
      boolean shouldRetry = true;
      int retryCount = CREATE_RETRY_COUNT;
      while (shouldRetry) {
        shouldRetry = false;
        try {
          stat = dfsClient.namenode.create(src, masked, dfsClient.clientName,
              new EnumSetWritable<CreateFlag>(flag), createParent, replication,
              blockSize, SUPPORTED_CRYPTO_VERSIONS, policy);
          break;
        } catch (RemoteException re) {
          IOException e = re.unwrapRemoteException(
              AccessControlException.class,
              DSQuotaExceededException.class,
              QuotaByStorageTypeExceededException.class,
              FileAlreadyExistsException.class,
              FileNotFoundException.class,
              ParentNotDirectoryException.class,
              NSQuotaExceededException.class,
              RetryStartFileException.class,
              SafeModeException.class,
              UnresolvedPathException.class,
              UnknownCryptoProtocolVersionException.class);
          if (e instanceof RetryStartFileException) {
            if (retryCount > 0) {
              shouldRetry = true;
              retryCount--;
            } else {
              throw new IOException("Too many retries because of encryption" + " zone operations", e);
            }
          } else {
            throw e;
          }
        }
      }
      Preconditions.checkNotNull(stat, "HdfsFileStatus should not be null!");
      final DFSOutputStream out = new DFSOutputStream(dfsClient, src, stat,
          flag, progress, checksum, favoredNodes, policy, dbFileMaxSize, forceClientToWriteSFToDisk);
      out.start();
      return out;
    } finally {
      scope.close();
    }
  }

  /** Construct a new output stream for append. */
  private DFSOutputStream(DFSClient dfsClient, String src,
      EnumSet<CreateFlag> flags, Progressable progress, LocatedBlock lastBlock,
      HdfsFileStatus stat, DataChecksum checksum, final int dbFileMaxSize, boolean forceClientToWriteSFToDisk,
      String[] favoredNodes) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    initialFileSize = stat.getLen(); // length of file when opened
    this.shouldSyncBlock = flags.contains(CreateFlag.SYNC_BLOCK);

    boolean toNewBlock = flags.contains(CreateFlag.NEW_BLOCK);

    // The last partial block of the file has to be filled.
    if (!toNewBlock && lastBlock != null && policySuite.getPolicy(stat.getStoragePolicy()).getStorageTypes()[0] != StorageType.DB) {
      // indicate that we are appending to an existing block
      streamer = new DataStreamer(lastBlock, stat, dfsClient, src, progress, checksum,
          cachingStrategy, byteArrayManager, dbFileMaxSize, forceClientToWriteSFToDisk);
      streamer.setBytesCurBlock(lastBlock.getBlockSize());
      adjustPacketChunkSize(stat);
      streamer.setPipelineInConstruction(lastBlock);
    } else {
      computePacketChunkSize(dfsClient.getConf().getWritePacketSize(),
          checksum.getBytesPerChecksum());
      if (policySuite.getPolicy(stat.getStoragePolicy()).getStorageTypes()[0] == StorageType.DB && lastBlock != null) {
        streamer = new DataStreamer(stat, null, dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager,
            dbFileMaxSize, forceClientToWriteSFToDisk, favoredNodes);
        streamer.setBytesCurBlock(0);
        write(lastBlock.getData(), 0, lastBlock.getData().length);
        LOG.debug("Stuffed Inode:  Putting Existing data in packets");
      } else {
        streamer = new DataStreamer(stat, lastBlock != null ? lastBlock.getBlock() : null,
            dfsClient, src, progress, checksum, cachingStrategy, byteArrayManager, dbFileMaxSize,
            forceClientToWriteSFToDisk, favoredNodes);
      }
    }
    streamer.setFileStoredInDB(policySuite.getPolicy(stat.getStoragePolicy()).getStorageTypes()[0] == StorageType.DB);
    this.fileEncryptionInfo = stat.getFileEncryptionInfo();
  }

  private void adjustPacketChunkSize(HdfsFileStatus stat) throws IOException{

    long usedInLastBlock = stat.getLen() % blockSize;
    int freeInLastBlock = (int)(blockSize - usedInLastBlock);

    // calculate the amount of free space in the pre-existing
    // last crc chunk
    int usedInCksum = (int)(stat.getLen() % checksum.getBytesPerChecksum());
    int freeInCksum = checksum.getBytesPerChecksum() - usedInCksum;

    // if there is space in the last block, then we have to
    // append to that block
    if (freeInLastBlock == blockSize) {
      throw new IOException("The last block for file " +
          src + " is full.");
    }

    if (usedInCksum > 0 && freeInCksum > 0) {
      // if there is space in the last partial chunk, then
      // setup in such a way that the next packet will have only
      // one chunk that fills up the partial chunk.
      //
      computePacketChunkSize(0, freeInCksum);
      setChecksumBufSize(freeInCksum);
      streamer.setAppendChunk(true);
    } else {
      // if the remaining space in the block is smaller than
      // that expected size of of a packet, then create
      // smaller size packet.
      //
      computePacketChunkSize(Math.min(dfsClient.getConf().getWritePacketSize(), freeInLastBlock),
          checksum.getBytesPerChecksum());
    }
  }

  static DFSOutputStream newStreamForAppend(DFSClient dfsClient, String src,
      EnumSet<CreateFlag> flags, int bufferSize, Progressable progress,
      LocatedBlock lastBlock, HdfsFileStatus stat, DataChecksum checksum,
      String[] favoredNodes,
      final int dbFileMaxSize, boolean forceClientToWriteSFToDisk) throws IOException {
    TraceScope scope =
        dfsClient.newPathTraceScope("newStreamForAppend", src);
    try {
      if (policySuite.getPolicy(stat.getStoragePolicy()).getStorageTypes()[0] == StorageType.DB) {
        String errorMessage = null;
        if (stat.getLen() > stat.getBlockSize()) {
          errorMessage = "Invalid parameters for appending a file stored in the database. Block size can not be smaller "
              + "than the max size of a file stored in the database";
        } else if (dbFileMaxSize > stat.getBlockSize()) {
          errorMessage = "Invalid parameters for appending a file stored in the database. Files stored in the database "
              + "can not be larger than a HDFS block";
        }

        if (errorMessage != null) {
          throw new IOException(errorMessage);
        }
      }
      final DFSOutputStream out = new DFSOutputStream(dfsClient, src, flags,
          progress, lastBlock, stat, checksum, dbFileMaxSize, forceClientToWriteSFToDisk, favoredNodes);
      out.start();
      return out;
    } finally {
      scope.close();
    }
  }

  /**
   * Construct a new output stream for a single block.
   */
  private DFSOutputStream(DFSClient dfsClient, String src, 
                          Progressable progress, HdfsFileStatus stat, LocatedBlock lb, DataChecksum checksum)
          throws IOException {
    this(dfsClient, src, progress, stat, checksum);
    singleBlock = true;

    computePacketChunkSize(dfsClient.getConf().getWritePacketSize(),
            checksum.getBytesPerChecksum());
    streamer = new DataStreamer(stat, lb, singleBlock, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManager, -1, false);
  }

  static DFSOutputStream newStreamForSingleBlock(DFSClient dfsClient,
                                                 String src, Progressable progress, LocatedBlock block,
                                                 DataChecksum checksum, HdfsFileStatus stat) throws IOException {
    final DFSOutputStream out =
            new DFSOutputStream(dfsClient, src, progress, stat, block, 
                    checksum);
    out.start();
    return out;
  }
  
  protected void computePacketChunkSize(int psize, int csize) {
    final int bodySize = psize - PacketHeader.PKT_MAX_HEADER_LEN;
    final int chunkSize = csize + checksum.getChecksumSize();
    chunksPerPacket = Math.max(bodySize/chunkSize, 1);
    packetSize = chunkSize*chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
                ", chunkSize=" + chunkSize +
                ", chunksPerPacket=" + chunksPerPacket +
                ", packetSize=" + packetSize);
    }
  }

  protected TraceScope createWriteTraceScope() {
    return dfsClient.newPathTraceScope("DFSOutputStream#write", src);
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len,
      byte[] checksum, int ckoff, int cklen) throws IOException {
    dfsClient.checkOpen();
    checkClosed();

    int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
                            " is larger than supported  bytesPerChecksum " +
                            bytesPerChecksum);
    }
    if (cklen != 0 && cklen != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
                            this.checksum.getChecksumSize() + " but found to be " + cklen);
    }

    if (currentPacket == null) {
      currentPacket = createPacket(packetSize, chunksPerPacket, 
          streamer.getBytesCurBlock(), streamer.getAndIncCurrentSeqno(), false);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
            currentPacket.getSeqno() +
            ", src=" + src +
            ", packetSize=" + packetSize +
            ", chunksPerPacket=" + chunksPerPacket +
            ", bytesCurBlock=" + streamer.getBytesCurBlock());
      }
    }

    currentPacket.writeChecksum(checksum, ckoff, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.incNumChunks();
    streamer.incBytesCurBlock(len);

    // If packet is full, enqueue it for transmission
    //
    if (currentPacket.getNumChunks() == currentPacket.getMaxChunks() ||
        streamer.getBytesCurBlock() == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
            currentPacket.getSeqno() +
            ", src=" + src +
            ", bytesCurBlock=" + streamer.getBytesCurBlock() +
            ", blockSize=" + blockSize +
            ", appendChunk=" + streamer.getAppendChunk());
      }
      streamer.waitAndQueuePacket(currentPacket);
      currentPacket = null;

      adjustChunkBoundary();

      endBlock();
    }
  }

  /**
   * If the reopened file did not end at chunk boundary and the above
   * write filled up its partial chunk. Tell the summer to generate full
   * crc chunks from now on.
   */
  protected void adjustChunkBoundary() {
    if (streamer.getAppendChunk() &&
        streamer.getBytesCurBlock() % this.checksum.getBytesPerChecksum() == 0) {
      streamer.setAppendChunk(false);
      resetChecksumBufSize();
    }

    if (!streamer.getAppendChunk()) {
      int psize = Math.min((int)(blockSize- streamer.getBytesCurBlock()),
          dfsClient.getConf().getWritePacketSize());
      computePacketChunkSize(psize, this.checksum.getBytesPerChecksum());
    }
  }

  /**
   * if encountering a block boundary, send an empty packet to
   * indicate the end of block and reset bytesCurBlock.
   *
   * @throws IOException
   */
  protected void endBlock() throws IOException {
    if (streamer.getBytesCurBlock() == blockSize) {
      currentPacket = createPacket(0, 0, streamer.getBytesCurBlock(),
          streamer.getAndIncCurrentSeqno(), true);
      currentPacket.setSyncBlock(shouldSyncBlock);
      streamer.waitAndQueuePacket(currentPacket);
      currentPacket = null;
      streamer.setBytesCurBlock(0);
      lastFlushOffset = 0;
    }
  }

  @Deprecated
  public void sync() throws IOException {
    hflush();
  }
  
  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.HSYNC:
    case StreamCapabilities.HFLUSH:
      return true;
    default:
      return false;
    }
  }
  
  /**
   * Flushes out to all replicas of the block. The data is in the buffers
   * of the DNs but not necessarily in the DN's OS buffers.
   *
   * It is a synchronous operation. When it returns,
   * it guarantees that flushed data become visible to new readers. 
   * It is not guaranteed that data has been flushed to 
   * persistent store on the datanode. 
   * Block allocations are persisted on namenode.
   */
  @Override
  public void hflush() throws IOException {
    TraceScope scope =
        dfsClient.newPathTraceScope("hflush", src);
    try {
      flushOrSync(false, EnumSet.noneOf(SyncFlag.class));
    } finally {
      scope.close();
    }
  }

  @Override
  public void hsync() throws IOException {
    TraceScope scope =
        dfsClient.newPathTraceScope("hsync", src);
    try {
      flushOrSync(true, EnumSet.noneOf(SyncFlag.class));
    } finally {
      scope.close();
    }
  }
  
  /**
   * The expected semantics is all data have flushed out to all replicas 
   * and all replicas have done posix fsync equivalent - ie the OS has 
   * flushed it to the disk device (but the disk may have it in its cache).
   * 
   * Note that only the current block is flushed to the disk device.
   * To guarantee durable sync across block boundaries the stream should
   * be created with {@link CreateFlag#SYNC_BLOCK}.
   * 
   * @param syncFlags
   *          Indicate the semantic of the sync. Currently used to specify
   *          whether or not to update the block length in NameNode.
   */
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    TraceScope scope =
        dfsClient.newPathTraceScope("hsync", src);
    try {
      flushOrSync(true, syncFlags);
    } finally {
      scope.close();
    }
  }

  /**
   * Flush/Sync buffered data to DataNodes.
   * 
   * @param isSync
   *          Whether or not to require all replicas to flush data to the disk
   *          device
   * @param syncFlags
   *          Indicate extra detailed semantic of the flush/sync. Currently
   *          mainly used to specify whether or not to update the file length in
   *          the NameNode
   * @throws IOException
   */
  private void flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags)
      throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    streamer.syncOrFlushCalled();
    try {
      long toWaitFor;
      long lastBlockLength = -1L;
      boolean updateLength = syncFlags.contains(SyncFlag.UPDATE_LENGTH);
      boolean endBlock = syncFlags.contains(SyncFlag.END_BLOCK);
      synchronized (this) {
        // flush checksum buffer, but keep checksum buffer intact if we do not
        // need to end the current block
        int numKept = flushBuffer(!endBlock, true);
        // bytesCurBlock potentially incremented if there was buffered data

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient flush():"
              + " bytesCurBlock=" + streamer.getBytesCurBlock()
              + " lastFlushOffset=" + lastFlushOffset
              + " createNewBlock=" + endBlock);
        }
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != streamer.getBytesCurBlock()) {
          assert streamer.getBytesCurBlock() > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = streamer.getBytesCurBlock();
          if (isSync && currentPacket == null && !endBlock) {
            // Nothing to send right now,
            // but sync was requested.
            // Send an empty packet if we do not end the block right now
            currentPacket = createPacket(packetSize, chunksPerPacket,
                streamer.getBytesCurBlock(), streamer.getAndIncCurrentSeqno(), false);
          }
        } else {
          if (isSync && streamer.getBytesCurBlock() > 0 && !endBlock) {
            // Nothing to send right now,
            // and the block was partially written,
            // and sync was requested.
            // So send an empty sync packet if we do not end the block right now
            currentPacket = createPacket(packetSize, chunksPerPacket,
                streamer.getBytesCurBlock(), streamer.getAndIncCurrentSeqno(), false);
          } else if (currentPacket != null) {
            // just discard the current packet since it is already been sent.
            currentPacket.releaseBuffer(byteArrayManager);
            currentPacket = null;
          }
        }
        if (currentPacket != null) {
          currentPacket.setSyncBlock(isSync);
          streamer.waitAndQueuePacket(currentPacket);
          currentPacket = null;
        }
        if (endBlock && streamer.getBytesCurBlock() > 0) {
          // Need to end the current block, thus send an empty packet to
          // indicate this is the end of the block and reset bytesCurBlock
          currentPacket = createPacket(0, 0, streamer.getBytesCurBlock(),
              streamer.getAndIncCurrentSeqno(), true);
          currentPacket.setSyncBlock(shouldSyncBlock || isSync);
          streamer.waitAndQueuePacket(currentPacket);
          currentPacket = null;
          streamer.setBytesCurBlock(0);
          lastFlushOffset = 0;
        } else {
          // Restore state of stream. Record the last flush offset
          // of the last full chunk that was flushed.
          streamer.setBytesCurBlock(streamer.getBytesCurBlock() - numKept);
        }

        toWaitFor = streamer.getLastQueuedSeqno();
      } // end synchronized

      streamer.waitForAckedSeqno(toWaitFor);

      // update the block length first time irrespective of flag
      if (updateLength || streamer.getPersistBlocks().get()) {
        synchronized (this) {
          if (!streamer.streamerClosed() && streamer.getBlock() != null) {
            lastBlockLength = streamer.getBlock().getNumBytes();
          }
        }
      }
      // If 1) any new blocks were allocated since the last flush, or 2) to
      // update length in NN is required, then persist block locations on
      // namenode.
      if (streamer.getPersistBlocks().getAndSet(false) || updateLength) {
        try {
          dfsClient.namenode.fsync(src, fileId, dfsClient.clientName,
              lastBlockLength);
        } catch (IOException ioe) {
          DFSClient.LOG.warn("Unable to persist blocks in hflush for " + src, ioe);
          // If we got an error here, it might be because some other thread called
          // close before our hflush completed. In that case, we should throw an
          // exception that the stream is closed.
          checkClosed();
          // If we aren't closed but failed to sync, we should expose that to the
          // caller.
          throw ioe;
        }
      }

      synchronized(this) {
        if (!streamer.streamerClosed()) {
          streamer.setHflush();
        }
      }
    } catch (InterruptedIOException interrupt) {
      // This kind of error doesn't mean that the stream itself is broken - just the
      // flushing thread got interrupted. So, we shouldn't close down the writer,
      // but instead just propagate the error
      throw interrupt;
    } catch (IOException e) {
      DFSClient.LOG.warn("Error while syncing", e);
      synchronized (this) {
        if (!isClosed()) {
          streamer.getLastException().set(e);
          closeThreads(true);
        }
      }
      throw e;
    }
  }

  /**
   * @deprecated use {@link HdfsDataOutputStream#getCurrentBlockReplication()}.
   */
  @Deprecated
  public synchronized int getNumCurrentReplicas() throws IOException {
    return getCurrentBlockReplication();
  }

  /**
   * Note that this is not a public API;
   * use {@link HdfsDataOutputStream#getCurrentBlockReplication()} instead.
   * 
   * @return the number of valid replicas of the current block
   */
  public synchronized int getCurrentBlockReplication() throws IOException {
    dfsClient.checkOpen();
    checkClosed();
    if (streamer.streamerClosed()) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    DatanodeInfo[] currentNodes = streamer.getNodes();
    if (currentNodes == null) {
      return blockReplication; // no pipeline, return repl factor of file
    }
    return currentNodes.length;
  }
  
  /**
   * Waits till all existing data is flushed and confirmations 
   * received from datanodes. 
   */
  protected void flushInternal() throws IOException {
    long toWaitFor;
    synchronized (this) {
      dfsClient.checkOpen();
      checkClosed();
      //
      // If there is data in the current buffer, send it across
      //
      streamer.queuePacket(currentPacket);
      currentPacket = null;
      toWaitFor = streamer.getLastQueuedSeqno();
    }

    streamer.waitForAckedSeqno(toWaitFor);
  }

  protected synchronized void start() {
    streamer.start();
  }
  
  /**
   * Aborts this output stream and releases any system 
   * resources associated with this stream.
   */
  synchronized void abort() throws IOException {
    if (isClosed()) {
      return;
    }
    streamer.getLastException().set(new IOException("Lease timeout of "
        + (dfsClient.getConf().getHdfsTimeout()/1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(fileId);
  }

  boolean isClosed() {
    return closed || streamer.streamerClosed();
  }

  void setClosed() {
    closed = true;
    streamer.release();
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  protected void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      streamer.closeSocket();
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer.setSocketToNull();
      setClosed();
    }
  }
  
  /**
   * Closes this output stream and releases any system 
   * resources associated with this stream.
   */
  @Override
  public synchronized void close() throws IOException {
    TraceScope scope =
        dfsClient.newPathTraceScope("DFSOutputStream#close", src);
    try {
      closeImpl();
    } finally {
      scope.close();
    }
  }

  protected synchronized void closeImpl() throws IOException {
    if (isClosed()) {
      streamer.getLastException().check(true);
      return;
    }

    try {
      closeInternal();
    } catch (ClosedChannelException e) {
    } catch (OutOfDBExtentsException e) {
      currentPacket = null;
      streamer.forwardSmallFilesPacketsToDataNodes(); // try to store the file on
      streamer.setBytesCurBlock(0);
      closeInternal();
    } finally {
      setClosed();
    }
  }
  
  private void closeInternal() throws IOException {
    flushBuffer();       // flush from all upper layers

    if (currentPacket != null) {
      streamer.waitAndQueuePacket(currentPacket);
      currentPacket = null;
    }

    if (streamer.getBytesCurBlock() != 0) {
      // send an empty packet to mark the end of the block
      currentPacket = createPacket(0, 0, streamer.getBytesCurBlock(),
          streamer.getAndIncCurrentSeqno(), true);
      currentPacket.setSyncBlock(shouldSyncBlock);
    }

    flushInternal();             // flush all data to Datanodes
    // get last block before destroying the streamer
    ExtendedBlock lastBlock = streamer.getBlock();
    TraceScope scope = dfsClient.getTracer().newScope("completeFile");
    try {
      completeFile(lastBlock);
    } finally {
      scope.close();
    }
    closeThreads(false);
    dfsClient.endFileLease(fileId);
  }

  // should be called holding (this) lock since setTestFilename() may 
  // be called during unit tests
  protected void completeFile(ExtendedBlock last) throws IOException {
    if (singleBlock) {
      return;
    }
    long localstart = Time.monotonicNow();
    final DfsClientConf conf = dfsClient.getConf();
    long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
    boolean fileComplete = false;
    int retries = conf.getNumBlockWriteLocateFollowingRetry();
    
    while (!fileComplete) {
      fileComplete = completeFileInternal(last);
      if (!fileComplete) {
        final int hdfsTimeout = conf.getHdfsTimeout();
        if (!dfsClient.clientRunning
            || (hdfsTimeout > 0
                && localstart + hdfsTimeout < Time.monotonicNow())) {
            String msg = "Unable to close file because dfsclient " +
                          " was unable to contact the HDFS servers." +
                          " clientRunning " + dfsClient.clientRunning +
                          " hdfsTimeout " + hdfsTimeout;
            DFSClient.LOG.info(msg);
            throw new IOException(msg);
        }
        try {
          if (retries == 0) {
            throw new IOException("Unable to close file because the last block"
                + " does not have enough number of replicas.");
          }
          retries--;
          Thread.sleep(sleeptime);
          sleeptime *= 2;
          if (Time.monotonicNow() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.warn("Caught exception ", ie);
        }
      }
    }
  }
  
  private boolean completeFileInternal(ExtendedBlock last) throws IOException {
    boolean fileComplete = false;
    byte data[] = null;
    if (streamer.canStoreFileInDB()) {
      data = getSmallFileData();
    }

    try {
      fileComplete =
              dfsClient.namenode.complete(src, dfsClient.clientName, last, fileId, data);
    } catch (RemoteException e) {
      IOException nonRetirableExceptions =
              e.unwrapRemoteException(NSQuotaExceededException.class,
                      DSQuotaExceededException.class,
                      OutOfDBExtentsException.class );
      if (nonRetirableExceptions != e) {
        throw nonRetirableExceptions; // no need to retry these exceptions
      } else {
        throw e;
      }
    }
    return fileComplete;
  }

  private byte[] getSmallFileData(){
    byte data[] = null;
    if (streamer.canStoreFileInDB()) {
      if (!streamer.getSmallFileDataQueue().isEmpty()) {
        LOG.debug("Stuffed Inode:  Sending data to the NameNode in comple file operation ");
        int length = 0;
        for (DFSPacket packet : streamer.getSmallFileDataQueue()) {
          if (!packet.isHeartbeatPacket()) {
            length += packet.getDataLength();
          }
        }
        LOG.debug("Stuffed Inode:  total data is " + length);
        data = new byte[length];
        int index = 0;
        for (DFSPacket packet : streamer.getSmallFileDataQueue()) {
          index += packet.writeToArray(data, index);
          
        }
      }
    }
    return data;
  }

  @VisibleForTesting
  public void setArtificialSlowdown(long period) {
    streamer.setArtificialSlowdown(period);
  }

  @VisibleForTesting
  public synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = (checksum.getBytesPerChecksum() + checksum.getChecksumSize()) * chunksPerPacket;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  public long getInitialLen() {
    return initialFileSize;
  }

  /**
   * @return the FileEncryptionInfo for this stream, or null if not encrypted.
   */
  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  synchronized Token<BlockTokenIdentifier> getBlockToken() {
    return streamer.getBlockToken();
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }
  
  @Override
  public void setDropBehind(Boolean dropBehind) throws IOException {
    CachingStrategy prevStrategy, nextStrategy;
    // CachingStrategy is immutable.  So build a new CachingStrategy with the
    // modifications we want, and compare-and-swap it in.
    do {
      prevStrategy = this.cachingStrategy.get();
      nextStrategy = new CachingStrategy.Builder(prevStrategy).
                        setDropBehind(dropBehind).build();
    } while (!this.cachingStrategy.compareAndSet(prevStrategy, nextStrategy));
  }

  @VisibleForTesting
  ExtendedBlock getBlock() {
    return streamer.getBlock();
  }

  @VisibleForTesting
  public long getFileId() {
    return fileId;
  }
  
  public void enableParityStream(int stripeLength, int parityLength,
                                 String sourceFile) throws IOException {
    streamer.enableParityStream(stripeLength, parityLength, sourceFile);
  }
  
  public Collection<DatanodeInfo> getUsedNodes() {
    return streamer.getUsedNodes();
  }
  
  public void setParityStripeNodesForNextStripe(Collection<DatanodeInfo> locations) {
    streamer.setParityStripeNodesForNextStripe(locations);
  }
  
  public void enableSourceStream(int stripeLength) {
    streamer.enableSourceStream(stripeLength);
  }
}
