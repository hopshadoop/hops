/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import com.google.common.annotations.VisibleForTesting;
import io.hops.erasure_coding.Codec;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StandardSocketFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;


/**
 * *************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 * <p/>
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 * <p/>
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 * <p/>
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
 * **************************************************************
 */
@InterfaceAudience.Private
public class DFSOutputStream extends FSOutputSummer implements Syncable {
  public static final Log LOG = LogFactory.getLog(DFSOutputStream.class);

  private final DFSClient dfsClient;
  private static final int MAX_PACKETS = 80; // each packet 64K, total 5MB
  private Socket s;
  // closed is accessed by different threads under different locks.
  private volatile boolean closed = false;

  private String src;
  private final long blockSize;
  private final DataChecksum checksum;
  // both dataQueue and ackQueue are protected by dataQueue lock
  private final LinkedList<Packet> dataQueue = new LinkedList<>();
  private final LinkedList<Packet> ackQueue = new LinkedList<>();
  private Packet currentPacket = null;
  private DataStreamer streamer;
  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes writen in current block
  private int packetSize = 0; // write packet size, not including the header.
  private int chunksPerPacket = 0;
  private volatile IOException lastException = null;
  private long artificialSlowdown = 0;
  private long lastFlushOffset = 0; // offset when flush was invoked
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private volatile boolean appendChunk = false;
  // appending to existing partial block
  private long initialFileSize = 0; // at time of file open
  private Progressable progress;
  private final short blockReplication; // replication factor of file
  private boolean shouldSyncBlock = false; // force blocks to disk upon close
  private boolean singleBlock = false;

  private boolean erasureCodingSourceStream = false;
  private boolean erasureCodingParityStream = false;
  private HashSet<DatanodeInfo> usedNodes = new HashSet<>();
  private int currentBlockIndex = 0;
  private int stripeLength;
  private int parityLength;
  private List<LocatedBlock> sourceBlocks = Collections.emptyList();
  private List<DatanodeInfo> stripeNodes = new LinkedList<>();
  private List<DatanodeInfo> parityStripeNodes = new LinkedList<>();

  //this queue holds data for small files that are stored in the database.
  //when the file is closed the data is send to a NameNode in the close RPC
  private final LinkedList<Packet> smallFileDataQueue = new LinkedList<>();
  private boolean saveSmallFilesInDB;
  private boolean isThisFileStoredInDB = false;
  private final int dbFileMaxSize;
  //if the client calls sync/flush method then the file will be stored on the
  //datanodes irrespective of the file size. The reason is that before the file
  //is close we are not sure about the final size of the file. If we store the
  // the data in the database and later on the file size exceeds the "dbFileMaxSize"
  // limit then we will have to transfer the data stored in the databse to the
  // datanodes. This will slow down the file creations and put unnecessary stress
  // on the NameNodes.
  private boolean syncOrFlushCalled = false;
  private int currentSizeOfSmallFile = 0;

  private class Packet {
    long seqno;               // sequencenumber of buffer in block
    long offsetInBlock;       // offset in block
    private boolean lastPacketInBlock;   // is this the last packet in block?
    boolean syncBlock;          // this packet forces the current block to disk
    int numChunks;           // number of chunks currently in packet
    int maxChunks;           // max chunks in packet

    byte[] buf;

    /**
     * buf is pointed into like follows:
     * (C is checksum data, D is payload data)
     * <p/>
     * [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
     * ^        ^               ^               ^
     * |        checksumPos     dataStart       dataPos
     * checksumStart
     * <p/>
     * Right before sending, we move the checksum data to immediately precede
     * the actual data, and then insert the header into the buffer immediately
     * preceding the checksum data, so we make sure to keep enough space in
     * front of the checksum data to support the largest conceivable header.
     */
    int checksumStart;
    int checksumPos;
    int dataStart;
    int dataPos;

    private static final long HEART_BEAT_SEQNO = -1L;

    /**
     * Create a heartbeat packet.
     */
    Packet() {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = 0;
      this.seqno = HEART_BEAT_SEQNO;

      buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];

      checksumStart =
              checksumPos = dataPos = dataStart = PacketHeader.PKT_MAX_HEADER_LEN;
      maxChunks = 0;
    }

    /**
     * Create a new packet.
     *
     * @param pktSize
     *     maximum size of the packet, including checksum data and actual data.
     * @param chunksPerPkt
     *     maximum number of chunks per packet.
     * @param offsetInBlock
     *     offset in bytes into the HDFS block.
     */
    Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
      this.lastPacketInBlock = false;
      this.numChunks = 0;
      this.offsetInBlock = offsetInBlock;
      this.seqno = currentSeqno;
      currentSeqno++;

      buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN + pktSize];

      checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
      checksumPos = checksumStart;
      dataStart = checksumStart + (chunksPerPkt * checksum.getChecksumSize());
      dataPos = dataStart;
      maxChunks = chunksPerPkt;
    }

    void writeData(byte[] inarray, int off, int len) {
      if (dataPos + len > buf.length) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, dataPos, len);
      dataPos += len;
    }

    void writeChecksum(byte[] inarray, int off, int len) {
      if (checksumPos + len > dataStart) {
        throw new BufferOverflowException();
      }
      System.arraycopy(inarray, off, buf, checksumPos, len);
      checksumPos += len;
    }

    /**
     * Write the full packet, including the header, to the given output stream.
     */
    void writeTo(DataOutputStream stm) throws IOException {
      final int dataLen = dataPos - dataStart;
      final int checksumLen = checksumPos - checksumStart;
      final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;

      PacketHeader header =
              new PacketHeader(pktLen, offsetInBlock, seqno, lastPacketInBlock,
                      dataLen, syncBlock);

      if (checksumPos != dataStart) {
        // Move the checksum to cover the gap. This can happen for the last
        // packet or during an hflush/hsync call.
        System.arraycopy(buf, checksumStart, buf, dataStart - checksumLen,
                checksumLen);
        checksumPos = dataStart;
        checksumStart = checksumPos - checksumLen;
      }

      final int headerStart = checksumStart - header.getSerializedSize();
      assert checksumStart + 1 >= header.getSerializedSize();
      assert checksumPos == dataStart;
      assert headerStart >= 0;
      assert headerStart + header.getSerializedSize() == checksumStart;

      // Copy the header data into the buffer immediately preceding the checksum
      // data.
      System.arraycopy(header.getBytes(), 0, buf, headerStart,
              header.getSerializedSize());

      // Write the now contiguous full packet to the output stream.
      stm.write(buf, headerStart,
              header.getSerializedSize() + checksumLen + dataLen);
    }

    // get the packet's last byte's offset in the block
    long getLastByteOffsetBlock() {
      return offsetInBlock + dataPos - dataStart;
    }

    /**
     * Check if this packet is a heart beat packet
     *
     * @return true if the sequence number is HEART_BEAT_SEQNO
     */
    private boolean isHeartbeatPacket() {
      return seqno == HEART_BEAT_SEQNO;
    }

    @Override
    public String toString() {
      return "packet seqno:" + this.seqno +
              " offsetInBlock:" + this.offsetInBlock +
              " lastPacketInBlock:" + this.lastPacketInBlock +
              " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
    }
  }

  //
  // The DataStreamer class is responsible for sending data packets to the
  // datanodes in the pipeline. It retrieves a new blockid and block locations
  // from the namenode, and starts streaming packets to the pipeline of
  // Datanodes. Every packet has a sequence number associated with
  // it. When all the packets for a block are sent out and acks for each
  // if them are received, the DataStreamer closes the current block.
  //
  class DataStreamer extends Daemon {
    private volatile boolean streamerClosed = false;
    private ExtendedBlock block; // its length is number of bytes acked
    private Token<BlockTokenIdentifier> accessToken;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private ResponseProcessor response = null;
    private volatile DatanodeInfo[] nodes = null;
    // list of targets for current block
    private ArrayList<DatanodeInfo> excludedNodes =
        new ArrayList<>();
    volatile boolean hasError = false;
    volatile int errorIndex = -1;
    private BlockConstructionStage stage;  // block construction stage
    private long bytesSent = 0; // number of bytes that've been sent

    /**
     * Nodes have been used in the pipeline before and have failed.
     */
    private final List<DatanodeInfo> failed = new ArrayList<>();
    /**
     * Has the current block been hflushed?
     */
    private boolean isHflushed = false;
    /**
     * Append on an existing block?
     */
    private final boolean isAppend;

    // Information for sending a single block
    private LocatedBlock lb;

    /**
     * Default construction for file create
     */
    private DataStreamer() {
      isAppend = false;
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }

    /**
     * Construct a data streamer for single block transfer
     */
    private DataStreamer(LocatedBlock lb) {
      isAppend = false;
      stage = BlockConstructionStage.PIPELINE_SETUP_SINGLE_BLOCK;
      this.lb = lb;
    }

    /**
     * Construct a data streamer for append
     *
     * @param lastBlock
     *     last block of the file to be appended
     * @param stat
     *     status of the file to be appended
     * @param bytesPerChecksum
     *     number of bytes per checksum
     * @throws IOException
     *     if error occurs
     */
    private DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat,
                         int bytesPerChecksum) throws IOException {
      isAppend = true;
      stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
      block = lastBlock.getBlock();
      bytesSent = block.getNumBytes();
      accessToken = lastBlock.getBlockToken();
      long usedInLastBlock = stat.getLen() % blockSize;
      int freeInLastBlock = (int) (blockSize - usedInLastBlock);

      // calculate the amount of free space in the pre-existing
      // last crc chunk
      int usedInCksum = (int) (stat.getLen() % bytesPerChecksum);
      int freeInCksum = bytesPerChecksum - usedInCksum;

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
        resetChecksumChunk(freeInCksum);
        appendChunk = true;
      } else {
        // if the remaining space in the block is smaller than
        // that expected size of of a packet, then create
        // smaller size packet.
        //
        computePacketChunkSize(
                Math.min(dfsClient.getConf().writePacketSize, freeInLastBlock),
                bytesPerChecksum);
      }

      // setup pipeline to append to the last block XXX retries??
      nodes = lastBlock.getLocations();
      errorIndex = -1;   // no errors yet.
      if (nodes.length < 1) {
        throw new IOException("Unable to retrieve blocks locations " +
                " for last block " + block +
                "of file " + src);

      }
    }

    /**
     * Initialize for data streaming
     */
    private void initDataStreaming() {
      this.setName("DataStreamer for file " + src +
              " block " + block);
      response = new ResponseProcessor(nodes);
      response.start();
      stage = BlockConstructionStage.DATA_STREAMING;
    }

    private void endBlock() {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Closing old block " + block);
      }
      this.setName("DataStreamer for file " + src);
      closeResponder();
      closeStream();
      nodes = null;
      stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
    }

    /*
     * streamer thread is the only thread that opens streams to datanode, 
     * and closes them. Any error recovery is also done by this thread.
     */
    @Override
    public void run() {
      long lastPacket = Time.now();
      while (!streamerClosed && dfsClient.clientRunning) {

        // if the Responder encountered an error, shutdown Responder
        if (hasError && response != null) {
          try {
            response.close();
            response.join();
            response = null;
          } catch (InterruptedException e) {
          }
        }

        Packet one = null;

        try {
          // process datanode IO errors if any
          boolean doSleep = false;
          if (hasError && errorIndex >= 0) {
            doSleep = processDatanodeError();
          }

          synchronized (dataQueue) {
            // wait for a packet to be sent.
            long now = Time.now();
            while ((!streamerClosed && !hasError && dfsClient.clientRunning &&
                    dataQueue.size() == 0 &&
                    (stage != BlockConstructionStage.DATA_STREAMING ||
                            stage == BlockConstructionStage.DATA_STREAMING &&
                                    now - lastPacket <
                                            dfsClient.getConf().socketTimeout / 2)) ||
                    doSleep) {
              long timeout =
                      dfsClient.getConf().socketTimeout / 2 - (now - lastPacket);
              timeout = timeout <= 0 ? 1000 : timeout;
              timeout =
                      (stage == BlockConstructionStage.DATA_STREAMING) ? timeout :
                              1000;
              try {
                dataQueue.wait(timeout);
              } catch (InterruptedException e) {
              }
              doSleep = false;
              now = Time.now();
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            // get packet to be sent.
            if (dataQueue.isEmpty()) {
              one = new Packet();  // heartbeat packet
            } else {
              one = dataQueue.getFirst(); // regular data packet
            }
          }
          assert one != null;

          // get new block from namenode.
          if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Allocating new block");
            }
            nodes = nextBlockOutputStream(src);
            initDataStreaming();
          } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Append to block " + block);
            }
            setupPipelineForAppendOrRecovery();
            initDataStreaming();
          } else if (stage ==
                  BlockConstructionStage.PIPELINE_SETUP_SINGLE_BLOCK) {
            // TODO This is sent by protobuf and somehow a hack
            stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("Send single block " + block);
            }
            nodes = setupPipelineForSingleBlock(lb);
            initDataStreaming();
          }

          long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
          if (lastByteOffsetInBlock > blockSize) {
            throw new IOException("BlockSize " + blockSize +
                    " is smaller than data size. " +
                    " Offset of packet in block " +
                    lastByteOffsetInBlock +
                    " Aborting file " + src);
          }

          if (one.lastPacketInBlock) {
            // wait for all data packets have been successfully acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError &&
                      ackQueue.size() != 0 && dfsClient.clientRunning) {
                try {
                  // wait for acks to arrive from datanodes
                  dataQueue.wait(1000);
                } catch (InterruptedException e) {
                }
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }
            stage = BlockConstructionStage.PIPELINE_CLOSE;
          }

          // send the packet
          synchronized (dataQueue) {
            // move packet from dataQueue to ackQueue
            if (!one.isHeartbeatPacket()) {
              dataQueue.removeFirst();
              ackQueue.addLast(one);
              dataQueue.notifyAll();
            }
          }

          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("DataStreamer block " + block +
                    " sending packet " + one);
          }

          // write out data to remote datanode
          try {
            one.writeTo(blockStream);
            blockStream.flush();
          } catch (IOException e) {
            // HDFS-3398 treat primary DN is down since client is unable to 
            // write to primary DN 
            errorIndex = 0;
            throw e;
          }
          lastPacket = Time.now();

          if (one.isHeartbeatPacket()) {  //heartbeat packet
          }

          // update bytesSent
          long tmpBytesSent = one.getLastByteOffsetBlock();
          if (bytesSent < tmpBytesSent) {
            bytesSent = tmpBytesSent;
          }

          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            // wait for the close packet has been acked
            synchronized (dataQueue) {
              while (!streamerClosed && !hasError &&
                      ackQueue.size() != 0 && dfsClient.clientRunning) {
                dataQueue.wait(1000);// wait for acks to arrive from datanodes
              }
            }
            if (streamerClosed || hasError || !dfsClient.clientRunning) {
              continue;
            }

            endBlock();
          }
          if (progress != null) {
            progress.progress();
          }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && dfsClient.clientRunning) {
            Thread.sleep(artificialSlowdown);
          }
        } catch (Throwable e) {
          DFSClient.LOG.warn("DataStreamer Exception", e);
          if (e instanceof IOException) {
            setLastException((IOException) e);
          }
          hasError = true;
          if (errorIndex == -1) { // not a datanode error
            streamerClosed = true;
          }
        }
      }
      closeInternal();
    }

    private void closeInternal() {
      closeResponder();       // close and join
      closeStream();
      streamerClosed = true;
      closed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
    }

    /*
     * close both streamer and DFSOutputStream, should be called only 
     * by an external thread and only after all data to be sent has 
     * been flushed to datanode.
     * 
     * Interrupt this data streamer if force is true
     * 
     * @param force if this data stream is forced to be closed 
     */
    void close(boolean force) {
      streamerClosed = true;
      synchronized (dataQueue) {
        dataQueue.notifyAll();
      }
      if (force) {
        this.interrupt();
      }
    }

    private void closeResponder() {
      if (response != null) {
        try {
          response.close();
          response.join();
        } catch (InterruptedException e) {
        } finally {
          response = null;
        }
      }
    }

    private void closeStream() {
      if (blockStream != null) {
        try {
          blockStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockStream = null;
        }
      }
      if (blockReplyStream != null) {
        try {
          blockReplyStream.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          blockReplyStream = null;
        }
      }
      if (null != s) {
        try {
          s.close();
        } catch (IOException e) {
          setLastException(e);
        } finally {
          s = null;
        }
      }
    }

    //
    // Processes reponses from the datanodes.  A packet is removed 
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Daemon {

      private volatile boolean responderClosed = false;
      private DatanodeInfo[] targets = null;
      private boolean isLastPacketInBlock = false;

      ResponseProcessor(DatanodeInfo[] targets) {
        this.targets = targets;
      }

      @Override
      public void run() {

        setName("ResponseProcessor for block " + block);
        PipelineAck ack = new PipelineAck();

        while (!responderClosed && dfsClient.clientRunning &&
                !isLastPacketInBlock) {
          // process responses from datanodes.
          try {
            // read an ack from the pipeline
            ack.readFields(blockReplyStream);
            if (DFSClient.LOG.isDebugEnabled()) {
              DFSClient.LOG.debug("DFSClient " + ack);
            }

            long seqno = ack.getSeqno();
            // processes response status from datanodes.
            for (int i = ack.getNumOfReplies() - 1;
                 i >= 0 && dfsClient.clientRunning; i--) {
              final Status reply = ack.getReply(i);
              if (reply != SUCCESS) {
                errorIndex = i; // first bad datanode
                throw new IOException("Bad response " + reply +
                        " for block " + block +
                        " from datanode " +
                        targets[i]);
              }
            }

            assert seqno != PipelineAck.UNKOWN_SEQNO :
                    "Ack for unkown seqno should be a failed ack: " + ack;
            if (seqno == Packet.HEART_BEAT_SEQNO) {  // a heartbeat ack
              continue;
            }

            // a success ack for a data packet
            Packet one = null;
            synchronized (dataQueue) {
              one = ackQueue.getFirst();
            }
            if (one.seqno != seqno) {
              throw new IOException("Responseprocessor: Expecting seqno " +
                      " for block " + block +
                      one.seqno + " but received " + seqno);
            }
            isLastPacketInBlock = one.lastPacketInBlock;
            // update bytesAcked
            block.setNumBytes(one.getLastByteOffsetBlock());

            synchronized (dataQueue) {
              lastAckedSeqno = seqno;
              ackQueue.removeFirst();
              dataQueue.notifyAll();
            }
          } catch (Exception e) {
            if (!responderClosed) {
              if (e instanceof IOException) {
                setLastException((IOException) e);
              }
              hasError = true;
              errorIndex = errorIndex == -1 ? 0 : errorIndex;
              synchronized (dataQueue) {
                dataQueue.notifyAll();
              }
              DFSClient.LOG.warn(
                      "DFSOutputStream ResponseProcessor exception " +
                              " for block " + block, e);
              responderClosed = true;
            }
          }
        }
      }

      void close() {
        responderClosed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError() throws IOException {
      if (response != null) {
        DFSClient.LOG.info("Error Recovery for " + block +
                " waiting for responder to exit. ");
        return true;
      }
      closeStream();

      // move packets from ack queue to front of the data queue
      synchronized (dataQueue) {
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      boolean doSleep = setupPipelineForAppendOrRecovery();

      if (!streamerClosed && dfsClient.clientRunning) {
        if (stage == BlockConstructionStage.PIPELINE_CLOSE) {

          // If we had an error while closing the pipeline, we go through a fast-path
          // where the BlockReceiver does not run. Instead, the DataNode just finalizes
          // the block immediately during the 'connect ack' process. So, we want to pull
          // the end-of-block packet from the dataQueue, since we don't actually have
          // a true pipeline to send it over.
          //
          // We also need to set lastAckedSeqno to the end-of-block Packet's seqno, so that
          // a client waiting on close() will be aware that the flush finished.
          synchronized (dataQueue) {
            assert dataQueue.size() == 1;
            Packet endOfBlockPacket =
                    dataQueue.remove();  // remove the end of block packet
            assert endOfBlockPacket.lastPacketInBlock;
            assert lastAckedSeqno == endOfBlockPacket.seqno - 1;
            lastAckedSeqno = endOfBlockPacket.seqno;
            dataQueue.notifyAll();
          }
          endBlock();
        } else {
          initDataStreaming();
        }
      }

      return doSleep;
    }

    private void setHflush() {
      isHflushed = true;
    }

    private int findNewDatanode(final DatanodeInfo[] original)
            throws IOException {
      if (nodes.length != original.length + 1) {
        throw new IOException(new StringBuilder().append(
                "Failed to replace a bad datanode on the existing pipeline ")
                .append("due to no more good datanodes being available to try. ")
                .append("(Nodes: current=").append(Arrays.asList(nodes))
                .append(", original=").append(Arrays.asList(original)).append("). ")
                .append("The current failed datanode replacement policy is ")
                .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
                .append("a client may configure this via '").append(
                        DFSConfigKeys.DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY)
                .append("' in its configuration.").toString());
      }
      for (int i = 0; i < nodes.length; i++) {
        int j = 0;
        for (; j < original.length && !nodes[i].equals(original[j]); j++) {
          ;
        }
        if (j == original.length) {
          return i;
        }
      }
      throw new IOException(
              "Failed: new datanode not found: nodes=" + Arrays.asList(nodes) +
                      ", original=" + Arrays.asList(original));
    }

    private void addDatanode2ExistingPipeline() throws IOException {
      if (DataTransferProtocol.LOG.isDebugEnabled()) {
        DataTransferProtocol.LOG.debug("lastAckedSeqno = " + lastAckedSeqno);
      }
      /*
       * Is data transfer necessary?  We have the following cases.
       * 
       * Case 1: Failure in Pipeline Setup
       * - Append
       *    + Transfer the stored replica, which may be a RBW or a finalized.
       * - Create
       *    + If no data, then no transfer is required.
       *    + If there are data written, transfer RBW. This case may happens 
       *      when there are streaming failure earlier in this pipeline.
       *
       * Case 2: Failure in Streaming
       * - Append/Create:
       *    + transfer RBW
       * 
       * Case 3: Failure in Close
       * - Append/Create:
       *    + no transfer, let NameNode replicates the block.
       */
      if (!isAppend && lastAckedSeqno < 0 &&
              stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
        //no data have been written
        return;
      } else if (stage == BlockConstructionStage.PIPELINE_CLOSE ||
              stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
        //pipeline is closing
        return;
      }

      //get a new datanode
      final DatanodeInfo[] original = nodes;
      final LocatedBlock lb = dfsClient.getAdditionalDatanode(src, block, nodes,
              failed.toArray(new DatanodeInfo[failed.size()]), 1,
              dfsClient.clientName);
      nodes = lb.getLocations();

      //find the new datanode
      final int d = findNewDatanode(original);

      //transfer replica
      final DatanodeInfo src = d == 0 ? nodes[1] : nodes[d - 1];
      final DatanodeInfo[] targets = {nodes[d]};
      transfer(src, targets, lb.getBlockToken());
    }

    private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
                          final Token<BlockTokenIdentifier> blockToken) throws IOException {
      //transfer replica to the new datanode
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock = createSocketForPipeline(src, 2, dfsClient);
        final long writeTimeout = dfsClient.getDatanodeWriteTimeout(2);

        OutputStream unbufOut = NetUtils.getOutputStream(sock, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(sock);
        if (dfsClient.shouldEncryptData()) {
          IOStreamPair encryptedStreams = DataTransferEncryptor
                  .getEncryptedStreams(unbufOut, unbufIn,
                          dfsClient.getDataEncryptionKey());
          unbufOut = encryptedStreams.out;
          unbufIn = encryptedStreams.in;
        }
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
                HdfsConstants.SMALL_BUFFER_SIZE));
        in = new DataInputStream(unbufIn);

        //send the TRANSFER_BLOCK request
        new Sender(out)
                .transferBlock(block, blockToken, dfsClient.clientName, targets);
        out.flush();

        //ack
        BlockOpResponseProto response =
                BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
        if (SUCCESS != response.getStatus()) {
          throw new IOException("Failed to add a datanode");
        }
      } finally {
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }

    /**
     * Open a DataOutputStream to a DataNode pipeline so that
     * it can be written to.
     * This happens when a file is appended or data streaming fails
     * It keeps on trying until a pipeline is setup
     */
    private boolean setupPipelineForAppendOrRecovery() throws IOException {
      // check number of datanodes
      if (nodes == null || nodes.length == 0) {
        String msg =
                "Could not get block locations. " + "Source file \"" + src +
                        "\" - Aborting...";
        DFSClient.LOG.warn(msg);
        setLastException(new IOException(msg));
        streamerClosed = true;
        return false;
      }

      boolean success = false;
      long newGS = 0L;
      while (!success && !streamerClosed && dfsClient.clientRunning) {
        boolean isRecovery = hasError;
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex >= 0) {
          StringBuilder pipelineMsg = new StringBuilder();
          for (int j = 0; j < nodes.length; j++) {
            pipelineMsg.append(nodes[j]);
            if (j < nodes.length - 1) {
              pipelineMsg.append(", ");
            }
          }
          if (nodes.length <= 1) {
            lastException = new IOException(
                    "All datanodes " + pipelineMsg + " are bad. Aborting...");
            streamerClosed = true;
            return false;
          }
          DFSClient.LOG.warn("Error Recovery for block " + block +
                  " in pipeline " + pipelineMsg +
                  ": bad datanode " + nodes[errorIndex]);
          failed.add(nodes[errorIndex]);

          DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length - 1];
          System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
          System.arraycopy(nodes, errorIndex + 1, newnodes, errorIndex,
                  newnodes.length - errorIndex);
          nodes = newnodes;
          hasError = false;
          lastException = null;
          errorIndex = -1;
        }

        // Check if replace-datanode policy is satisfied.
        if (dfsClient.dtpReplaceDatanodeOnFailure
                .satisfy(blockReplication, nodes, isAppend, isHflushed)) {
          addDatanode2ExistingPipeline();
        }

        // get a new generation stamp and an access token
        LocatedBlock lb =
                dfsClient.updateBlockForPipeline(block, dfsClient.clientName);
        newGS = lb.getBlock().getGenerationStamp();
        accessToken = lb.getBlockToken();

        // set up the pipeline again with the remaining nodes
        success = createBlockOutputStream(nodes, newGS, isRecovery);
      }

      if (success) {
        // update pipeline at the namenode
        ExtendedBlock newBlock =
                new ExtendedBlock(block.getBlockPoolId(), block.getBlockId(),
                        block.getNumBytes(), newGS);
        dfsClient.updatePipeline(dfsClient.clientName, block, newBlock, nodes);
        // update client side generation stamp
        block = newBlock;
      }
      return false; // do not sleep, continue processing
    }

    private DatanodeInfo[] setupPipelineForSingleBlock(LocatedBlock lb)
            throws IOException {
      DatanodeInfo[] nodes;
      int count = dfsClient.getConf().nBlockWriteRetry;
      boolean success;
      do {
        hasError = false;
        lastException = null;
        errorIndex = -1;

        block = lb.getBlock();
        block.setNumBytes(0);
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();

        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, 0L, false);

        if (!success) {
          // TODO Request another location from the NameNode
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to initiate single block send.");
      }
      return nodes;
    }

    private DatanodeInfo[] nextBlockOutputStream(String client)
            throws IOException {
      LocatedBlock lb = null;
      DatanodeInfo[] nodes = null;
      int count = dfsClient.getConf().nBlockWriteRetry;
      boolean success = false;
      ExtendedBlock oldBlock = block;
      do {
        hasError = false;
        lastException = null;
        errorIndex = -1;
        success = false;

        DatanodeInfo[] excluded;
        long startTime = Time.now();

        if ((erasureCodingSourceStream &&
                currentBlockIndex % stripeLength == 0)) {
          usedNodes.clear();
          LOG.info("Stripe length " + stripeLength + " parity length " +
                  parityLength);
          LOG.info("Source write block index " + currentBlockIndex);
        }

        if (erasureCodingParityStream &&
                currentBlockIndex % parityLength == 0) {
          usedNodes.clear();
          stripeNodes.clear();
          int stripe =
                  (int) Math.ceil(currentBlockIndex / (float) parityLength);
          int index = stripe * stripeLength;
          LOG.info("Stripe length " + stripeLength + " parity length " +
                  parityLength);
          LOG.info("Parity write block index " + currentBlockIndex +
                  " found index " + index + " end " + (index + stripeLength));
          for (int j = index;
               j < sourceBlocks.size() && j < index + stripeLength; j++) {
            DatanodeInfo[] nodeInfos = sourceBlocks.get(j).getLocations();
            Collections.addAll(stripeNodes, nodeInfos);
          }
        }

        if (erasureCodingSourceStream || erasureCodingParityStream) {
          excluded = new DatanodeInfo[excludedNodes.size() + usedNodes.size() +
                  stripeNodes.size() + parityStripeNodes.size()];
          int i = 0;
          for (DatanodeInfo node : excludedNodes) {
            excluded[i] = node;
            LOG.info("Excluding node " + node);
            i++;
          }
          for (DatanodeInfo node : usedNodes) {
            excluded[i] = node;
            LOG.info((erasureCodingSourceStream ?
                    "Source stream: " : " Parity stream: ")
                    + "Block " + currentBlockIndex + " excluding used node "
                    + node);
            i++;
          }
          for (DatanodeInfo node : stripeNodes) {
            excluded[i] = node;
            LOG.info((erasureCodingSourceStream ?
                    "Source stream: " : " Parity stream: ")
                    + "Block " + currentBlockIndex + " excluding stripe node "
                    + node);
            i++;
          }
          for (DatanodeInfo node : parityStripeNodes) {
            excluded[i] = node;
            LOG.info((erasureCodingSourceStream ?
                    "Source stream: " : " Parity stream: ")
                    + "Block " + currentBlockIndex + " excluding parity node "
                    + node);
            i++;
          }
          currentBlockIndex++;
        } else {
          excluded =
                  excludedNodes.toArray(new DatanodeInfo[excludedNodes.size()]);
        }
        block = oldBlock;
        lb = locateFollowingBlock(startTime,
                excluded.length > 0 ? excluded : null);
        block = lb.getBlock();
        block.setNumBytes(0);
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();

        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, 0L, false);

        if (!success) {
          DFSClient.LOG.info("Abandoning " + block);
          dfsClient.abandonBlock(block, src, dfsClient.clientName);
          block = null;
          DFSClient.LOG.info("Excluding datanode " + nodes[errorIndex]);
          excludedNodes.add(nodes[errorIndex]);
        }
      } while (!success && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }

      if (erasureCodingSourceStream || erasureCodingParityStream) {
        Collections.addAll(usedNodes, nodes);
      }

      return nodes;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    private boolean createBlockOutputStream(DatanodeInfo[] nodes, long newGS,
                                            boolean recoveryFlag) {
      Status pipelineStatus = SUCCESS;
      String firstBadLink = "";
      if (DFSClient.LOG.isDebugEnabled()) {
        for (DatanodeInfo node : nodes) {
          DFSClient.LOG.debug("pipeline = " + node);
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks.set(true);

      int refetchEncryptionKey = 1;
      while (true) {
        boolean result = false;
        DataOutputStream out = null;
        try {
          assert null == s : "Previous socket unclosed";
          assert
                  null == blockReplyStream : "Previous blockReplyStream unclosed";
          s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
          long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);

          OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
          InputStream unbufIn = NetUtils.getInputStream(s);
          if (dfsClient.shouldEncryptData()) {
            IOStreamPair encryptedStreams = DataTransferEncryptor
                    .getEncryptedStreams(unbufOut, unbufIn,
                            dfsClient.getDataEncryptionKey());
            unbufOut = encryptedStreams.out;
            unbufIn = encryptedStreams.in;
          }
          out = new DataOutputStream(new BufferedOutputStream(unbufOut,
                  HdfsConstants.SMALL_BUFFER_SIZE));
          blockReplyStream = new DataInputStream(unbufIn);

          //
          // Xmit header info to datanode
          //

          // send the request
          new Sender(out)
                  .writeBlock(block, accessToken, dfsClient.clientName, nodes, null,
                          recoveryFlag ? stage.getRecoveryStage() : stage, nodes.length,
                          block.getNumBytes(), bytesSent, newGS, checksum);

          // receive ack for connect
          BlockOpResponseProto resp = BlockOpResponseProto
                  .parseFrom(PBHelper.vintPrefixed(blockReplyStream));
          pipelineStatus = resp.getStatus();
          firstBadLink = resp.getFirstBadLink();

          if (pipelineStatus != SUCCESS) {
            if (pipelineStatus == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                      "Got access token error for connect ack with firstBadLink as " +
                              firstBadLink);
            } else {
              throw new IOException(
                      "Bad connect ack with firstBadLink as " + firstBadLink);
            }
          }
          assert null == blockStream : "Previous blockStream unclosed";
          blockStream = out;
          result = true; // success

        } catch (IOException ie) {
          DFSClient.LOG.info("Exception in createBlockOutputStream", ie);
          if (ie instanceof InvalidEncryptionKeyException &&
                  refetchEncryptionKey > 0) {
            DFSClient.LOG.info("Will fetch a new encryption key and retry, " +
                    "encryption key was invalid when connecting to " + nodes[0] +
                    " : " + ie);
            // The encryption key used is invalid.
            refetchEncryptionKey--;
            dfsClient.clearDataEncryptionKey();
            // Don't close the socket/exclude this node just yet. Try again with
            // a new encryption key.
            continue;
          }

          // find the datanode that matches
          if (firstBadLink.length() != 0) {
            for (int i = 0; i < nodes.length; i++) {
              // NB: Unconditionally using the xfer addr w/o hostname
              if (firstBadLink.equals(nodes[i].getXferAddr())) {
                errorIndex = i;
                break;
              }
            }
          } else {
            errorIndex = 0;
          }
          hasError = true;
          setLastException(ie);
          result = false;  // error
        } finally {
          if (!result) {
            IOUtils.closeSocket(s);
            s = null;
            IOUtils.closeStream(out);
            out = null;
            IOUtils.closeStream(blockReplyStream);
            blockReplyStream = null;
          }
        }
        return result;
      }
    }

    private LocatedBlock locateFollowingBlock(long start,
                                              DatanodeInfo[] excludedNodes)
            throws IOException, UnresolvedLinkException {
      int retries = dfsClient.getConf().nBlockWriteLocateFollowingRetry;

      long sleeptime = 1000;  //HOP default value was 400

      while (true) {
        long localstart = Time.now();
        while (true) {
          try {
            return dfsClient
                    .addBlock(src, dfsClient.clientName, block, excludedNodes);
          } catch (RemoteException e) {
            IOException ue =
                    e.unwrapRemoteException(FileNotFoundException.class,
                            AccessControlException.class,
                            NSQuotaExceededException.class,
                            DSQuotaExceededException.class,
                            UnresolvedPathException.class);
            if (ue != e) {
              throw ue; // no need to retry these exceptions
            }


            if (NotReplicatedYetException.class.getName().
                    equals(e.getClassName())) {
              if (retries == 0) {
                throw e;
              } else {
                --retries;
                DFSClient.LOG.debug("Exception while adding a block", e);
                if (Time.now() - localstart > 5000) {
                  DFSClient.LOG.debug("Waiting for replication for " +
                          (Time.now() - localstart) / 1000 + " seconds");
                }
                try {
                  Thread.sleep(sleeptime);
                  sleeptime *= 2;
                } catch (InterruptedException ie) {
                }
              }
            } else {
              throw e;
            }

          }
        }
      }
    }

    ExtendedBlock getBlock() {
      return block;
    }

    DatanodeInfo[] getNodes() {
      return nodes;
    }

    Token<BlockTokenIdentifier> getBlockToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      if (lastException == null) {
        lastException = e;
      }
    }
  }

  /**
   * Create a socket for a write pipeline
   *
   * @param first
   *     the first datanode
   * @param length
   *     the pipeline length
   * @param client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
                                        final int length, final DFSClient client) throws IOException {
    final String dnAddr = first.getXferAddr(client.connectToDnViaHostname());
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    //final Socket sock = client.socketFactory.createSocket();
    
    SocketFactory socketFactory = new StandardSocketFactory();
    final Socket sock = socketFactory.createSocket();
    
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), timeout);
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }

  private void isClosed() throws IOException {
    if (closed) {
      IOException e = lastException;
      throw e != null ? e : new IOException("DFSOutputStream is closed");
    }
  }

  //
  // returns the list of targets, if any, that is being currently used.
  //
  @VisibleForTesting
  public synchronized DatanodeInfo[] getPipeline() {
    if (streamer == null) {
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

  private DFSOutputStream(DFSClient dfsClient, String src, long blockSize,
                          Progressable progress, DataChecksum checksum, short replication, boolean saveSmallFilesInDB,
                          final int dbFileMaxSize)
          throws IOException {
    super(checksum, checksum.getBytesPerChecksum(), checksum.getChecksumSize());
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    this.dfsClient = dfsClient;
    this.src = src;
    this.blockSize = blockSize;
    this.blockReplication = replication;
    this.progress = progress;
    this.saveSmallFilesInDB = saveSmallFilesInDB;
    if (saveSmallFilesInDB) {
      isThisFileStoredInDB = true; // treat the current file as small file
    }
    this.dbFileMaxSize = dbFileMaxSize;
    if ((progress != null) && DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG
              .debug("Set non-null progress callback on DFSOutputStream " + src);
    }

    if (bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
      throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
              ") and blockSize(" + blockSize +
              ") do not match. " + "blockSize should be a " +
              "multiple of io.bytes.per.checksum");

    }
    this.checksum = checksum;
    DFSClient.LOG.debug("Stuffed Inode:  Stroing small files in the database: " + saveSmallFilesInDB + " Database file max " +
            "size: " + dbFileMaxSize);
  }

  /**
   * Construct a new output stream for creating a file.
   */
  private DFSOutputStream(DFSClient dfsClient, String src, FsPermission masked,
                          EnumSet<CreateFlag> flag, boolean createParent, short replication,
                          long blockSize, Progressable progress, int buffersize,
                          DataChecksum checksum, EncodingPolicy policy, boolean saveSmallFilesInDB, final int dbFileMaxSize)
          throws IOException {
    this(dfsClient, src, blockSize, progress, checksum, replication, saveSmallFilesInDB, dbFileMaxSize);
    this.shouldSyncBlock = flag.contains(CreateFlag.SYNC_BLOCK);

    if (policy != null) {
      Codec codec = Codec.getCodec(policy.getCodec());
      if (codec == null) {
        throw new IOException("Unkown codec: " + policy.getCodec());
      }
      enableSourceStream(codec.getStripeLength());
    }

    computePacketChunkSize(dfsClient.getConf().writePacketSize,
            checksum.getBytesPerChecksum());

    try {
      dfsClient.create(src, masked, dfsClient.clientName,
          new EnumSetWritable<>(flag), createParent, replication,
              blockSize, policy);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              DSQuotaExceededException.class, FileAlreadyExistsException.class,
              FileNotFoundException.class, ParentNotDirectoryException.class,
              NSQuotaExceededException.class, SafeModeException.class,
              UnresolvedPathException.class);
    }
    streamer = new DataStreamer();
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
                                            FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
                                            short replication, long blockSize, Progressable progress, int buffersize,
                                            DataChecksum checksum, boolean saveSmallFilesInDB, final int dbFileMaxSize) throws IOException {
    return newStreamForCreate(dfsClient, src, masked, flag, createParent,
            replication, blockSize, progress, buffersize, checksum, null, saveSmallFilesInDB, dbFileMaxSize);
  }

  static DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
                                            FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
                                            short replication, long blockSize, Progressable progress, int buffersize,
                                            DataChecksum checksum, EncodingPolicy policy, boolean saveSmallFilesInDB,
                                            final int dbFileMaxSize) throws IOException {
    final DFSOutputStream out =
            new DFSOutputStream(dfsClient, src, masked, flag, createParent,
                    replication, blockSize, progress, buffersize, checksum, policy, saveSmallFilesInDB, dbFileMaxSize);
    out.streamer.start();
    return out;
  }

  /**
   * Construct a new output stream for append.
   */
  private DFSOutputStream(DFSClient dfsClient, String src, int buffersize,
                          Progressable progress, LocatedBlock lastBlock, HdfsFileStatus stat,
                          DataChecksum checksum, boolean saveSmallFilesInDB, final int dbFileMaxSize)
      throws IOException {
    this(dfsClient, src, stat.getBlockSize(), progress, checksum, stat.getReplication(), saveSmallFilesInDB,
        dbFileMaxSize);

    initialFileSize = stat.getLen(); // length of file when opened
    if (!stat.isFileStoredInDB()) {
      isThisFileStoredInDB = false;
    } else {
      isThisFileStoredInDB = true;
    }
    //
    // The last partial block of the file has to be filled.
    //
    if (lastBlock != null && !isThisFileStoredInDB) {
      LOG.debug("Stuffed Inode:  appending to a file stored on datanodes");
      // indicate that we are appending to an existing block
      bytesCurBlock = lastBlock.getBlockSize();
      streamer = new DataStreamer(lastBlock, stat, checksum.getBytesPerChecksum());
    } else {

      computePacketChunkSize(dfsClient.getConf().writePacketSize, checksum.getBytesPerChecksum());
      streamer = new DataStreamer();
      if (isThisFileStoredInDB && lastBlock != null) {
        bytesCurBlock = 0;
        write(lastBlock.getData(), 0, lastBlock.getData().length);
        LOG.debug("Stuffed Inode:  Putting Existing data in packets");
      }
    }
  }

  static DFSOutputStream newStreamForAppend(DFSClient dfsClient, String src,
                                            int buffersize, Progressable progress, LocatedBlock lastBlock,
                                            HdfsFileStatus stat, DataChecksum checksum, boolean saveSmallFilesInDB,
                                            final int dbFileMaxSize) throws IOException {
    String errorMessage = null;
    if ((stat.isFileStoredInDB() == true && !saveSmallFilesInDB)) {
      errorMessage = "The file is stored in the database. Parameter to store the data in the database is disabled. " +
              "Set the " + DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY + " configuration parameter in the hdfs " +
              "configuration file";
    } else if (stat.isFileStoredInDB()) {
      if (stat.getLen() > stat.getBlockSize()) {
        errorMessage = "Invalid paraters for appending a file stored in the database. Block size can not be smaller " +
                "than the max size of a file stored in the database";
      } else if (dbFileMaxSize > stat.getBlockSize()) {
        errorMessage = "Invalid paraters for appending a file stored in the database. Files stored in the database " +
                "can not be larger than a HDFS block";
      }
    }

    if (errorMessage != null) {
      throw new IOException(errorMessage + " Stat.isStoredInDB: " + stat.isFileStoredInDB() +
              " saveSmallFilesInDB: " + saveSmallFilesInDB + " Stat.len: " + stat.getLen() + " dbFileMaxSize: " +
              dbFileMaxSize + " BlockSize: " + stat.getBlockSize());
    }


    final DFSOutputStream out =
            new DFSOutputStream(dfsClient, src, buffersize, progress, lastBlock,
                    stat, checksum, saveSmallFilesInDB, dbFileMaxSize);
    out.streamer.start();
    return out;
  }

  /**
   * Construct a new output stream for a single block.
   */
  private DFSOutputStream(DFSClient dfsClient, String src, int buffersize,
                          Progressable progress, LocatedBlock lb, DataChecksum checksum)
          throws IOException {
    this(dfsClient, src, lb.getBlockSize(), progress, checksum,
            (short) lb.getLocations().length, false, -1);
    singleBlock = true;

    computePacketChunkSize(dfsClient.getConf().writePacketSize,
            checksum.getBytesPerChecksum());
    streamer = new DataStreamer(lb);
  }

  static DFSOutputStream newStreamForSingleBlock(DFSClient dfsClient,
                                                 String src, int buffersize, Progressable progress, LocatedBlock block,
                                                 DataChecksum checksum) throws IOException {
    final DFSOutputStream out =
            new DFSOutputStream(dfsClient, src, buffersize, progress, block,
                    checksum);
    out.streamer.start();
    return out;
  }

  private void computePacketChunkSize(int psize, int csize) {
    int chunkSize = csize + checksum.getChecksumSize();
    chunksPerPacket = Math.max(psize / chunkSize, 1);
    packetSize = chunkSize * chunksPerPacket;
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("computePacketChunkSize: src=" + src +
              ", chunkSize=" + chunkSize +
              ", chunksPerPacket=" + chunksPerPacket +
              ", packetSize=" + packetSize);
    }
  }

  private void queueCurrentPacket() {
    synchronized (dataQueue) {
      if (currentPacket == null) {
        return;
      }

      // put it is the small files buffer
      if (canStoreFileInDB() && (currentPacket.getLastByteOffsetBlock() <= dbFileMaxSize)) {
        LOG.debug("Stuffed Inode:  Temporarily withholding the packet in a buffer for small files");
        smallFileDataQueue.addLast(currentPacket);
      } else {
        //Some condition for storing the data in the database has failed. Store the data on the datanodes
        forwardSmallFilesPacketsToDataNodes();

        dataQueue.addLast(currentPacket);
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Queued packet " + currentPacket.seqno);
        }
      }
      lastQueuedSeqno = currentPacket.seqno;
      currentPacket = null;
      dataQueue.notifyAll();
    }
  }

  private boolean canStoreFileInDB() {
    return saveSmallFilesInDB &&
            isThisFileStoredInDB &&
            !syncOrFlushCalled;
  }

  private void forwardSmallFilesPacketsToDataNodes() {
    // can not save the data in the database
    if (saveSmallFilesInDB && isThisFileStoredInDB) {
      LOG.debug("Stuffed Inode:  The file can not be stored  in the database");
      isThisFileStoredInDB = false;
      if (!smallFileDataQueue.isEmpty()) {
        LOG.debug("Stuffed Inode:  Sync/Flush Called: " + syncOrFlushCalled + " Current File Size: " + currentPacket
                .getLastByteOffsetBlock() + " Max size of a file in DB: " + dbFileMaxSize);

        for (Packet packet : smallFileDataQueue) {
          dataQueue.addLast(packet);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Queued packet " + packet.seqno);
          }
        }
        smallFileDataQueue.clear();
      }
    }
  }

  private void waitAndQueueCurrentPacket() throws IOException {
    synchronized (dataQueue) {
      // If queue is full, then wait till we have enough space
      while (!closed && dataQueue.size() + ackQueue.size() > MAX_PACKETS) {
        try {
          dataQueue.wait();
        } catch (InterruptedException e) {
          // If we get interrupted while waiting to queue data, we still need to get rid
          // of the current packet. This is because we have an invariant that if
          // currentPacket gets full, it will get queued before the next writeChunk.
          //
          // Rather than wait around for space in the queue, we should instead try to
          // return to the caller as soon as possible, even though we slightly overrun
          // the MAX_PACKETS iength.
          Thread.currentThread().interrupt();
          break;
        }
      }
      isClosed();
      queueCurrentPacket();
    }
  }

  // @see FSOutputSummer#writeChunk()
  @Override
  protected synchronized void writeChunk(byte[] b, int offset, int len,
                                         byte[] checksum) throws IOException {
    dfsClient.checkOpen();
    isClosed();

    int cklen = checksum.length;
    int bytesPerChecksum = this.checksum.getBytesPerChecksum();
    if (len > bytesPerChecksum) {
      throw new IOException("writeChunk() buffer size is " + len +
              " is larger than supported  bytesPerChecksum " +
              bytesPerChecksum);
    }
    if (checksum.length != this.checksum.getChecksumSize()) {
      throw new IOException("writeChunk() checksum size is supposed to be " +
              this.checksum.getChecksumSize() +
              " but found to be " + checksum.length);
    }

    if (currentPacket == null) {
      currentPacket = new Packet(packetSize, chunksPerPacket, bytesCurBlock);
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG
                .debug("DFSClient writeChunk allocating new packet seqno=" +
                        currentPacket.seqno +
                        ", src=" + src +
                        ", packetSize=" + packetSize +
                        ", chunksPerPacket=" + chunksPerPacket +
                        ", bytesCurBlock=" + bytesCurBlock);
      }
    }

    currentPacket.writeChecksum(checksum, 0, cklen);
    currentPacket.writeData(b, offset, len);
    currentPacket.numChunks++;
    bytesCurBlock += len;

    // If packet is full, enqueue it for transmission
    //
    if (currentPacket.numChunks == currentPacket.maxChunks ||
            bytesCurBlock == blockSize) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("DFSClient writeChunk packet full seqno=" +
                currentPacket.seqno +
                ", src=" + src +
                ", bytesCurBlock=" + bytesCurBlock +
                ", blockSize=" + blockSize +
                ", appendChunk=" + appendChunk);
      }
      waitAndQueueCurrentPacket();

      // If the reopened file did not end at chunk boundary and the above
      // write filled up its partial chunk. Tell the summer to generate full 
      // crc chunks from now on.
      if (appendChunk && bytesCurBlock % bytesPerChecksum == 0) {
        appendChunk = false;
        resetChecksumChunk(bytesPerChecksum);
      }

      if (!appendChunk) {
        int psize = Math.min((int) (blockSize - bytesCurBlock),
                dfsClient.getConf().writePacketSize);
        computePacketChunkSize(psize, bytesPerChecksum);
      }
      //
      // if encountering a block boundary, send an empty packet to 
      // indicate the end of block and reset bytesCurBlock.
      //
      if (bytesCurBlock == blockSize) {
        currentPacket = new Packet(0, 0, bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
        currentPacket.syncBlock = shouldSyncBlock;
        waitAndQueueCurrentPacket();
        bytesCurBlock = 0;
        lastFlushOffset = 0;
      }
    }
  }

  @Override
  @Deprecated
  public void sync() throws IOException {
    hflush();
  }

  /**
   * Flushes out to all replicas of the block. The data is in the buffers
   * of the DNs but not necessarily in the DN's OS buffers.
   * <p/>
   * It is a synchronous operation. When it returns,
   * it guarantees that flushed data become visible to new readers.
   * It is not guaranteed that data has been flushed to
   * persistent store on the datanode.
   * Block allocations are persisted on namenode.
   */
  @Override
  public void hflush() throws IOException {
    flushOrSync(false, EnumSet.noneOf(SyncFlag.class));
  }

  @Override
  public void hsync() throws IOException {
    hsync(EnumSet.noneOf(SyncFlag.class));
  }

  /**
   * The expected semantics is all data have flushed out to all replicas
   * and all replicas have done posix fsync equivalent - ie the OS has
   * flushed it to the disk device (but the disk may have it in its cache).
   * <p/>
   * Note that only the current block is flushed to the disk device.
   * To guarantee durable sync across block boundaries the stream should
   * be created with {@link CreateFlag#SYNC_BLOCK}.
   *
   * @param syncFlags
   *     Indicate the semantic of the sync. Currently used to specify
   *     whether or not to update the block length in NameNode.
   */
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    flushOrSync(true, syncFlags);
  }

  /**
   * Flush/Sync buffered data to DataNodes.
   *
   * @param isSync
   *     Whether or not to require all replicas to flush data to the disk
   *     device
   * @param syncFlags
   *     Indicate extra detailed semantic of the flush/sync. Currently
   *     mainly used to specify whether or not to update the file length in
   *     the NameNode
   * @throws IOException
   */
  private void flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags)
          throws IOException {
    dfsClient.checkOpen();
    isClosed();
    syncOrFlushCalled = true;
    try {

      long toWaitFor;
      long lastBlockLength = -1L;
      boolean updateLength = syncFlags.contains(SyncFlag.UPDATE_LENGTH);
      synchronized (this) {
        /* Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed.
         * After the flush, reset the bytesCurBlock back to its previous value,
         * any partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;
        Packet oldCurrentPacket = currentPacket;
        // flush checksum buffer, but keep checksum buffer intact
        flushBuffer(true);
        // bytesCurBlock potentially incremented if there was buffered data

        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("DFSClient flush() : saveOffset " + saveOffset +
                  " bytesCurBlock " + bytesCurBlock +
                  " lastFlushOffset " + lastFlushOffset);
        }
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {
          assert bytesCurBlock > lastFlushOffset;
          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;
          if (isSync && currentPacket == null) {
            // Nothing to send right now,
            // but sync was requested.
            // Send an empty packet
            currentPacket =
                    new Packet(packetSize, chunksPerPacket, bytesCurBlock);
          }
        } else {
          // We already flushed up to this offset.
          // This means that we haven't written anything since the last flush
          // (or the beginning of the file). Hence, we should not have any
          // packet queued prior to this call, since the last flush set
          // currentPacket = null.
          assert oldCurrentPacket ==
                  null : "Empty flush should not occur with a currentPacket";

          if (isSync && bytesCurBlock > 0) {
            // Nothing to send right now,
            // and the block was partially written,
            // and sync was requested.
            // So send an empty sync packet.
            currentPacket =
                    new Packet(packetSize, chunksPerPacket, bytesCurBlock);
          } else {
            // just discard the current packet since it is already been sent.
            currentPacket = null;
          }
        }
        if (currentPacket != null) {
          currentPacket.syncBlock = isSync;
          waitAndQueueCurrentPacket();
        }
        // Restore state of stream. Record the last flush offset 
        // of the last full chunk that was flushed.
        //
        bytesCurBlock = saveOffset;
        toWaitFor = lastQueuedSeqno;
      } // end synchronized

      waitForAckedSeqno(toWaitFor);

      if (updateLength) {
        synchronized (this) {
          if (streamer != null && streamer.block != null) {
            lastBlockLength = streamer.block.getNumBytes();
          }
        }
      }
      // If 1) any new blocks were allocated since the last flush, or 2) to
      // update length in NN is requried, then persist block locations on
      // namenode.
      if (persistBlocks.getAndSet(false) || updateLength) {
        try {
          dfsClient.fsync(src, dfsClient.clientName, lastBlockLength);
        } catch (IOException ioe) {
          DFSClient.LOG
                  .warn("Unable to persist blocks in hflush for " + src, ioe);
          // If we got an error here, it might be because some other thread called
          // close before our hflush completed. In that case, we should throw an
          // exception that the stream is closed.
          isClosed();
          // If we aren't closed but failed to sync, we should expose that to the
          // caller.
          throw ioe;
        }
      }

      synchronized (this) {
        if (streamer != null) {
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
        if (!closed) {
          lastException = new IOException("IOException flush:" + e);
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
    isClosed();
    if (streamer == null) {
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
  private void flushInternal() throws IOException {
    long toWaitFor;
    synchronized (this) {
      dfsClient.checkOpen();
      isClosed();
      //
      // If there is data in the current buffer, send it across
      //
      queueCurrentPacket();
      toWaitFor = lastQueuedSeqno;
    }

    waitForAckedSeqno(toWaitFor);
  }

  private void waitForAckedSeqno(long seqno) throws IOException {
    if (canStoreFileInDB()) {
      LOG.debug("Stuffed Inode:  Closing File. Datanode ack skipped. All the data will be stored in the database");
    } else {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("Waiting for ack for: " + seqno);
      }
      synchronized (dataQueue) {
        while (!closed) {
          isClosed();
          if (lastAckedSeqno >= seqno) {
            break;
          }
          try {
            dataQueue
                    .wait(1000); // when we receive an ack, we notify on dataQueue
          } catch (InterruptedException ie) {
            throw new InterruptedIOException(
                    "Interrupted while waiting for data to be acknowledged by pipeline");
          }
        }
      }
      isClosed();
    }
  }

  /**
   * Aborts this output stream and releases any system
   * resources associated with this stream.
   */
  synchronized void abort() throws IOException {
    if (closed) {
      return;
    }
    streamer.setLastException(new IOException("Lease timeout of " +
            (dfsClient.hdfsTimeout / 1000) + " seconds expired."));
    closeThreads(true);
    dfsClient.endFileLease(src);
  }

  // shutdown datastreamer and responseprocessor threads.
  // interrupt datastreamer if force is true
  private void closeThreads(boolean force) throws IOException {
    try {
      streamer.close(force);
      streamer.join();
      if (s != null) {
        s.close();
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed to shutdown streamer");
    } finally {
      streamer = null;
      s = null;
      closed = true;
    }
  }

  /**
   * Closes this output stream and releases any system
   * resources associated with this stream.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      IOException e = lastException;
      if (e == null) {
        return;
      } else {
        throw e;
      }
    }

    try {
      flushBuffer();       // flush from all upper layers

      if (currentPacket != null) {
        waitAndQueueCurrentPacket();
      }

      if (bytesCurBlock != 0) {
        // send an empty packet to mark the end of the block
        currentPacket = new Packet(0, 0, bytesCurBlock);
        currentPacket.lastPacketInBlock = true;
        currentPacket.syncBlock = shouldSyncBlock;
      }

      flushInternal();             // flush all data to Datanodes
      // get last block before destroying the streamer
      ExtendedBlock lastBlock = streamer.getBlock();
      closeThreads(false);
      completeFile(lastBlock);
      dfsClient.endFileLease(src);
    } finally {
      closed = true;
    }
  }

  // should be called holding (this) lock since setTestFilename() may 
  // be called during unit tests
  private void completeFile(ExtendedBlock last) throws IOException {
    if (singleBlock) {
      return;
    }

    long localstart = Time.now();
    long localtimeout = 400;
    boolean fileComplete = false;
    while (!fileComplete) {
      byte data[] = null;
      if (canStoreFileInDB()) {
        if (!this.smallFileDataQueue.isEmpty()) {
          LOG.debug("Stuffed Inode:  Sending data to the NameNode in comple file operation ");
          int length = 0;
          for (Packet packet : smallFileDataQueue) {
            LOG.debug("Stuffed Inode:  No: " + packet.seqno + " " + packet.dataStart + " " + packet.dataPos + " " + packet
                    .checksumStart + " " + packet
                    .checksumPos + " " + packet.buf.length);
            if (!packet.isHeartbeatPacket()) {
              length += (packet.dataPos - packet.dataStart);
            }
          }
          LOG.debug("Stuffed Inode:  total data is " + length);
          data = new byte[length];
          int index = 0;
          for (Packet packet : smallFileDataQueue) {
            System.arraycopy(packet.buf, packet.dataStart, data, index, (packet.dataPos - packet.dataStart));
            index += (packet.dataPos - packet.dataStart);
          }
        }
      }

      try {
        if((last != null && last.getNumBytes() > 0)) {
          // Default delay is 0.
          // A small delay ensures that the namenodes have processed
          // the incremental block reports before the complete file request.
          Thread.sleep(dfsClient.getConf().delayBeforeClose);
        }
      } catch (InterruptedException e) {
      }

      try {
        fileComplete =
            dfsClient.complete(src, dfsClient.clientName, last, data);
      }catch (RemoteException e){
        IOException ue =
            e.unwrapRemoteException(NSQuotaExceededException.class,
                DSQuotaExceededException.class);
        if (ue != e) {
          throw ue; // no need to retry these exceptions
        }else{
          throw e;
        }
      }

      if (!fileComplete) {
        if (!dfsClient.clientRunning || (dfsClient.hdfsTimeout > 0 &&
                localstart + dfsClient.hdfsTimeout < Time.now())) {
          String msg = "Unable to close file because dfsclient " +
                  " was unable to contact the HDFS servers." +
                  " clientRunning " + dfsClient.clientRunning +
                  " hdfsTimeout " + dfsClient.hdfsTimeout;
          DFSClient.LOG.info(msg);
          throw new IOException(msg);
        }
        try {
          Thread.sleep(localtimeout);
          localtimeout*=2;
          if (Time.now() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  @VisibleForTesting
  public void setArtificialSlowdown(long period) {
    artificialSlowdown = period;
  }

  @VisibleForTesting
  public synchronized void setChunksPerPacket(int value) {
    chunksPerPacket = Math.min(chunksPerPacket, value);
    packetSize = (checksum.getBytesPerChecksum() + checksum.getChecksumSize()) *
            chunksPerPacket;
  }

  synchronized void setTestFilename(String newname) {
    src = newname;
  }

  /**
   * Returns the size of a file as it was when this stream was opened
   */
  long getInitialLen() {
    return initialFileSize;
  }

  /**
   * Returns the access token currently used by streamer, for testing only
   */
  synchronized Token<BlockTokenIdentifier> getBlockToken() {
    return streamer.getBlockToken();
  }

  public void enableSourceStream(int stripeLength) {
    this.erasureCodingSourceStream = true;
    this.stripeLength = stripeLength;
  }

  public void enableParityStream(int stripeLength, int parityLength,
                                 String sourceFile) throws IOException {
    this.erasureCodingParityStream = true;
    this.stripeLength = stripeLength;
    this.parityLength = parityLength;
    if (sourceFile != null) {
      this.sourceBlocks = new ArrayList(dfsClient.getLocatedBlocks(
              sourceFile, 0, Long.MAX_VALUE).getLocatedBlocks());
      Collections.sort(sourceBlocks, LocatedBlock.blockIdComparator);
    }
  }

  @Override
  protected void checkClosed() throws IOException {
    if (closed) {
      IOException e = lastException;
      throw e != null ? e : new ClosedChannelException();
    }
  }

  public Collection<DatanodeInfo> getUsedNodes() {
    return usedNodes;
  }

  public void setParityStripeNodesForNextStripe(
          Collection<DatanodeInfo> locations) {
    parityStripeNodes.clear();
    parityStripeNodes.addAll(locations);
  }
}
