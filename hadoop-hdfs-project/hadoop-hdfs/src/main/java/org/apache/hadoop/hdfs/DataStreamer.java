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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import javax.net.SocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.StandardSocketFactory;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/*********************************************************************
 *
 * The DataStreamer class is responsible for sending data packets to the
 * datanodes in the pipeline. It retrieves a new blockid and block locations
 * from the namenode, and starts streaming packets to the pipeline of
 * Datanodes. Every packet has a sequence number associated with
 * it. When all the packets for a block are sent out and acks for each
 * if them are received, the DataStreamer closes the current block.
 *
 * The DataStreamer thread picks up packets from the dataQueue, sends it to
 * the first datanode in the pipeline and moves it from the dataQueue to the
 * ackQueue. The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the ackQueue.
 *
 * In case of error, all outstanding packets are moved from ackQueue. A new
 * pipeline is setup by eliminating the bad datanode from the original
 * pipeline. The DataStreamer now starts sending packets from the dataQueue.
 *
 *********************************************************************/

@InterfaceAudience.Private
class DataStreamer extends Daemon {
  static final Log LOG = LogFactory.getLog(DataStreamer.class);
  /**
   * Create a socket for a write pipeline
   *
   * @param first the first datanode
   * @param length the pipeline length
   * @param client client
   * @return the socket connected to the first datanode
   */
  static Socket createSocketForPipeline(final DatanodeInfo first,
      final int length, final DFSClient client) throws IOException {
    final DfsClientConf conf = client.getConf();
    final String dnAddr = first.getXferAddr(conf.isConnectToDnViaHostname());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr);
    }
    final InetSocketAddress isa = NetUtils.createSocketAddr(dnAddr);
    SocketFactory socketFactory = new StandardSocketFactory();
    final Socket sock = socketFactory.createSocket();
    final int timeout = client.getDatanodeReadTimeout(length);
    NetUtils.connect(sock, isa, client.getRandomLocalInterfaceAddr(), conf.getSocketTimeout());
    sock.setSoTimeout(timeout);
    sock.setSendBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Send buf size " + sock.getSendBufferSize());
    }
    return sock;
  }

  /**
   * release a list of packets to ByteArrayManager
   *
   * @param packets packets to be release
   * @param bam ByteArrayManager
   */
  private static void releaseBuffer(List<DFSPacket> packets, ByteArrayManager bam) {
    for(DFSPacket p : packets) {
      p.releaseBuffer(bam);
    }
    packets.clear();
  }
  
  static class LastExceptionInStreamer {
    private IOException thrown;

    synchronized void set(Throwable t) {
      assert t != null;
      this.thrown = t instanceof IOException ?
          (IOException) t : new IOException(t);
    }

    synchronized void clear() {
      thrown = null;
    }

    /** Check if there already is an exception. */
    synchronized void check(boolean resetToNull) throws IOException {
      if (thrown != null) {
        if (LOG.isTraceEnabled()) {
          // wrap and print the exception to know when the check is called
          LOG.trace("Got Exception while checking", new Throwable(thrown));
        }
        final IOException e = thrown;
        if (resetToNull) {
          thrown = null;
        }
        throw e;
      }
    }

    synchronized void throwException4Close() throws IOException {
      check(false);
      throw new ClosedChannelException();
    }
  }

  private volatile boolean streamerClosed = false;
  private ExtendedBlock block; // its length is number of bytes acked
  private Token<BlockTokenIdentifier> accessToken;
  private DataOutputStream blockStream;
  private DataInputStream blockReplyStream;
  private ResponseProcessor response = null;
  private volatile DatanodeInfo[] nodes = null; // list of targets for current block
  private volatile StorageType[] storageTypes = null;
  private volatile String[] storageIDs = null;
  volatile boolean hasError = false;
  volatile int errorIndex = -1;
  // Restarting node index
  AtomicInteger restartingNodeIndex = new AtomicInteger(-1);
  private long restartDeadline = 0; // Deadline of DN restart
  private BlockConstructionStage stage;  // block construction stage
  private long bytesSent = 0; // number of bytes that've been sent

  /** Nodes have been used in the pipeline before and have failed. */
  private final List<DatanodeInfo> failed = new ArrayList<>();
  /** The last ack sequence number before pipeline failure. */
  private long lastAckedSeqnoBeforeFailure = -1;
  private int pipelineRecoveryCount = 0;
  /** Has the current block been hflushed? */
  private boolean isHflushed = false;
  /** Append on an existing block? */
  private final boolean isAppend;

  private long currentSeqno = 0;
  private long lastQueuedSeqno = -1;
  private long lastAckedSeqno = -1;
  private long bytesCurBlock = 0; // bytes written in current block
  private final LastExceptionInStreamer lastException = new LastExceptionInStreamer();
  private Socket s;

  private final DFSClient dfsClient;
  private final String src;
  /** Only for DataTransferProtocol.writeBlock(..) */
  private final DataChecksum checksum;
  private final Progressable progress;
  private final HdfsFileStatus stat;
  // appending to existing partial block
  private volatile boolean appendChunk = false;
  // both dataQueue and ackQueue are protected by dataQueue lock
  private final LinkedList<DFSPacket> dataQueue = new LinkedList<>();
  private final LinkedList<DFSPacket> ackQueue = new LinkedList<>();
  private final AtomicReference<CachingStrategy> cachingStrategy;
  private final ByteArrayManager byteArrayManager;
  private static final BlockStoragePolicySuite blockStoragePolicySuite =
      BlockStoragePolicySuite.createDefaultSuite();
  //persist blocks on namenode
  private final AtomicBoolean persistBlocks = new AtomicBoolean(false);
  private boolean failPacket = false;
  private final long dfsclientSlowLogThresholdMs;
  private long artificialSlowdown = 0;
  // List of congested data nodes. The stream will back off if the DataNodes
  // are congested
  private final List<DatanodeInfo> congestedNodes = new ArrayList<>();
  private static final int CONGESTION_BACKOFF_MEAN_TIME_IN_MS = 5000;
  private static final int CONGESTION_BACK_OFF_MAX_TIME_IN_MS =
      CONGESTION_BACKOFF_MEAN_TIME_IN_MS * 10;
  private int lastCongestionBackoffTime;

  private final LoadingCache<DatanodeInfo, DatanodeInfo> excludedNodes;
  private final String[] favoredNodes;
  
  private final LinkedList<DFSPacket> smallFileDataQueue = new LinkedList<>();

  // Information for sending a single block
  private LocatedBlock lb;
  //erasure coding
  private boolean erasureCodingSourceStream = false;
  private int currentBlockIndex = 0;
  private int stripeLength;
  private HashSet<DatanodeInfo> usedNodes = new HashSet<>();
  private int parityLength;
  private boolean erasureCodingParityStream = false;
  private List<DatanodeInfo> stripeNodes = new LinkedList<>();
  private List<LocatedBlock> sourceBlocks = Collections.emptyList();
  private List<DatanodeInfo> parityStripeNodes = new LinkedList<>();
  
  //samll files in db
  private final int dbFileMaxSize;
  private final boolean forceClientToWriteSFToDisk;
  private boolean isThisFileStoredInDB = false;
  //if the client calls sync/flush method then the file will be stored on the
  //datanodes irrespective of the file size. The reason is that before the file
  //is close we are not sure about the final size of the file. If we store the
  // the data in the database and later on the file size exceeds the "dbFileMaxSize"
  // limit then we will have to transfer the data stored in the databse to the
  // datanodes. This will slow down the file creations and put unnecessary stress
  // on the NameNodes.
  private boolean syncOrFlushCalled = false;

  private static BlockStoragePolicySuite policySuite = BlockStoragePolicySuite.createDefaultSuite();

  private DataStreamer(HdfsFileStatus stat, DFSClient dfsClient, String src,
                       Progressable progress, DataChecksum checksum,
                       AtomicReference<CachingStrategy> cachingStrategy,
                       ByteArrayManager byteArrayManage, int dbFileMaxSize,
                       boolean forceClientToWriteSFToDisk,
                       boolean isAppend, String[] favoredNodes){
    this.dfsClient = dfsClient;
    this.src = src;
    this.progress = progress;
    this.stat = stat;
    this.cachingStrategy = cachingStrategy;
    this.byteArrayManager = byteArrayManage;
    this.dfsclientSlowLogThresholdMs =
        dfsClient.getConf().getSlowIoWarningThresholdMs();
    this.excludedNodes = initExcludedNodes();
    this.isAppend = isAppend;
    this.favoredNodes = favoredNodes;
    this.checksum = checksum;
    this.dbFileMaxSize = dbFileMaxSize;
    this.forceClientToWriteSFToDisk = forceClientToWriteSFToDisk;
    if(this.forceClientToWriteSFToDisk) {
      isThisFileStoredInDB = false;
    }else{
      isThisFileStoredInDB =
              policySuite.getPolicy(stat.getStoragePolicy()).getStorageTypes()[0] == StorageType.DB;
    }
  }

  /**
   * construction with tracing info
   */
  DataStreamer(HdfsFileStatus stat, ExtendedBlock block, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, int dbFileMaxSize,
               boolean forceClientToWriteSFToDisk, String[] favoredNodes) {
    this(stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, dbFileMaxSize, forceClientToWriteSFToDisk, false, favoredNodes);
    this.block = block;
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  /**
   * Construct a data streamer for appending to the last partial block
   * @param lastBlock last block of the file to be appended
   * @param stat status of the file to be appended
   * @throws IOException if error occurs
   */
  DataStreamer(LocatedBlock lastBlock, HdfsFileStatus stat, DFSClient dfsClient,
               String src, Progressable progress, DataChecksum checksum,
               AtomicReference<CachingStrategy> cachingStrategy,
               ByteArrayManager byteArrayManage, int dbFileMaxSize,
               boolean forceClientToWriteSFToDisk) throws IOException {
    this(stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage,dbFileMaxSize, forceClientToWriteSFToDisk, true, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_APPEND;
    block = lastBlock.getBlock();
    bytesSent = block.getNumBytes();
    accessToken = lastBlock.getBlockToken();
  }

  /**
   * Construct a data streamer for single block transfer
   */
  DataStreamer(HdfsFileStatus stat, LocatedBlock lb, boolean sigleBlock, DFSClient dfsClient,
      String src, Progressable progress, DataChecksum checksum,
      AtomicReference<CachingStrategy> cachingStrategy,
      ByteArrayManager byteArrayManage, int dbFileMaxSize,
      boolean saveSmallFilesInDB) {
    this(stat, dfsClient, src, progress, checksum, cachingStrategy,
        byteArrayManage, dbFileMaxSize, saveSmallFilesInDB, false, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_SINGLE_BLOCK;
    this.lb = lb;
  }
  
  /**
   * Set pipeline in construction
   *
   * @param lastBlock the last block of a file
   * @throws IOException
   */
  void setPipelineInConstruction(LocatedBlock lastBlock) throws IOException{
    // setup pipeline to append to the last block XXX retries??
    setPipeline(lastBlock);
    errorIndex = -1;   // no errors yet.
    if (nodes.length < 1) {
      throw new IOException("Unable to retrieve blocks locations " +
          " for last block " + block +
          "of file " + src);
    }
  }

  private void setPipeline(LocatedBlock lb) {
    setPipeline(lb.getLocations(), lb.getStorageTypes(), lb.getStorageIDs());
  }

  private void setPipeline(DatanodeInfo[] nodes, StorageType[] storageTypes,
                           String[] storageIDs) {
    this.nodes = nodes;
    this.storageTypes = storageTypes;
    this.storageIDs = storageIDs;
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
    if(LOG.isDebugEnabled()) {
      LOG.debug("Closing old block " + block);
    }
    this.setName("DataStreamer for file " + src);
    closeResponder();
    closeStream();
    setPipeline(null, null, null);
    stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
  }

  /*
   * streamer thread is the only thread that opens streams to datanode,
   * and closes them. Any error recovery is also done by this thread.
   */
  @Override
  public void run() {
    long lastPacket = Time.monotonicNow();
    TraceScope scope = null;
    while (!streamerClosed && dfsClient.clientRunning) {
      // if the Responder encountered an error, shutdown Responder
      if (hasError && response != null) {
        try {
          response.close();
          response.join();
          response = null;
        } catch (InterruptedException  e) {
          LOG.warn("Caught exception", e);
        }
      }

      DFSPacket one;
      try {
        // process datanode IO errors if any
        boolean doSleep = false;
        if (hasError && (errorIndex >= 0 || restartingNodeIndex.get() >= 0)) {
          doSleep = processDatanodeError();
        }

        final int halfSocketTimeout = dfsClient.getConf().getSocketTimeout()/2; 
        synchronized (dataQueue) {
          // wait for a packet to be sent.
          long now = Time.monotonicNow();
          while ((!streamerClosed && !hasError && dfsClient.clientRunning
              && dataQueue.size() == 0 &&
              (stage != BlockConstructionStage.DATA_STREAMING ||
                  stage == BlockConstructionStage.DATA_STREAMING &&
                      now - lastPacket < halfSocketTimeout)) || doSleep ) {
            long timeout = halfSocketTimeout - (now-lastPacket);
            timeout = timeout <= 0 ? 1000 : timeout;
            timeout = (stage == BlockConstructionStage.DATA_STREAMING)?
                timeout : 1000;
            try {
              dataQueue.wait(timeout);
            } catch (InterruptedException  e) {
              LOG.warn("Caught exception", e);
            }
            doSleep = false;
            now = Time.monotonicNow();
          }
          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }
          // get packet to be sent.
          if (dataQueue.isEmpty()) {
            one = createHeartbeatPacket();
            assert one != null;
          } else {
            try {
              backOffIfNecessary();
            } catch (InterruptedException e) {
              LOG.warn("Caught exception", e);
            }
            one = dataQueue.getFirst(); // regular data packet
            SpanId[] parents = one.getTraceParents();
            if (parents.length > 0) {
              scope = dfsClient.getTracer().
                    newScope("dataStreamer", parents[0]);
                scope.getSpan().setParents(parents);
            }
          }
        }

        // get new block from namenode.
        if (stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Allocating new block");
          }
          setPipeline(nextBlockOutputStream());
          initDataStreaming();
        } else if (stage == BlockConstructionStage.PIPELINE_SETUP_APPEND) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Append to block " + block);
          }
          setupPipelineForAppendOrRecovery();
          initDataStreaming();
        } else if (stage ==
                  BlockConstructionStage.PIPELINE_SETUP_SINGLE_BLOCK) {
            // TODO This is sent by protobuf and somehow a hack
            stage = BlockConstructionStage.PIPELINE_SETUP_CREATE;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Send single block " + block);
            }
            setPipeline(lb);
            nodes = setupPipelineForSingleBlock(lb);
            initDataStreaming();
          }

        long lastByteOffsetInBlock = one.getLastByteOffsetBlock();
        if (lastByteOffsetInBlock > stat.getBlockSize()) {
          throw new IOException("BlockSize " + stat.getBlockSize() +
              " is smaller than data size. " +
              " Offset of packet in block " +
              lastByteOffsetInBlock +
              " Aborting file " + src);
        }

        if (one.isLastPacketInBlock()) {
          // wait for all data packets have been successfully acked
          synchronized (dataQueue) {
            while (!streamerClosed && !hasError &&
                ackQueue.size() != 0 && dfsClient.clientRunning) {
              try {
                // wait for acks to arrive from datanodes
                dataQueue.wait(1000);
              } catch (InterruptedException  e) {
                LOG.warn("Caught exception", e);
              }
            }
          }
          if (streamerClosed || hasError || !dfsClient.clientRunning) {
            continue;
          }
          stage = BlockConstructionStage.PIPELINE_CLOSE;
        }

        // send the packet
        SpanId spanId = SpanId.INVALID;
        synchronized (dataQueue) {
          // move packet from dataQueue to ackQueue
          if (!one.isHeartbeatPacket()) {
            if (scope != null) {
              spanId = scope.getSpanId();
              scope.detach();
              one.setTraceScope(scope);
            }
            scope = null;
            dataQueue.removeFirst();
            ackQueue.addLast(one);
            dataQueue.notifyAll();
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("DataStreamer block " + block +
              " sending packet " + one);
        }

        // write out data to remote datanode
        try (TraceScope ignored = dfsClient.getTracer().
              newScope("DataStreamer#writeTo", spanId)) {
          one.writeTo(blockStream);
          blockStream.flush();
        } catch (IOException e) {
          // HDFS-3398 treat primary DN is down since client is unable to
          // write to primary DN. If a failed or restarting node has already
          // been recorded by the responder, the following call will have no
          // effect. Pipeline recovery can handle only one node error at a
          // time. If the primary node fails again during the recovery, it
          // will be taken out then.
          tryMarkPrimaryDatanodeFailed();
          throw e;
        }
        lastPacket = Time.monotonicNow();

        // update bytesSent
        long tmpBytesSent = one.getLastByteOffsetBlock();
        if (bytesSent < tmpBytesSent) {
          bytesSent = tmpBytesSent;
        }

        if (streamerClosed || hasError || !dfsClient.clientRunning) {
          continue;
        }

        // Is this block full?
        if (one.isLastPacketInBlock()) {
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
        if (progress != null) { progress.progress(); }

        // This is used by unit test to trigger race conditions.
        if (artificialSlowdown != 0 && dfsClient.clientRunning) {
          Thread.sleep(artificialSlowdown);
        }
      } catch (Throwable e) {
        // Log warning if there was a real error.
        if (restartingNodeIndex.get() == -1) {
          // Since their messages are descriptive enough, do not always
          // log a verbose stack-trace WARN for quota exceptions.
          if (e instanceof QuotaExceededException) {
            LOG.debug("DataStreamer Quota Exception", e);
          } else {
            LOG.warn("DataStreamer Exception", e);
          }
        }
        lastException.set(e);
        hasError = true;
        if (errorIndex == -1 && restartingNodeIndex.get() == -1) {
          // Not a datanode issue
          streamerClosed = true;
        }
      } finally {
        if (scope != null) {
          scope.close();
          scope = null;
        }
      }
    }
    closeInternal();
  }

  private void closeInternal() {
    closeResponder();       // close and join
    closeStream();
    streamerClosed = true;
    release();
    synchronized (dataQueue) {
      dataQueue.notifyAll();
    }
  }

  /**
   * release the DFSPackets in the two queues
   *
   */
  void release() {
    synchronized (dataQueue) {
      releaseBuffer(dataQueue, byteArrayManager);
      releaseBuffer(ackQueue, byteArrayManager);
    }
  }

  /**
   * wait for the ack of seqno
   *
   * @param seqno the sequence number to be acked
   * @throws IOException
   */
  void waitForAckedSeqno(long seqno) throws IOException {
    TraceScope scope = dfsClient.getTracer().
        newScope("waitForAckedSeqno");
    try {
      if (canStoreFileInDB()) {
        LOG.debug("Stuffed Inode:  Closing File. Datanode ack skipped. All the data will be stored in the database");
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for ack for: " + seqno);
        }
        long begin = Time.monotonicNow();
        try {
          synchronized (dataQueue) {
            while (!streamerClosed) {
              checkClosed();
              if (lastAckedSeqno >= seqno) {
                break;
              }
              try {
                dataQueue.wait(1000); // when we receive an ack, we notify on
                // dataQueue
              } catch (InterruptedException ie) {
                throw new InterruptedIOException(
                    "Interrupted while waiting for data to be acknowledged by pipeline");
              }
            }
          }
          checkClosed();
        } catch (ClosedChannelException e) {
        }
        long duration = Time.monotonicNow() - begin;
        if (duration > dfsclientSlowLogThresholdMs) {
          LOG.warn("Slow waitForAckedSeqno took " + duration
              + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms)");
        }
      }
    } finally {
      scope.close();
    }
  }

  /**
   * wait for space of dataQueue and queue the packet
   *
   * @param packet  the DFSPacket to be queued
   * @throws IOException
   */
  void waitAndQueuePacket(DFSPacket packet) throws IOException {
    synchronized (dataQueue) {
      try {
        // If queue is full, then wait till we have enough space
        boolean firstWait = true;
        try {
          while (!streamerClosed && dataQueue.size() + ackQueue.size() >
              dfsClient.getConf().getWriteMaxPackets()) {
            if (firstWait) {
              Span span = Tracer.getCurrentSpan();
              if (span != null) {
                span.addTimelineAnnotation("dataQueue.wait");
              }
              firstWait = false;
            }
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
              // If we get interrupted while waiting to queue data, we still need to get rid
              // of the current packet. This is because we have an invariant that if
              // currentPacket gets full, it will get queued before the next writeChunk.
              //
              // Rather than wait around for space in the queue, we should instead try to
              // return to the caller as soon as possible, even though we slightly overrun
              // the MAX_PACKETS length.
              Thread.currentThread().interrupt();
              break;
            }
          }
        } finally {
          Span span = Tracer.getCurrentSpan();
          if ((span != null) && (!firstWait)) {
            span.addTimelineAnnotation("end.wait");
          }
        }
        checkClosed();
        queuePacket(packet);
      } catch (ClosedChannelException e) {
      }
    }
  }

  /*
   * close the streamer, should be called only by an external thread
   * and only after all data to be sent has been flushed to datanode.
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


  private void checkClosed() throws IOException {
    if (streamerClosed) {
      lastException.throwException4Close();
    }
  }

  private void closeResponder() {
    if (response != null) {
      try {
        response.close();
        response.join();
      } catch (InterruptedException  e) {
        LOG.warn("Caught exception", e);
      } finally {
        response = null;
      }
    }
  }

  private void closeStream() {
    final MultipleIOException.Builder b = new MultipleIOException.Builder();

    if (blockStream != null) {
      try {
        blockStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockStream = null;
      }
    }
    if (blockReplyStream != null) {
      try {
        blockReplyStream.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        blockReplyStream = null;
      }
    }
    if (null != s) {
      try {
        s.close();
      } catch (IOException e) {
        b.add(e);
      } finally {
        s = null;
      }
    }

    final IOException ioe = b.build();
    if (ioe != null) {
      lastException.set(ioe);
    }
  }

  // The following synchronized methods are used whenever
  // errorIndex or restartingNodeIndex is set. This is because
  // check & set needs to be atomic. Simply reading variables
  // does not require a synchronization. When responder is
  // not running (e.g. during pipeline recovery), there is no
  // need to use these methods.

  /** Set the error node index. Called by responder */
  synchronized void setErrorIndex(int idx) {
    errorIndex = idx;
  }

  /** Set the restarting node index. Called by responder */
  synchronized void setRestartingNodeIndex(int idx) {
    restartingNodeIndex.set(idx);
    // If the data streamer has already set the primary node
    // bad, clear it. It is likely that the write failed due to
    // the DN shutdown. Even if it was a real failure, the pipeline
    // recovery will take care of it.
    errorIndex = -1;
  }

  /**
   * This method is used when no explicit error report was received,
   * but something failed. When the primary node is a suspect or
   * unsure about the cause, the primary node is marked as failed.
   */
  synchronized void tryMarkPrimaryDatanodeFailed() {
    // There should be no existing error and no ongoing restart.
    if ((errorIndex == -1) && (restartingNodeIndex.get() == -1)) {
      errorIndex = 0;
    }
  }

  /**
   * Examine whether it is worth waiting for a node to restart.
   * @param index the node index
   */
  boolean shouldWaitForRestart(int index) {
    // Only one node in the pipeline.
    if (nodes.length == 1) {
      return true;
    }

    // Is it a local node?
    InetAddress addr = null;
    try {
      addr = InetAddress.getByName(nodes[index].getIpAddr());
    } catch (java.net.UnknownHostException e) {
      // we are passing an ip address. this should not happen.
      assert false;
    }

    if (addr != null && NetUtils.isLocalAddress(addr)) {
      return true;
    }
    return false;
  }

  //
  // Processes responses from the datanodes.  A packet is removed
  // from the ackQueue when its response arrives.
  //
  private class ResponseProcessor extends Daemon {

    private volatile boolean responderClosed = false;
    private DatanodeInfo[] targets = null;
    private boolean isLastPacketInBlock = false;

    ResponseProcessor (DatanodeInfo[] targets) {
      this.targets = targets;
    }

    @Override
    public void run() {

      setName("ResponseProcessor for block " + block);
      PipelineAck ack = new PipelineAck();

      TraceScope scope = null;
      while (!responderClosed && dfsClient.clientRunning && !isLastPacketInBlock) {
        // process responses from datanodes.
        try {
          // read an ack from the pipeline
          long begin = Time.monotonicNow();
          ack.readFields(blockReplyStream);
          long duration = Time.monotonicNow() - begin;
          if (duration > dfsclientSlowLogThresholdMs
              && ack.getSeqno() != DFSPacket.HEART_BEAT_SEQNO) {
            LOG.warn("Slow ReadProcessor read fields took " + duration
                + "ms (threshold=" + dfsclientSlowLogThresholdMs + "ms); ack: "
                + ack + ", targets: " + Arrays.asList(targets));
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient " + ack);
          }

          long seqno = ack.getSeqno();
          // processes response status from datanodes.
          ArrayList<DatanodeInfo> congestedNodesFromAck = new ArrayList<>();
          for (int i = ack.getNumOfReplies()-1; i >=0  && dfsClient.clientRunning; i--) {
            final Status reply = PipelineAck.getStatusFromHeader(ack
                .getHeaderFlag(i));
            if (PipelineAck.getECNFromHeader(ack.getHeaderFlag(i)) ==
                PipelineAck.ECN.CONGESTED) {
              congestedNodesFromAck.add(targets[i]);
            }
            // Restart will not be treated differently unless it is
            // the local node or the only one in the pipeline.
            if (PipelineAck.isRestartOOBStatus(reply) &&
                shouldWaitForRestart(i)) {
              restartDeadline = dfsClient.getConf().getDatanodeRestartTimeout()
                  + Time.monotonicNow();
              setRestartingNodeIndex(i);
              String message = "A datanode is restarting: " + targets[i];
              LOG.info(message);
              throw new IOException(message);
            }
            // node error
            if (reply != SUCCESS) {
              setErrorIndex(i); // first bad datanode
              throw new IOException("Bad response " + reply +
                  " for block " + block +
                  " from datanode " +
                  targets[i]);
            }
          }

          if (!congestedNodesFromAck.isEmpty()) {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              congestedNodes.addAll(congestedNodesFromAck);
            }
          } else {
            synchronized (congestedNodes) {
              congestedNodes.clear();
              lastCongestionBackoffTime = 0;
            }
          }

          assert seqno != PipelineAck.UNKOWN_SEQNO :
              "Ack for unknown seqno should be a failed ack: " + ack;
          if (seqno == DFSPacket.HEART_BEAT_SEQNO) {  // a heartbeat ack
            continue;
          }

          // a success ack for a data packet
          DFSPacket one;
          synchronized (dataQueue) {
            one = ackQueue.getFirst();
          }
          if (one.getSeqno() != seqno) {
            throw new IOException("ResponseProcessor: Expecting seqno " +
                " for block " + block +
                one.getSeqno() + " but received " + seqno);
          }
          isLastPacketInBlock = one.isLastPacketInBlock();

          // Fail the packet write for testing in order to force a
          // pipeline recovery.
          if (DFSClientFaultInjector.get().failPacket() &&
              isLastPacketInBlock) {
            failPacket = true;
            throw new IOException(
                "Failing the last packet for testing.");
          }

          // update bytesAcked
          block.setNumBytes(one.getLastByteOffsetBlock());

          synchronized (dataQueue) {
            scope = one.getTraceScope();
            if (scope != null) {
              scope.reattach();
              one.setTraceScope(null);
            }
            lastAckedSeqno = seqno;
            ackQueue.removeFirst();
            dataQueue.notifyAll();

            one.releaseBuffer(byteArrayManager);
          }
        } catch (Exception e) {
          if (!responderClosed) {
            lastException.set(e);
            hasError = true;
            // If no explicit error report was received, mark the primary
            // node as failed.
            tryMarkPrimaryDatanodeFailed();
            synchronized (dataQueue) {
              dataQueue.notifyAll();
            }
            if (restartingNodeIndex.get() == -1) {
              LOG.warn("Exception for " + block, e);
            }
            responderClosed = true;
          }
        } finally {
          if (scope != null) {
            scope.close();
          }
          scope = null;
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
      LOG.info("Error Recovery for " + block +
          " waiting for responder to exit. ");
      return true;
    }
    closeStream();

    // move packets from ack queue to front of the data queue
    synchronized (dataQueue) {
      dataQueue.addAll(0, ackQueue);
      ackQueue.clear();
    }

    // Record the new pipeline failure recovery.
    if (lastAckedSeqnoBeforeFailure != lastAckedSeqno) {
      lastAckedSeqnoBeforeFailure = lastAckedSeqno;
      pipelineRecoveryCount = 1;
    } else {
      // If we had to recover the pipeline five times in a row for the
      // same packet, this client likely has corrupt data or corrupting
      // during transmission.
      if (++pipelineRecoveryCount > 5) {
        LOG.warn("Error recovering pipeline for writing " +
            block + ". Already retried 5 times for the same packet.");
        lastException.set(new IOException("Failing write. Tried pipeline " +
            "recovery 5 times without success."));
        streamerClosed = true;
        return false;
      }
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
          DFSPacket endOfBlockPacket = dataQueue.remove();  // remove the end of block packet
          TraceScope scope = endOfBlockPacket.getTraceScope();
          if (scope != null) {
            scope.reattach();
            scope.close();
            endOfBlockPacket.setTraceScope(null);
          }
          assert endOfBlockPacket.isLastPacketInBlock();
          assert lastAckedSeqno == endOfBlockPacket.getSeqno() - 1;
          lastAckedSeqno = endOfBlockPacket.getSeqno();
          dataQueue.notifyAll();
        }
        endBlock();
      } else {
        initDataStreaming();
      }
    }

    return doSleep;
  }

  void setHflush() {
    isHflushed = true;
  }

  private int findNewDatanode(final DatanodeInfo[] original
  ) throws IOException {
    if (nodes.length != original.length + 1) {
      throw new IOException(
          new StringBuilder()
              .append("Failed to replace a bad datanode on the existing pipeline ")
              .append("due to no more good datanodes being available to try. ")
              .append("(Nodes: current=").append(Arrays.asList(nodes))
              .append(", original=").append(Arrays.asList(original)).append("). ")
              .append("The current failed datanode replacement policy is ")
              .append(dfsClient.dtpReplaceDatanodeOnFailure).append(", and ")
              .append("a client may configure this via '")
              .append(HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.POLICY_KEY)
              .append("' in its configuration.")
              .toString());
    }
    for(int i = 0; i < nodes.length; i++) {
      int j = 0;
      for(; j < original.length && !nodes[i].equals(original[j]); j++);
      if (j == original.length) {
        return i;
      }
    }
    throw new IOException("Failed: new datanode not found: nodes="
        + Arrays.asList(nodes) + ", original=" + Arrays.asList(original));
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
    if (!isAppend && lastAckedSeqno < 0
        && stage == BlockConstructionStage.PIPELINE_SETUP_CREATE) {
      //no data have been written
      return;
    } else if (stage == BlockConstructionStage.PIPELINE_CLOSE
        || stage == BlockConstructionStage.PIPELINE_CLOSE_RECOVERY) {
      //pipeline is closing
      return;
    }

    //get a new datanode
    final DatanodeInfo[] original = nodes;
    final LocatedBlock lb = dfsClient.namenode.getAdditionalDatanode(
        src, stat.getFileId(), block, nodes, storageIDs,
        failed.toArray(new DatanodeInfo[failed.size()]),
        1, dfsClient.clientName);
    setPipeline(lb);

    //find the new datanode
    final int d = findNewDatanode(original);

    //transfer replica
    final DatanodeInfo src = d == 0? nodes[1]: nodes[d - 1];
    final DatanodeInfo[] targets = {nodes[d]};
    final StorageType[] targetStorageTypes = {storageTypes[d]};
    transfer(src, targets, targetStorageTypes, lb.getBlockToken());
  }

  private void transfer(final DatanodeInfo src, final DatanodeInfo[] targets,
                        final StorageType[] targetStorageTypes,
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
      IOStreamPair saslStreams = dfsClient.saslClient.socketSend(sock,
          unbufOut, unbufIn, dfsClient, blockToken, src);
      unbufOut = saslStreams.out;
      unbufIn = saslStreams.in;
      out = new DataOutputStream(new BufferedOutputStream(unbufOut,
          HdfsConstants.SMALL_BUFFER_SIZE));
      in = new DataInputStream(unbufIn);

      //send the TRANSFER_BLOCK request
      new Sender(out).transferBlock(block, blockToken, dfsClient.clientName,
          targets, targetStorageTypes);
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
   * Open a DataStreamer to a DataNode pipeline so that
   * it can be written to.
   * This happens when a file is appended or data streaming fails
   * It keeps on trying until a pipeline is setup
   */
  private boolean setupPipelineForAppendOrRecovery() throws IOException {
    // check number of datanodes
    if (nodes == null || nodes.length == 0) {
      String msg = "Could not get block locations. " + "Source file \""
          + src + "\" - Aborting...";
      LOG.warn(msg);
      lastException.set(new IOException(msg));
      streamerClosed = true;
      return false;
    }

    boolean success = false;
    long newGS = 0L;
    while (!success && !streamerClosed && dfsClient.clientRunning) {
      // Sleep before reconnect if a dn is restarting.
      // This process will be repeated until the deadline or the datanode
      // starts back up.
      if (restartingNodeIndex.get() >= 0) {
        // 4 seconds or the configured deadline period, whichever is shorter.
        // This is the retry interval and recovery will be retried in this
        // interval until timeout or success.
        long delay = Math.min(dfsClient.getConf().getDatanodeRestartTimeout(),
            4000L);
        try {
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          lastException.set(new IOException("Interrupted while waiting for " +
              "datanode to restart. " + nodes[restartingNodeIndex.get()]));
          streamerClosed = true;
          return false;
        }
      }
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
          lastException.set(new IOException("All datanodes " + pipelineMsg
              + " are bad. Aborting..."));
          streamerClosed = true;
          return false;
        }
        LOG.warn("Error Recovery for block " + block +
            " in pipeline " + pipelineMsg +
            ": bad datanode " + nodes[errorIndex]);
        failed.add(nodes[errorIndex]);

        DatanodeInfo[] newnodes = new DatanodeInfo[nodes.length-1];
        arraycopy(nodes, newnodes, errorIndex);

        final StorageType[] newStorageTypes = new StorageType[newnodes.length];
        arraycopy(storageTypes, newStorageTypes, errorIndex);

        final String[] newStorageIDs = new String[newnodes.length];
        arraycopy(storageIDs, newStorageIDs, errorIndex);

        setPipeline(newnodes, newStorageTypes, newStorageIDs);

        // Just took care of a node error while waiting for a node restart
        if (restartingNodeIndex.get() >= 0) {
          // If the error came from a node further away than the restarting
          // node, the restart must have been complete.
          if (errorIndex > restartingNodeIndex.get()) {
            restartingNodeIndex.set(-1);
          } else if (errorIndex < restartingNodeIndex.get()) {
            // the node index has shifted.
            restartingNodeIndex.decrementAndGet();
          } else {
            // this shouldn't happen...
            assert false;
          }
        }

        if (restartingNodeIndex.get() == -1) {
          hasError = false;
        }
        lastException.clear();
        errorIndex = -1;
      }

      // Check if replace-datanode policy is satisfied.
      if (dfsClient.dtpReplaceDatanodeOnFailure.satisfy(stat.getReplication(),
          nodes, isAppend, isHflushed)) {
        try {
          addDatanode2ExistingPipeline();
        } catch(IOException ioe) {
          if (!dfsClient.dtpReplaceDatanodeOnFailure.isBestEffort()) {
            throw ioe;
          }
          LOG.warn("Failed to replace datanode."
              + " Continue with the remaining datanodes since "
              + HdfsClientConfigKeys.BlockWrite.ReplaceDatanodeOnFailure.BEST_EFFORT_KEY
              + " is set to true.", ioe);
        }
      }

      // get a new generation stamp and an access token
      LocatedBlock lb = dfsClient.namenode.updateBlockForPipeline(block, dfsClient.clientName);
      newGS = lb.getBlock().getGenerationStamp();
      accessToken = lb.getBlockToken();

      // set up the pipeline again with the remaining nodes
      if (failPacket) { // for testing
        success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
        failPacket = false;
        try {
          // Give DNs time to send in bad reports. In real situations,
          // good reports should follow bad ones, if client committed
          // with those nodes.
          Thread.sleep(2000);
        } catch (InterruptedException ie) {}
      } else {
        success = createBlockOutputStream(nodes, storageTypes, newGS, isRecovery);
      }

      if (restartingNodeIndex.get() >= 0) {
        assert hasError == true;
        // check errorIndex set above
        if (errorIndex == restartingNodeIndex.get()) {
          // ignore, if came from the restarting node
          errorIndex = -1;
        }
        // still within the deadline
        if (Time.monotonicNow() < restartDeadline) {
          continue; // with in the deadline
        }
        // expired. declare the restarting node dead
        restartDeadline = 0;
        int expiredNodeIndex = restartingNodeIndex.get();
        restartingNodeIndex.set(-1);
        LOG.warn("Datanode did not restart in time: " +
            nodes[expiredNodeIndex]);
        // Mark the restarting node as failed. If there is any other failed
        // node during the last pipeline construction attempt, it will not be
        // overwritten/dropped. In this case, the restarting node will get
        // excluded in the following attempt, if it still does not come up.
        if (errorIndex == -1) {
          errorIndex = expiredNodeIndex;
        }
        // From this point on, normal pipeline recovery applies.
      }
    } // while

    if (success) {
      // update pipeline at the namenode
      ExtendedBlock newBlock = new ExtendedBlock(
          block.getBlockPoolId(), block.getBlockId(), block.getNumBytes(), newGS);
      dfsClient.namenode.updatePipeline(dfsClient.clientName, block, newBlock,
          nodes, storageIDs);
      // update client side generation stamp
      block = newBlock;
    }
    return false; // do not sleep, continue processing
  }

  /**
   * Open a DataStreamer to a DataNode so that it can be written to.
   * This happens when a file is created and each time a new block is allocated.
   * Must get block ID and the IDs of the destinations from the namenode.
   * Returns the list of target datanodes.
   */
  private LocatedBlock nextBlockOutputStream() throws IOException {
    LocatedBlock lb = null;
    DatanodeInfo[] nodes = null;
    StorageType[] storageTypes = null;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success = false;
    ExtendedBlock oldBlock = block;
    do {
      hasError = false;
      lastException.clear();
      errorIndex = -1;
      success = false;

      DatanodeInfo[] excluded;
      long startTime = Time.now();

      if ((erasureCodingSourceStream && currentBlockIndex % stripeLength == 0)) {
        usedNodes.clear();
        LOG.info("Stripe length " + stripeLength + " parity length " + parityLength);
        LOG.info("Source write block index " + currentBlockIndex);
      }

      if (erasureCodingParityStream && currentBlockIndex % parityLength == 0) {
        usedNodes.clear();
        stripeNodes.clear();
        int stripe = (int) Math.ceil(currentBlockIndex / (float) parityLength);
        int index = stripe * stripeLength;
        LOG.info("Stripe length " + stripeLength + " parity length " + parityLength);
        LOG.info("Parity write block index " + currentBlockIndex + " found index " + index + " end " + (index
            + stripeLength));
        for (int j = index;
            j < sourceBlocks.size() && j < index + stripeLength; j++) {
          DatanodeInfo[] nodeInfos = sourceBlocks.get(j).getLocations();
          Collections.addAll(stripeNodes, nodeInfos);
        }
      }

      if (erasureCodingSourceStream || erasureCodingParityStream) {
        ImmutableSet<DatanodeInfo> excludedSet = excludedNodes.getAllPresent(excludedNodes.asMap().keySet()).keySet();

        excluded = new DatanodeInfo[excludedSet.size() + usedNodes.
            size() + stripeNodes.size() + parityStripeNodes.size()];
        int i = 0;
        for (DatanodeInfo node : excludedSet) {
          excluded[i] = node;
          LOG.info("Excluding node " + node);
          i++;
        }
        for (DatanodeInfo node : usedNodes) {
          excluded[i] = node;
          LOG.info((erasureCodingSourceStream ? "Source stream: " : " Parity stream: ")
              + "Block " + currentBlockIndex + " excluding used node "
              + node);
          i++;
        }
        for (DatanodeInfo node : stripeNodes) {
          excluded[i] = node;
          LOG.info((erasureCodingSourceStream ? "Source stream: " : " Parity stream: ")
              + "Block " + currentBlockIndex + " excluding stripe node "
              + node);
          i++;
        }
        for (DatanodeInfo node : parityStripeNodes) {
          excluded[i] = node;
          LOG.info((erasureCodingSourceStream ? "Source stream: " : " Parity stream: ")
              + "Block " + currentBlockIndex + " excluding parity node "
              + node);
          i++;
        }
        currentBlockIndex++;
      } else {
        excluded = excludedNodes.getAllPresent(excludedNodes.asMap().keySet())
            .keySet()
            .toArray(new DatanodeInfo[0]);
      }
      block = oldBlock;
      lb = locateFollowingBlock(excluded.length > 0 ? excluded : null);
      block = lb.getBlock();
      block.setNumBytes(0);
      bytesSent = 0;
      accessToken = lb.getBlockToken();
      nodes = lb.getLocations();
      storageTypes = lb.getStorageTypes();

      //
      // Connect to first DataNode in the list.
      //
      success = createBlockOutputStream(nodes, storageTypes, 0L, false);

      if (!success) {
        LOG.info("Abandoning " + block);
        dfsClient.namenode.abandonBlock(block, stat.getFileId(), src,
            dfsClient.clientName);
        block = null;
        LOG.info("Excluding datanode " + nodes[errorIndex]);
        excludedNodes.put(nodes[errorIndex], nodes[errorIndex]);
      }
    } while (!success && --count >= 0);

    if (!success) {
      throw new IOException("Unable to create new block.");
    }
    
    if (erasureCodingSourceStream || erasureCodingParityStream) {
        Collections.addAll(usedNodes, nodes);
    }
    
    return lb;
  }

  // connects to the first datanode in the pipeline
  // Returns true if success, otherwise return failure.
  //
  private boolean createBlockOutputStream(DatanodeInfo[] nodes,
      StorageType[] nodeStorageTypes, long newGS, boolean recoveryFlag) {
    if (nodes.length == 0) {
      LOG.info("nodes are empty for write pipeline of " + block);
      return false;
    }
    Status pipelineStatus = SUCCESS;
    String firstBadLink = "";
    boolean checkRestart = false;
    if (LOG.isDebugEnabled()) {
      LOG.debug("pipeline = " + Arrays.asList(nodes));
    }

    // persist blocks on namenode on next flush
    persistBlocks.set(true);

    int refetchEncryptionKey = 1;
    while (true) {
      boolean result = false;
      DataOutputStream out = null;
      try {
        assert null == s : "Previous socket unclosed";
        assert null == blockReplyStream : "Previous blockReplyStream unclosed";
        s = createSocketForPipeline(nodes[0], nodes.length, dfsClient);
        long writeTimeout = dfsClient.getDatanodeWriteTimeout(nodes.length);

        OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
        InputStream unbufIn = NetUtils.getInputStream(s);
        IOStreamPair saslStreams = dfsClient.saslClient.socketSend(s,
            unbufOut, unbufIn, dfsClient, accessToken, nodes[0]);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(unbufIn);

        //
        // Xmit header info to datanode
        //

        BlockConstructionStage bcs = recoveryFlag? stage.getRecoveryStage(): stage;

        // We cannot change the block length in 'block' as it counts the number
        // of bytes ack'ed.
        ExtendedBlock blockCopy = new ExtendedBlock(block);
        blockCopy.setNumBytes(stat.getBlockSize());

        boolean[] targetPinnings = getPinnings(nodes, true);
        // send the request
        new Sender(out).writeBlock(blockCopy, nodeStorageTypes[0], accessToken,
            dfsClient.clientName, nodes, nodeStorageTypes, null, bcs,
            nodes.length, block.getNumBytes(), bytesSent, newGS,
            checksum, cachingStrategy.get(),
            (targetPinnings == null ? false : targetPinnings[0]), targetPinnings);

        // receive ack for connect
        BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
            PBHelper.vintPrefixed(blockReplyStream));
        pipelineStatus = resp.getStatus();
        firstBadLink = resp.getFirstBadLink();

        // Got an restart OOB ack.
        // If a node is already restarting, this status is not likely from
        // the same node. If it is from a different node, it is not
        // from the local datanode. Thus it is safe to treat this as a
        // regular node error.
        if (PipelineAck.isRestartOOBStatus(pipelineStatus) &&
            restartingNodeIndex.get() == -1) {
          checkRestart = true;
          throw new IOException("A datanode is restarting.");
        }
		
        String logInfo = "ack with firstBadLink as " + firstBadLink;
        DataTransferProtoUtil.checkBlockOpStatus(resp, logInfo);

        assert null == blockStream : "Previous blockStream unclosed";
        blockStream = out;
        result =  true; // success
        restartingNodeIndex.set(-1);
        hasError = false;
      } catch (IOException ie) {
        if (restartingNodeIndex.get() == -1) {
          LOG.info("Exception in createBlockOutputStream", ie);
        }
        if (ie instanceof InvalidEncryptionKeyException && refetchEncryptionKey > 0) {
          LOG.info("Will fetch a new encryption key and retry, "
              + "encryption key was invalid when connecting to "
              + nodes[0] + " : " + ie);
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
          assert checkRestart == false;
          errorIndex = 0;
        }
        // Check whether there is a restart worth waiting for.
        if (checkRestart && shouldWaitForRestart(errorIndex)) {
          restartDeadline = dfsClient.getConf().getDatanodeRestartTimeout()
              + Time.monotonicNow();
          restartingNodeIndex.set(errorIndex);
          errorIndex = -1;
          LOG.info("Waiting for the datanode to be restarted: " +
              nodes[restartingNodeIndex.get()]);
        }
        hasError = true;
        lastException.set(ie);
        result =  false;  // error
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

  private boolean[] getPinnings(DatanodeInfo[] nodes, boolean shouldLog) {
    if (favoredNodes == null) {
      return null;
    } else {
      boolean[] pinnings = new boolean[nodes.length];
      HashSet<String> favoredSet =
          new HashSet<String>(Arrays.asList(favoredNodes));
      for (int i = 0; i < nodes.length; i++) {
        pinnings[i] = favoredSet.remove(nodes[i].getXferAddrWithHostname());
        if (LOG.isDebugEnabled()) {
          LOG.debug(nodes[i].getXferAddrWithHostname() +
              " was chosen by name node (favored=" + pinnings[i] + ").");
        }
      }
      if (shouldLog && !favoredSet.isEmpty()) {
        // There is one or more favored nodes that were not allocated.
        LOG.warn("These favored nodes were specified but not chosen: "
            + favoredSet + " Specified favored nodes: "
            + Arrays.toString(favoredNodes));

      }
      return pinnings;
    }
  }

  protected LocatedBlock locateFollowingBlock(DatanodeInfo[] excludedNodes)
      throws IOException {
    final DfsClientConf conf = dfsClient.getConf(); 
    int retries = conf.getNumBlockWriteLocateFollowingRetry();
    long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
    while (true) {
      long localstart = Time.monotonicNow();
      while (true) {
        try {
          return dfsClient.namenode.addBlock(src, dfsClient.clientName,
              block, excludedNodes, stat.getFileId(), favoredNodes);
        } catch (RemoteException e) {
          IOException ue =
              e.unwrapRemoteException(FileNotFoundException.class,
                  AccessControlException.class,
                  NSQuotaExceededException.class,
                  DSQuotaExceededException.class,
                  QuotaByStorageTypeExceededException.class,
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
              LOG.info("Exception while adding a block", e);
              long elapsed = Time.monotonicNow() - localstart;
              if (elapsed > 5000) {
                LOG.info("Waiting for replication for "
                    + (elapsed / 1000) + " seconds");
              }
              try {
                LOG.warn("NotReplicatedYetException sleeping " + src
                    + " retries left " + retries);
                Thread.sleep(sleeptime);
                sleeptime *= 2;
              } catch (InterruptedException ie) {
                LOG.warn("Caught exception", ie);
              }
            }
          } else {
            throw e;
          }

        }
      }
    }
  }

  /**
   * This function sleeps for a certain amount of time when the writing
   * pipeline is congested. The function calculates the time based on a
   * decorrelated filter.
   *
   * @see
   * <a href="http://www.awsarchitectureblog.com/2015/03/backoff.html">
   *   http://www.awsarchitectureblog.com/2015/03/backoff.html</a>
   */
  private void backOffIfNecessary() throws InterruptedException {
    int t = 0;
    synchronized (congestedNodes) {
      if (!congestedNodes.isEmpty()) {
        StringBuilder sb = new StringBuilder("DataNode");
        for (DatanodeInfo i : congestedNodes) {
          sb.append(' ').append(i);
        }
        int range = Math.abs(lastCongestionBackoffTime * 3 -
                                CONGESTION_BACKOFF_MEAN_TIME_IN_MS);
        int base = Math.min(lastCongestionBackoffTime * 3,
                            CONGESTION_BACKOFF_MEAN_TIME_IN_MS);
        t = Math.min(CONGESTION_BACK_OFF_MAX_TIME_IN_MS,
                     (int)(base + Math.random() * range));
        lastCongestionBackoffTime = t;
        sb.append(" are congested. Backing off for ").append(t).append(" ms");
        LOG.info(sb.toString());
        congestedNodes.clear();
      }
    }
    if (t != 0) {
      Thread.sleep(t);
    }
  }

  /**
   * get the block this streamer is writing to
   *
   * @return the block this streamer is writing to
   */
  ExtendedBlock getBlock() {
    return block;
  }

  /**
   * return the target datanodes in the pipeline
   *
   * @return the target datanodes in the pipeline
   */
  DatanodeInfo[] getNodes() {
    return nodes;
  }

  /**
   * return the token of the block
   *
   * @return the token of the block
   */
  Token<BlockTokenIdentifier> getBlockToken() {
    return accessToken;
  }

  /**
   * Put a packet to the data queue
   *
   * @param packet the packet to be put into the data queued
   */
  void queuePacket(DFSPacket packet) {
    synchronized (dataQueue) {
      if (packet == null) return;
      packet.addTraceParent(Tracer.getCurrentSpanId());
      // put it is the small files buffer
      if (canStoreFileInDB() && (packet.getLastByteOffsetBlock() <= dbFileMaxSize)) {
        LOG.debug("Stuffed Inode:  Temporarily withholding the packet in a buffer for small files");
        smallFileDataQueue.addLast(packet);
      } else {
        //Some condition for storing the data in the database has failed. Store the data on the datanodes
        forwardSmallFilesPacketsToDataNodes();
        dataQueue.addLast(packet);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Queued packet " + packet.getSeqno());
        }
      }
      lastQueuedSeqno = packet.getSeqno();
      dataQueue.notifyAll();
    }
  }

  /**
   * For heartbeat packets, create buffer directly by new byte[]
   * since heartbeats should not be blocked.
   */
  private DFSPacket createHeartbeatPacket() throws InterruptedIOException {
    final byte[] buf = new byte[PacketHeader.PKT_MAX_HEADER_LEN];
    return new DFSPacket(buf, 0, 0, DFSPacket.HEART_BEAT_SEQNO, 0, false);
  }

  private LoadingCache<DatanodeInfo, DatanodeInfo> initExcludedNodes() {
    return CacheBuilder.newBuilder().expireAfterWrite(
        dfsClient.getConf().getExcludedNodesCacheExpiry(),
        TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener<DatanodeInfo, DatanodeInfo>() {
          @Override
          public void onRemoval(
              RemovalNotification<DatanodeInfo, DatanodeInfo> notification) {
            LOG.info("Removing node " + notification.getKey()
                + " from the excluded nodes list");
          }
        }).build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
          @Override
          public DatanodeInfo load(DatanodeInfo key) throws Exception {
            return key;
          }
        });
  }

  private static <T> void arraycopy(T[] srcs, T[] dsts, int skipIndex) {
    System.arraycopy(srcs, 0, dsts, 0, skipIndex);
    System.arraycopy(srcs, skipIndex+1, dsts, skipIndex, dsts.length-skipIndex);
  }

  /**
   * check if to persist blocks on namenode
   *
   * @return if to persist blocks on namenode
   */
  AtomicBoolean getPersistBlocks(){
    return persistBlocks;
  }

  /**
   * check if to append a chunk
   *
   * @param appendChunk if to append a chunk
   */
  void setAppendChunk(boolean appendChunk){
    this.appendChunk = appendChunk;
  }

  /**
   * get if to append a chunk
   *
   * @return if to append a chunk
   */
  boolean getAppendChunk(){
    return appendChunk;
  }

  /**
   * @return the last exception
   */
  LastExceptionInStreamer getLastException(){
    return lastException;
  }

  /**
   * set socket to null
   */
  void setSocketToNull() {
    this.s = null;
  }

  /**
   * return current sequence number and then increase it by 1
   *
   * @return current sequence number before increasing
   */
  long getAndIncCurrentSeqno() {
    long old = this.currentSeqno;
    this.currentSeqno++;
    return old;
  }

  /**
   * get last queued sequence number
   *
   * @return last queued sequence number
   */
  long getLastQueuedSeqno() {
    return lastQueuedSeqno;
  }

  /**
   * get the number of bytes of current block
   *
   * @return the number of bytes of current block
   */
  long getBytesCurBlock() {
    return bytesCurBlock;
  }

  /**
   * set the bytes of current block that have been written
   *
   * @param bytesCurBlock bytes of current block that have been written
   */
  void setBytesCurBlock(long bytesCurBlock) {
    this.bytesCurBlock = bytesCurBlock;
  }

  /**
   * increase bytes of current block by len.
   *
   * @param len how many bytes to increase to current block
   */
  void incBytesCurBlock(long len) {
    this.bytesCurBlock += len;
  }

  /**
   * set artificial slow down for unit test
   *
   * @param period artificial slow down
   */
  void setArtificialSlowdown(long period) {
    this.artificialSlowdown = period;
  }

  /**
   * if this streamer is to terminate
   *
   * @return if this streamer is to terminate
   */
  boolean streamerClosed(){
    return streamerClosed;
  }

  void closeSocket() throws IOException {
    if (s != null) {
      s.close();
    }
  }
  
  private DatanodeInfo[] setupPipelineForSingleBlock(LocatedBlock lb)
      throws IOException {
    DatanodeInfo[] nodes;
    int count = dfsClient.getConf().getNumBlockWriteRetry();
    boolean success;
    do {
      hasError = false;
      lastException.clear();
      errorIndex = -1;

      block = lb.getBlock();
      block.setNumBytes(0);
      bytesSent = 0;
      accessToken = lb.getBlockToken();
      nodes = lb.getLocations();

      //
      // Connect to first DataNode in the list.
      //
      success = createBlockOutputStream(nodes, storageTypes, 0L, false);

      if (!success) {
        // TODO Request another location from the NameNode
      }
    } while (!success && --count >= 0);

    if (!success) {
      throw new IOException("Unable to initiate single block send.");
    }
    return nodes;
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
  
  public void setParityStripeNodesForNextStripe(Collection<DatanodeInfo> locations) {
    parityStripeNodes.clear();
    parityStripeNodes.addAll(locations);
  }
  
  public Collection<DatanodeInfo> getUsedNodes() {
    return usedNodes;
  }
  
  public boolean canStoreFileInDB() {
    return isThisFileStoredInDB && !syncOrFlushCalled;
  }
  
  public void forwardSmallFilesPacketsToDataNodes() {
    // can not save the data in the database
    if (isThisFileStoredInDB) {
      LOG.debug("Stuffed Inode:  The file can not be stored  in the database");
      isThisFileStoredInDB = false;
      if (!smallFileDataQueue.isEmpty()) {

        for (DFSPacket packet : smallFileDataQueue) {
          packet.addTraceParent(Tracer.getCurrentSpanId());
          dataQueue.addLast(packet);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Queued packet " + packet.getSeqno());
          }
        }
        smallFileDataQueue.clear();
      }
    }
  }
  
  public void syncOrFlushCalled(){
    syncOrFlushCalled = true;
  }
  
  public List<DFSPacket> getSmallFileDataQueue(){
    return smallFileDataQueue;
  }
  
  public void setFileStoredInDB(boolean isThisFileStoredInDB){
    this.isThisFileStoredInDB = isThisFileStoredInDB;
  }
}
