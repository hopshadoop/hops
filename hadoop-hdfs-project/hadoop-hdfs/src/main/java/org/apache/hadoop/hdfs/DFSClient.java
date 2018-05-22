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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.MD5MD5CRC32CastagnoliFileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.NamenodeSelector.NamenodeHandle;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsBlocksMetadata;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastUpdatedContentSummary;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DataChecksum.Type;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import javax.net.SocketFactory;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_READ_PREFETCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_READS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_DROP_BEHIND_WRITES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHE_READAHEAD;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import org.apache.hadoop.hdfs.client.ClientMmapManager;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.ipc.RetriableException;

/**
 * *****************************************************
 * DFSClient can connect to a Hadoop Filesystem and
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects
 * directly to DataNodes to read/write block data.
 * <p/>
 * Hadoop DFS users should obtain an instance of
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 * <p/>
 * ******************************************************
 */
@InterfaceAudience.Private
public class DFSClient implements java.io.Closeable {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L;
      // 1 hour
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB

  private final Configuration conf;
  private final Conf dfsClientConf;
  final NamenodeSelector namenodeSelector;
  final int MAX_RPC_RETRIES;

  /* The service used for delegation tokens */
  private Text dtService;

  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  volatile long lastLeaseRenewal;
  private volatile FsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  final String clientName;
  SocketFactory socketFactory;
  final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;
  final FileSystem.Statistics stats;
  private final String authority;
  final PeerCache peerCache;
  private Random r = new Random();
  private SocketAddress[] localInterfaceAddrs;
  private DataEncryptionKey encryptionKey;

  private boolean shouldUseLegacyBlockReaderLocal;
  private final CachingStrategy defaultReadCachingStrategy;
  private final CachingStrategy defaultWriteCachingStrategy;
  private ClientMmapManager mmapManager;
  
  private static final ClientMmapManagerFactory MMAP_MANAGER_FACTORY =
      new ClientMmapManagerFactory();

  private static final class ClientMmapManagerFactory {
    private ClientMmapManager mmapManager = null;
    /**
     * Tracks the number of users of mmapManager.
     */
    private int refcnt = 0;

    synchronized ClientMmapManager get(Configuration conf) {
      if (refcnt++ == 0) {
        mmapManager = ClientMmapManager.fromConf(conf);
      } else {
        String mismatches = mmapManager.verifyConfigurationMatches(conf);
        if (!mismatches.isEmpty()) {
          LOG.warn("The ClientMmapManager settings you specified " +
            "have been ignored because another thread created the " +
            "ClientMmapManager first.  " + mismatches);
        }
      }
      return mmapManager;
    }
    
    synchronized void unref(ClientMmapManager mmapManager) {
      if (this.mmapManager != mmapManager) {
        throw new IllegalArgumentException();
      }
      if (--refcnt == 0) {
        IOUtils.cleanup(LOG, mmapManager);
        mmapManager = null;
      }
    }
  }
  
  /**
   * DFSClient configuration
   */
  public static class Conf {
    final int hdfsTimeout;    // timeout value for a DFS operation.
    final int maxFailoverAttempts;
    final int failoverSleepBaseMillis;
    final int failoverSleepMaxMillis;
    final int maxBlockAcquireFailures;
    final int confTime;
    final int ioBufferSize;
    final ChecksumOpt defaultChecksumOpt;
    final int writePacketSize;
    final int socketTimeout;
    final int socketCacheCapacity;
    final long socketCacheExpiry;
    final long excludedNodesCacheExpiry;
    /**
     * Wait time window (in msec) if BlockMissingException is caught
     */
    final int timeWindow;
    final int nCachedConnRetry;
    final int nBlockWriteRetry;
    final int nBlockWriteLocateFollowingRetry;
    final long defaultBlockSize;
    final long prefetchSize;
    final short defaultReplication;
    final String taskId;
    final FsPermission uMask;
    final boolean connectToDnViaHostname;
    final boolean getHdfsBlocksMetadataEnabled;
    final int getFileBlockStorageLocationsNumThreads;
    final int getFileBlockStorageLocationsTimeout;
    final int retryTimesForGetLastBlockLength;
    final int retryIntervalForGetLastBlockLength;
    
    final boolean useLegacyBlockReader;
    final boolean useLegacyBlockReaderLocal;
    final String domainSocketPath;
    final boolean skipShortCircuitChecksums;
    final int shortCircuitBufferSize;
    final boolean shortCircuitLocalReads;
    final boolean domainSocketDataTraffic;
    final int shortCircuitStreamsCacheSize;
    final long shortCircuitStreamsCacheExpiryMs;
    final int dbFileMaxSize;
    final boolean storeSmallFilesInDB;
    final int dfsClientInitialWaitOnRetry;
    //small delay before closing the file ensures that incremental block reporst are processed by the
    //namenodes before the file close operation.
    final int delayBeforeClose;
    //only for testing
    final boolean hdfsClientEmulationForSF;

    public Conf(Configuration conf) {
      // The hdfsTimeout is currently the same as the ipc timeout
      hdfsTimeout = Client.getTimeout(conf);

      maxFailoverAttempts = conf.getInt(DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
          DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);
      failoverSleepBaseMillis =
          conf.getInt(DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY,
              DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT);
      failoverSleepMaxMillis =
          conf.getInt(DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY,
              DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT);

      maxBlockAcquireFailures =
          conf.getInt(DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
              DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
      confTime = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
          HdfsServerConstants.WRITE_TIMEOUT);
      ioBufferSize =
          conf.getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
              CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
      defaultChecksumOpt = getChecksumOptFromConf(conf);
      socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
          HdfsServerConstants.READ_TIMEOUT);
      /** dfs.write.packet.size is an internal config variable */
      writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
          DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
      defaultBlockSize =
          conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
      defaultReplication =
          (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
      String machineName = "";
      try {
        machineName = InetAddress.getLocalHost().getHostName() + "_";
      } catch (UnknownHostException e) {
      }
      taskId = conf.get("mapreduce.task.attempt.id", machineName+"NONMAPREDUCE");
      socketCacheCapacity = conf.getInt(DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      socketCacheExpiry = conf.getLong(DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT);
      excludedNodesCacheExpiry = conf.getLong(
          DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL,
          DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
      prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
          10 * defaultBlockSize);
      timeWindow = conf.getInt(DFS_CLIENT_RETRY_WINDOW_BASE, 3000);
      nCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
          DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
      nBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
          DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT);
      nBlockWriteLocateFollowingRetry =
          conf.getInt(DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
              DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
      uMask = FsPermission.getUMask(conf);
      connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
          DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
      getHdfsBlocksMetadataEnabled =
          conf.getBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
              DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
      getFileBlockStorageLocationsNumThreads = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT);
      getFileBlockStorageLocationsTimeout = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT,
          DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_DEFAULT);
      retryTimesForGetLastBlockLength = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH,
          DFSConfigKeys.DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
      retryIntervalForGetLastBlockLength = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH,
        DFSConfigKeys.DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);
       
      useLegacyBlockReader = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT);
      useLegacyBlockReaderLocal = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT);
      domainSocketDataTraffic = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      domainSocketPath = conf.getTrimmed(
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);
      
      if (BlockReaderLocal.LOG.isDebugEnabled()) {
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
            + " = " + useLegacyBlockReaderLocal);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_KEY
            + " = " + shortCircuitLocalReads);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
            + " = " + domainSocketDataTraffic);
        BlockReaderLocal.LOG.debug(
            DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY
            + " = " + domainSocketPath);
      }
      
      skipShortCircuitChecksums = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY,
          DFSConfigKeys.DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      dfsClientInitialWaitOnRetry =
          conf.getInt(DFSConfigKeys.DFS_CLIENT_INITIAL_WAIT_ON_RETRY_IN_MS_KEY,
              DFSConfigKeys.DFS_CLIENT_INITIAL_WAIT_ON_RETRY_IN_MS_DEFAULT);

      storeSmallFilesInDB = conf.getBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
              DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_DEFAULT);

      dbFileMaxSize = conf.getInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY,
              DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_DEFAULT);

      hdfsClientEmulationForSF = conf.getBoolean("hdfsClientEmulationForSF",false);

      delayBeforeClose = conf.getInt(DFSConfigKeys.DFS_CLIENT_DELAY_BEFORE_FILE_CLOSE_KEY,
              DFSConfigKeys.DFS_CLIENT_DELAY_BEFORE_FILE_CLOSE_DEFAULT);
    }

    private DataChecksum.Type getChecksumType(Configuration conf) {
      final String checksum = conf.get(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY,
          DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
      try {
        return DataChecksum.Type.valueOf(checksum);
      } catch (IllegalArgumentException iae) {
        LOG.warn("Bad checksum type: " + checksum + ". Using default " +
            DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
        return DataChecksum.Type
            .valueOf(DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
      }
    }

    // Construct a checksum option from conf
    private ChecksumOpt getChecksumOptFromConf(Configuration conf) {
      DataChecksum.Type type = getChecksumType(conf);
      int bytesPerChecksum = conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
          DFS_BYTES_PER_CHECKSUM_DEFAULT);
      return new ChecksumOpt(type, bytesPerChecksum);
    }

    // create a DataChecksum with the default option.
    private DataChecksum createChecksum() throws IOException {
      return createChecksum(null);
    }

    private DataChecksum createChecksum(ChecksumOpt userOpt)
        throws IOException {
      // Fill in any missing field with the default.
      ChecksumOpt myOpt =
          ChecksumOpt.processChecksumOpt(defaultChecksumOpt, userOpt);
      DataChecksum dataChecksum = DataChecksum
          .newDataChecksum(myOpt.getChecksumType(),
              myOpt.getBytesPerChecksum());
      if (dataChecksum == null) {
        throw new IOException("Invalid checksum type specified: " +
            myOpt.getChecksumType().name());
      }
      return dataChecksum;
    }
  }

  public Conf getConf() {
    return dfsClientConf;
  }
  
  Configuration getConfiguration() {
    return conf;
  }
  
  /**
   * A map from file names to {@link DFSOutputStream} objects
   * that are currently being written by this client.
   * Note that a file can only be written by a single client.
   */
  private final Map<String, DFSOutputStream> filesBeingWritten =
      new HashMap<>();

  private final DomainSocketFactory domainSocketFactory;
  
  /**
   * Same as this(NameNode.getAddress(conf), conf);
   *
   * @see #DFSClient(InetSocketAddress, Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }
  
  public DFSClient(InetSocketAddress address, Configuration conf)
      throws IOException {
    this(NameNode.getUri(address), conf);
  }

  /**
   * Same as this(nameNodeUri, conf, null);
   *
   * @see #DFSClient(URI, Configuration, FileSystem.Statistics)
   */
  public DFSClient(URI nameNodeUri, Configuration conf) throws IOException {
    this(nameNodeUri, conf, null);
  }

  /**
   * Same as this(nameNodeUri, null, conf, stats);
   *
   * @see #DFSClient(URI, ClientProtocol, Configuration, FileSystem.Statistics)
   */
  public DFSClient(URI nameNodeUri, Configuration conf,
      FileSystem.Statistics stats) throws IOException {
    this(nameNodeUri, null, conf, stats);
  }
  
  /**
   * Create a new DFSClient connected to the given nameNodeUri or rpcNamenode.
   * If a positive value is set for 
   * {@link DFSConfigKeys#DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY} in the
   * configuration, the DFSClient will use {@link LossyRetryInvocationHandler}
   * as its RetryInvocationHandler. Otherwise one of nameNodeUri or rpcNamenode 
   * must be null.
   */
  @VisibleForTesting
  public DFSClient(URI nameNodeUri, ClientProtocol rpcNamenode, Configuration conf,
      FileSystem.Statistics stats) throws IOException {
    // Copy only the required DFSClient configuration
    this.dfsClientConf = new Conf(conf);
    this.shouldUseLegacyBlockReaderLocal =
        this.dfsClientConf.useLegacyBlockReaderLocal;
    if (this.dfsClientConf.useLegacyBlockReaderLocal) {
      LOG.debug("Using legacy short-circuit local reads.");
    }
    checkSmallFilesSupportConf(conf);
    this.conf = conf;
    this.stats = stats;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

    this.ugi = UserGroupInformation.getCurrentUser();
    
    this.authority = nameNodeUri == null ? "null" : nameNodeUri.getAuthority();
    String clientNamePrefix = "";
    if(dfsClientConf.hdfsClientEmulationForSF){
      clientNamePrefix = "DFSClient";
    }else{
      clientNamePrefix = "HopsFS_DFSClient";
    }
    this.clientName = clientNamePrefix+ "_" + dfsClientConf.taskId + "_" +
        DFSUtil.getRandom().nextInt() + "_" + Thread.currentThread().getId();
    
    int numResponseToDrop = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY,
        DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT);
    NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = null;
    if (numResponseToDrop > 0) {
      // This case is used for testing.
      LOG.warn(DFSConfigKeys.DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY
          + " is set to " + numResponseToDrop
          + ", this hacked client will proactively drop responses");
      proxyInfo = NameNodeProxies.createProxyWithLossyRetryHandler(conf,
          nameNodeUri, ClientProtocol.class, numResponseToDrop);
    }
    
    if (proxyInfo != null) {
      this.dtService = proxyInfo.getDelegationTokenService();
      namenodeSelector = new NamenodeSelector(conf, nameNodeUri, this.ugi);
    } else if (rpcNamenode != null) {
      // This case is used for testing.
      Preconditions.checkArgument(nameNodeUri == null);
      namenodeSelector = new NamenodeSelector(conf, rpcNamenode, this.ugi);
      dtService = null;
    } else {
      Preconditions.checkArgument(nameNodeUri != null, "null URI");
      proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
          ClientProtocol.class);
      this.dtService = proxyInfo.getDelegationTokenService();
      namenodeSelector = new NamenodeSelector(conf, nameNodeUri, this.ugi);
    }

    // read directly from the block file if configured.
    this.domainSocketFactory = new DomainSocketFactory(dfsClientConf);
    
    String localInterfaces[] =
        conf.getTrimmedStrings(DFSConfigKeys.DFS_CLIENT_LOCAL_INTERFACES);
    localInterfaceAddrs = getLocalInterfaceAddrs(localInterfaces);
    if (LOG.isDebugEnabled() && 0 != localInterfaces.length) {
      LOG.debug("Using local interfaces [" +
          Joiner.on(',').join(localInterfaces) + "] with addresses [" +
          Joiner.on(',').join(localInterfaceAddrs) + "]");
    }
    
    this.peerCache = PeerCache.getInstance(dfsClientConf.socketCacheCapacity, dfsClientConf.socketCacheExpiry);
    
     Boolean readDropBehind = (conf.get(DFS_CLIENT_CACHE_DROP_BEHIND_READS) == null) ?
        null : conf.getBoolean(DFS_CLIENT_CACHE_DROP_BEHIND_READS, false);
    Long readahead = (conf.get(DFS_CLIENT_CACHE_READAHEAD) == null) ?
        null : conf.getLong(DFS_CLIENT_CACHE_READAHEAD, 0);
    Boolean writeDropBehind = (conf.get(DFS_CLIENT_CACHE_DROP_BEHIND_WRITES) == null) ?
        null : conf.getBoolean(DFS_CLIENT_CACHE_DROP_BEHIND_WRITES, false);
    this.defaultReadCachingStrategy =
        new CachingStrategy(readDropBehind, readahead);
    this.defaultWriteCachingStrategy =
        new CachingStrategy(writeDropBehind, readahead);
    this.mmapManager = MMAP_MANAGER_FACTORY.get(conf);
    
    this.MAX_RPC_RETRIES =
        conf.getInt(DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_KEY,
            DFSConfigKeys.DFS_CLIENT_RETRIES_ON_FAILURE_DEFAULT);

  }

  private void checkSmallFilesSupportConf(Configuration conf) throws IOException {
    if(isStoreSmallFilesInDB()) {
      if(getDBFileMaxSize() > getDefaultBlockSize()){
        throw new IOException("The size of a file stored in the database should not more than the size of the default block size");
      }
    }
  }
  /**
   * Return the socket addresses to use with each configured
   * local interface. Local interfaces may be specified by IP
   * address, IP address range using CIDR notation, interface
   * name (e.g. eth0) or sub-interface name (e.g. eth0:0).
   * The socket addresses consist of the IPs for the interfaces
   * and the ephemeral port (port 0). If an IP, IP range, or
   * interface name matches an interface with sub-interfaces
   * only the IP of the interface is used. Sub-interfaces can
   * be used by specifying them explicitly (by IP or name).
   *
   * @return SocketAddresses for the configured local interfaces,
   * or an empty array if none are configured
   * @throws UnknownHostException
   *     if a given interface name is invalid
   */
  private static SocketAddress[] getLocalInterfaceAddrs(String interfaceNames[])
      throws UnknownHostException {
    List<SocketAddress> localAddrs = new ArrayList<>();
    for (String interfaceName : interfaceNames) {
      if (InetAddresses.isInetAddress(interfaceName)) {
        localAddrs.add(new InetSocketAddress(interfaceName, 0));
      } else if (NetUtils.isValidSubnet(interfaceName)) {
        for (InetAddress addr : NetUtils.getIPs(interfaceName, false)) {
          localAddrs.add(new InetSocketAddress(addr, 0));
        }
      } else {
        for (String ip : DNS.getIPs(interfaceName, false)) {
          localAddrs.add(new InetSocketAddress(ip, 0));
        }
      }
    }
    return localAddrs.toArray(new SocketAddress[localAddrs.size()]);
  }

  /**
   * Select one of the configured local interfaces at random. We use a random
   * interface because other policies like round-robin are less effective
   * given that we cache connections to datanodes.
   *
   * @return one of the local interface addresses at random, or null if no
   * local interfaces are configured
   */
  SocketAddress getRandomLocalInterfaceAddr() {
    if (localInterfaceAddrs.length == 0) {
      return null;
    }
    final int idx = r.nextInt(localInterfaceAddrs.length);
    final SocketAddress addr = localInterfaceAddrs[idx];
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using local interface " + addr);
    }
    return addr;
  }

  /**
   * Return the number of times the client should go back to the namenode
   * to retrieve block locations when reading.
   */
  int getMaxBlockAcquireFailures() {
    return dfsClientConf.maxBlockAcquireFailures;
  }

  /**
   * Return the timeout that clients should use when writing to datanodes.
   *
   * @param numNodes
   *     the number of nodes in the pipeline.
   */
  int getDatanodeWriteTimeout(int numNodes) {
    return (dfsClientConf.confTime > 0) ? (dfsClientConf.confTime +
        HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * numNodes) : 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    return dfsClientConf.socketTimeout > 0 ?
        (HdfsServerConstants.READ_TIMEOUT_EXTENSION * numNodes +
            dfsClientConf.socketTimeout) : 0;
  }
  
  int getHdfsTimeout() {
    return dfsClientConf.hdfsTimeout;
  }
  
  @VisibleForTesting
  public String getClientName() {
    return clientName;
  }

  void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }

  /**
   * Return the lease renewer instance. The renewer thread won't start
   * until the first output stream is created. The same instance will
   * be returned until all output streams are closed.
   */
  public LeaseRenewer getLeaseRenewer() throws IOException {
    return LeaseRenewer.getInstance(authority, ugi, this);
  }

  /**
   * Get a lease and start automatic renewal
   */
  private void beginFileLease(final String src, final DFSOutputStream out)
      throws IOException {
    getLeaseRenewer().put(src, out, this);
  }

  /**
   * Stop renewal of lease for the file.
   */
  void endFileLease(final String src) throws IOException {
    getLeaseRenewer().closeFile(src, this);
  }


  /**
   * Put a file. Only called from LeaseRenewer, where proper locking is
   * enforced to consistently update its local dfsclients array and
   * client's filesBeingWritten map.
   */
  void putFileBeingWritten(final String src, final DFSOutputStream out) {
    synchronized (filesBeingWritten) {
      filesBeingWritten.put(src, out);
      // update the last lease renewal time only when there was no
      // writes. once there is one write stream open, the lease renewer
      // thread keeps it updated well with in anyone's expiration time.
      if (lastLeaseRenewal == 0) {
        updateLastLeaseRenewal();
      }
    }
  }

  /**
   * Remove a file. Only called from LeaseRenewer.
   */
  void removeFileBeingWritten(final String src) {
    synchronized (filesBeingWritten) {
      filesBeingWritten.remove(src);
      if (filesBeingWritten.isEmpty()) {
        lastLeaseRenewal = 0;
      }
    }
  }

  /**
   * Is file-being-written map empty?
   */
  boolean isFilesBeingWrittenEmpty() {
    synchronized (filesBeingWritten) {
      return filesBeingWritten.isEmpty();
    }
  }
  
  /**
   * @return true if the client is running
   */
  boolean isClientRunning() {
    return clientRunning;
  }

  long getLastLeaseRenewal() {
    return lastLeaseRenewal;
  }

  void updateLastLeaseRenewal() {
    synchronized (filesBeingWritten) {
      if (filesBeingWritten.isEmpty()) {
        return;
      }
      lastLeaseRenewal = Time.now();
    }
  }

  /**
   * Renew leases.
   *
   * @return true if lease was renewed. May return false if this
   * client has been closed or has no files open.
   */
  boolean renewLease() throws IOException {
    if (clientRunning && !isFilesBeingWrittenEmpty()) {
      try {
        ClientActionHandler handler = new ClientActionHandler() {
          @Override
          public Object doAction(ClientProtocol namenode)
              throws RemoteException, IOException {
            namenode.renewLease(clientName);
            return null;
          }
        };
        doClientActionWithRetry(handler, "renewLease");

        updateLastLeaseRenewal();
        return true;
      } catch (IOException e) {
        // Abort if the lease has already expired. 
        final long elapsed = Time.now() - getLastLeaseRenewal();
        if (elapsed > HdfsConstants.LEASE_HARDLIMIT_PERIOD) {
          LOG.warn("Failed to renew lease for " + clientName + " for " +
              (elapsed / 1000) + " seconds (>= hard-limit =" +
              (HdfsConstants.LEASE_HARDLIMIT_PERIOD / 1000) + " seconds.) " +
              "Closing all files being written ...", e);
          closeAllFilesBeingWritten(true);
        } else {
          // Let the lease renewer handle it and retry.
          throw e;
        }
      }
    }
    return false;
  }
  
  /**
   * Close connections the Namenode.
   */
  void closeConnectionToNamenode() {
    namenodeSelector.close();
  }
  
  /**
   * Abort and release resources held.  Ignore all errors.
   */
  void abort() {
    if (mmapManager != null) {
      MMAP_MANAGER_FACTORY.unref(mmapManager);
      mmapManager = null;
    }
    clientRunning = false;
    closeAllFilesBeingWritten(true);
    try {
      // remove reference to this client and stop the renewer,
      // if there is no more clients under the renewer.
      getLeaseRenewer().closeClient(this);
    } catch (IOException ioe) {
      LOG.info("Exception occurred while aborting the client " + ioe);
    }
    closeConnectionToNamenode();
  }

  /**
   * Close/abort all files being written.
   */
  private void closeAllFilesBeingWritten(final boolean abort) {
    for (; ; ) {
      final String src;
      final DFSOutputStream out;
      synchronized (filesBeingWritten) {
        if (filesBeingWritten.isEmpty()) {
          return;
        }
        src = filesBeingWritten.keySet().iterator().next();
        out = filesBeingWritten.remove(src);
      }
      if (out != null) {
        try {
          if (abort) {
            out.abort();
          } else {
            out.close();
          }
        } catch (IOException ie) {
          LOG.error("Failed to " + (abort ? "abort" : "close") + " file " + src,
              ie);
        }
      }
    }
  }

  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  @Override
  public synchronized void close() throws IOException {
    if (mmapManager != null) {
      MMAP_MANAGER_FACTORY.unref(mmapManager);
      mmapManager = null;
    }
    if (clientRunning) {
      closeAllFilesBeingWritten(false);
      clientRunning = false;
      getLeaseRenewer().closeClient(this);
      // close connections to the namenode
      closeConnectionToNamenode();
    }
  }

  /**
   * Get the default block size for this cluster
   *
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return dfsClientConf.defaultBlockSize;
  }

  /**
   * Is storing small files in the database enabled?
   * @return True/False
     */
  private boolean isStoreSmallFilesInDB(){
    return dfsClientConf.storeSmallFilesInDB;
  }

  /**
   * Get max size of a file that can be stored in the database.
   * @return Max size of a file that can be stored in the database.
   */
  private int getDBFileMaxSize(){
    return  dfsClientConf.dbFileMaxSize;
  }

  /**
   * @see ClientProtocol#getPreferredBlockSize(String)
   */
  public long getBlockSize(final String f) throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getPreferredBlockSize(f);
        }
      };
      return (Long) doClientActionWithRetry(handler, "getBlockSize");

    } catch (IOException ie) {
      LOG.warn("Problem getting block size", ie);
      throw ie;
    }
  }

  /**
   * Get server default values for a number of configuration params.
   *
   * @see ClientProtocol#getServerDefaults()
   */
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = Time.now();
    if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getServerDefaults();
        }
      };
      serverDefaults = (FsServerDefaults) doClientActionWithRetry(handler,
          "getServerDefaults");
      serverDefaultsLastUpdate = now;
    }
    return serverDefaults;
  }
  
  /**
   * Get a canonical token service name for this client's tokens.  Null should
   * be returned if the client is not using tokens.
   *
   * @return the token service for the client
   */
  @InterfaceAudience.LimitedPrivate({"HDFS"})
  public String getCanonicalServiceName() {
    return (dtService != null) ? dtService.toString() : null;
  }
  
  /**
   * @see ClientProtocol#getDelegationToken(Text)
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(final Text renewer)
      throws IOException {
    assert dtService != null;
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getDelegationToken(renewer);
      }
    };
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) doClientActionWithRetry(handler,
            "getDelegationToken");
    token.setService(this.dtService);

    LOG.info("Created " + DelegationTokenIdentifier.stringifyToken(token));
    return token;
  }

  /**
   * Renew a delegation token
   *
   * @param token
   *     the token to renew
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  @Deprecated
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Renewing " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      return token.renew(conf);
    } catch (InterruptedException ie) {
      throw new RuntimeException("caught interrupted", ie);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
          AccessControlException.class);
    }
  }
  
  private static Map<String, Boolean> localAddrMap =
      Collections.synchronizedMap(new HashMap<String, Boolean>());
  
  static boolean isLocalAddress(InetSocketAddress targetAddr) {
    InetAddress addr = targetAddr.getAddress();
    Boolean cached = localAddrMap.get(addr.getHostAddress());
    if (cached != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Address " + targetAddr +
            (cached ? " is local" : " is not local"));
      }
      return cached;
    }

    boolean local = NetUtils.isLocalAddress(addr);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Address " + targetAddr +
          (local ? " is local" : " is not local"));
    }
    localAddrMap.put(addr.getHostAddress(), local);
    return local;
  }
  
  /**
   * Should the block access token be refetched on an exception
   *
   * @param ex
   *     Exception received
   * @param targetAddr
   *     Target datanode address from where exception was received
   * @return true if block access token has expired or invalid and it should be
   * refetched
   */
  private static boolean tokenRefetchNeeded(IOException ex,
      InetSocketAddress targetAddr) {
    /*
     * Get a new access token and retry. Retry is needed in 2 cases. 1) When
     * both NN and DN re-started while DFSClient holding a cached access token.
     * 2) In the case that NN fails to update its access key at pre-set interval
     * (by a wide margin) and subsequently restarts. In this case, DN
     * re-registers itself with NN and receives a new access key, but DN will
     * delete the old access key from its memory since it's considered expired
     * based on the estimated expiration date.
     */
    if (ex instanceof InvalidBlockTokenException ||
        ex instanceof InvalidToken) {
      LOG.info(
          "Access token was invalid when connecting to " + targetAddr + " : " +
              ex);
      return true;
    }
    return false;
  }

  /**
   * Cancel a delegation token
   *
   * @param token
   *     the token to cancel
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  @Deprecated
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Cancelling " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      token.cancel(conf);
    } catch (InterruptedException ie) {
      throw new RuntimeException("caught interrupted", ie);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
          AccessControlException.class);
    }
  }
  
  @InterfaceAudience.Private
  public static class Renewer extends TokenRenewer {
    
    static {
      //Ensure that HDFS Configuration files are loaded before trying to use
      // the renewer.
      HdfsConfiguration.init();
    }
    
    @Override
    public boolean handleKind(Text kind) {
      return DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(kind);
    }

    @SuppressWarnings("unchecked")
    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException {
      Token<DelegationTokenIdentifier> delToken =
          (Token<DelegationTokenIdentifier>) token;
      ClientProtocol nn = getNNProxy(delToken, conf);
      try {
        return nn.renewDelegationToken(delToken);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(InvalidToken.class,
            AccessControlException.class);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException {
      Token<DelegationTokenIdentifier> delToken =
          (Token<DelegationTokenIdentifier>) token;
      LOG.info(
          "Cancelling " + DelegationTokenIdentifier.stringifyToken(delToken));
      ClientProtocol nn = getNNProxy(delToken, conf);
      try {
        nn.cancelDelegationToken(delToken);
      } catch (RemoteException re) {
        throw re.unwrapRemoteException(InvalidToken.class,
            AccessControlException.class);
      }
    }
    
    private static ClientProtocol getNNProxy(
        Token<DelegationTokenIdentifier> token, Configuration conf)
        throws IOException {
      URI uri = DFSUtil.getServiceUriFromToken(token);
      NameNodeProxies.ProxyAndInfo<ClientProtocol> info =
          NameNodeProxies.createProxy(conf, uri, ClientProtocol.class);
      assert info.getDelegationTokenService().equals(token.getService()) :
          "Returned service '" + info.getDelegationTokenService().toString() +
              "' doesn't match expected service '" +
              token.getService().toString() + "'";

      return info.getProxy();
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      return true;
    }
    
  }

  /**
   * Report corrupt blocks that were discovered by the client.
   *
   * @see ClientProtocol#reportBadBlocks(LocatedBlock[])
   */
  public void reportBadBlocks(final LocatedBlock[] blocks) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.reportBadBlocks(blocks);
        return null;
      }
    };
    doClientActionWithRetry(handler, "reportBadBlocks");
  }
  
  public short getDefaultReplication() {
    return dfsClientConf.defaultReplication;
  }
  
  public LocatedBlocks getLocatedBlocks(String src, long start)
      throws IOException {
    return getLocatedBlocks(src, start, dfsClientConf.prefetchSize);
  }

  /*
   * This is just a wrapper around callGetBlockLocations, but non-static so that
   * we can stub it out for tests.
   */
  @VisibleForTesting
  public LocatedBlocks getLocatedBlocks(final String src, final long start,
      final long length) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return callGetBlockLocations(namenode, src, start, length);
      }
    };
    return (LocatedBlocks) doClientActionWithRetry(handler, "getLocatedBlocks");
  }

  /**
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) throws IOException {
    try {
      return namenode.getBlockLocations(src, start, length);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * @see ClientProtocol#getMissingBlockLocations(String)
   */
  public LocatedBlocks getMissingLocatedBlocks(final String src)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return callGetMissingBlockLocations(namenode, src);
      }
    };
    return (LocatedBlocks) doClientActionWithRetry(handler,
        "getMissingLocatedBlocks");
  }

  static LocatedBlocks callGetMissingBlockLocations(ClientProtocol namenode,
      String filePath) throws IOException {
    try {
      return namenode.getMissingBlockLocations(filePath);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * @see ClientProtocol#addBlockChecksum(String, int, long)
   */
  public void addBlockChecksum(final String src, final int blockIndex,
      final long checksum) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode) throws IOException {
        try {
          namenode.addBlockChecksum(src, blockIndex, checksum);
        } catch (RemoteException re) {
          throw re.unwrapRemoteException();
        }
        return null;
      }
    };
    doClientActionWithRetry(handler, "addBlockChecksum");
  }

  /**
   * @see ClientProtocol#getBlockChecksum(String, int)
   */
  public long getBlockChecksum(final String filePath, final int blockIndex)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode) throws IOException {
        try {
          return namenode.getBlockChecksum(filePath, blockIndex);
        } catch (RemoteException e) {
          throw e.unwrapRemoteException();
        }
      }
    };
    return (Long) doClientActionWithRetry(handler, "getBlockChecksum");
  }

  /**
   * Recover a file's lease
   *
   * @param src
   *     a file's path
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.recoverLease(src, clientName);
        }
      };
      return (Boolean) doClientActionWithRetry(handler, "recoverLease");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
          AccessControlException.class);
    }
  }

  /**
   * Get block location info about file
   * <p/>
   * getBlockLocations() returns a list of hostnames that store
   * data for a specific file region.  It returns a set of hostnames
   * for every block within the indicated region.
   * <p/>
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes.
   */
  public BlockLocation[] getBlockLocations(String src, long start, long length)
      throws IOException, UnresolvedLinkException {
    LocatedBlocks blocks = getLocatedBlocks(src, start, length);
    BlockLocation[] locations = DFSUtil.locatedBlocks2Locations(blocks);
    HdfsBlockLocation[] hdfsLocations = new HdfsBlockLocation[locations.length];
    for (int i = 0; i < locations.length; i++) {
      hdfsLocations[i] = new HdfsBlockLocation(locations[i], blocks.get(i));
    }
    return hdfsLocations;
  }
  
  /**
   * Get block location information about a list of {@link HdfsBlockLocation}.
   * Used by {@link DistributedFileSystem#getFileBlockStorageLocations(List)}
   * to
   * get {@link BlockStorageLocation}s for blocks returned by
   * {@link DistributedFileSystem#getFileBlockLocations(org.apache.hadoop.fs.FileStatus,
   * long, long)}
   * .
   * <p/>
   * This is done by making a round of RPCs to the associated datanodes, asking
   * the volume of each block replica. The returned array of
   * {@link BlockStorageLocation} expose this information as a
   * {@link VolumeId}.
   *
   * @param blockLocations
   *     target blocks on which to query volume location information
   * @return volumeBlockLocations original block array augmented with additional
   * volume location information for each replica.
   */
  public BlockStorageLocation[] getBlockStorageLocations(
      List<BlockLocation> blockLocations)
      throws IOException, UnsupportedOperationException,
      InvalidBlockTokenException {
    if (!getConf().getHdfsBlocksMetadataEnabled) {
      throw new UnsupportedOperationException("Datanode-side support for " +
          "getVolumeBlockLocations() must also be enabled in the client " +
          "configuration.");
    }
    // Downcast blockLocations and fetch out required LocatedBlock(s)
    List<LocatedBlock> blocks = new ArrayList<>();
    for (BlockLocation loc : blockLocations) {
      if (!(loc instanceof HdfsBlockLocation)) {
        throw new ClassCastException("DFSClient#getVolumeBlockLocations " +
            "expected to be passed HdfsBlockLocations");
      }
      HdfsBlockLocation hdfsLoc = (HdfsBlockLocation) loc;
      blocks.add(hdfsLoc.getLocatedBlock());
    }
    
    // Re-group the LocatedBlocks to be grouped by datanodes, with the values
    // a list of the LocatedBlocks on the datanode.
    Map<DatanodeInfo, List<LocatedBlock>> datanodeBlocks =
        new LinkedHashMap<>();
    for (LocatedBlock b : blocks) {
      for (DatanodeInfo info : b.getLocations()) {
        if (!datanodeBlocks.containsKey(info)) {
          datanodeBlocks.put(info, new ArrayList<LocatedBlock>());
        }
        List<LocatedBlock> l = datanodeBlocks.get(info);
        l.add(b);
      }
    }

    // Make RPCs to the datanodes to get volume locations for its replicas
    List<HdfsBlocksMetadata> metadatas = BlockStorageLocationUtil
        .queryDatanodesForHdfsBlocksMetadata(conf, datanodeBlocks,
            getConf().getFileBlockStorageLocationsNumThreads,
            getConf().getFileBlockStorageLocationsTimeout,
            getConf().connectToDnViaHostname);
    
    // Regroup the returned VolumeId metadata to again be grouped by
    // LocatedBlock rather than by datanode
    Map<LocatedBlock, List<VolumeId>> blockVolumeIds = BlockStorageLocationUtil
        .associateVolumeIdsWithBlocks(blocks, datanodeBlocks, metadatas);
    
    // Combine original BlockLocations with new VolumeId information
    BlockStorageLocation[] volumeBlockLocations = BlockStorageLocationUtil
        .convertToVolumeBlockLocations(blocks, blockVolumeIds);

    return volumeBlockLocations;
  }
  
  public DFSInputStream open(String src)
      throws IOException, UnresolvedLinkException {
    return open(src, dfsClientConf.ioBufferSize, true, null);
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   *
   * @deprecated Use {@link #open(String, int, boolean)} instead.
   */
  @Deprecated
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
      FileSystem.Statistics stats) throws IOException, UnresolvedLinkException {
    return open(src, buffersize, verifyChecksum);
  }
  

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(this, src, buffersize, verifyChecksum, dfsClientConf.hdfsClientEmulationForSF);
  }

  /**
   * Get the namenode associated with this DFSClient object
   *
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() throws IOException {
    return namenodeSelector.getNextNamenode().getRPCHandle();
  }
  
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code> and null <code>
   * progress</code>.
   */
  public OutputStream create(String src, boolean overwrite) throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, null);
  }

  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code>.
   */
  public OutputStream create(String src, boolean overwrite,
      Progressable progress) throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, progress);
  }

  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * null <code>progress</code>.
   */
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Call {@link #create(String, boolean, short, long, Progressable, int)}
   * with default bufferSize.
   */
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize, Progressable progress) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        dfsClientConf.ioBufferSize);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, short, long,
   * Progressable, int, ChecksumOpt)} with default <code>permission</code>
   * {@link FsPermission#getFileDefault()}.
   *
   * @param src
   *     File name
   * @param overwrite
   *     overwrite an existing file if true
   * @param replication
   *     replication factor for the file
   * @param blockSize
   *     maximum block size
   * @param progress
   *     interface for reporting client progress
   * @param buffersize
   *     underlying buffersize
   * @return output stream
   */
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize, Progressable progress, int buffersize)
      throws IOException {
    return create(src, FsPermission.getFileDefault(),
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE) :
            EnumSet.of(CreateFlag.CREATE), replication, blockSize, progress,
        buffersize, null);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, boolean, short,
   * long, Progressable, int, ChecksumOpt)} with <code>createParent</code>
   * set to true.
   */
  public DFSOutputStream create(String src, FsPermission permission,
      EnumSet<CreateFlag> flag, short replication, long blockSize,
      Progressable progress, int buffersize, ChecksumOpt checksumOpt)
      throws IOException {
    return create(src, permission, flag, true, replication, blockSize, progress,
        buffersize, checksumOpt);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, boolean, short,
   * long, Progressable, int, ChecksumOpt)} with <code>createParent</code>
   * set to true.
   */
  public DFSOutputStream create(String src, FsPermission permission,
      EnumSet<CreateFlag> flag, short replication, long blockSize,
      Progressable progress, int buffersize, ChecksumOpt checksumOpt,
      EncodingPolicy policy) throws IOException {
    return create(src, permission, flag, true, replication, blockSize, progress,
        buffersize, checksumOpt, null, policy);
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src
   *     File name
   * @param permission
   *     The permission of the directory being created.
   *     If null, use default permission {@link FsPermission#getFileDefault()}
   * @param flag
   *     indicates create a new file or create/overwrite an
   *     existing file or append to an existing file
   * @param createParent
   *     create missing parent directory if true
   * @param replication
   *     block replication
   * @param blockSize
   *     maximum block size
   * @param progress
   *     interface for reporting client progress
   * @param buffersize
   *     underlying buffer size
   * @param checksumOpt
   *     checksum options
   * @return output stream
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable,
   * boolean, short, long) for detailed description of exceptions thrown
   */
  public DFSOutputStream create(String src, FsPermission permission,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      ChecksumOpt checksumOpt) throws IOException {
    return create(src, permission, flag, createParent, replication, blockSize,
        progress, buffersize, checksumOpt, null, null);
  }

  /**
   * Create a new dfs file with the specified block replication
   * with write-progress reporting and return an output stream for writing
   * into the file.
   *
   * @param src
   *     File name
   * @param permission
   *     The permission of the directory being created.
   *     If null, use default permission {@link FsPermission#getFileDefault()}
   * @param flag
   *     indicates create a new file or create/overwrite an
   *     existing file or append to an existing file
   * @param createParent
   *     create missing parent directory if true
   * @param replication
   *     block replication
   * @param blockSize
   *     maximum block size
   * @param progress
   *     interface for reporting client progress
   * @param buffersize
   *     underlying buffer size
   * @param checksumOpt
   *     checksum options
   * @return output stream
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable,
   * boolean, short, long) for detailed description of exceptions thrown
   */
  public DFSOutputStream create(String src, FsPermission permission,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      ChecksumOpt checksumOpt, InetSocketAddress[] favoredNodes, EncodingPolicy policy) throws IOException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getFileDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    if (LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + masked);
    }
    String[] favoredNodeStrs = null;
    if (favoredNodes != null) {
      favoredNodeStrs = new String[favoredNodes.length];
      for (int i = 0; i < favoredNodes.length; i++) {
        favoredNodeStrs[i] = 
            favoredNodes[i].getHostName() + ":"
                         + favoredNodes[i].getPort();
      }
    }
    final DFSOutputStream result = DFSOutputStream
        .newStreamForCreate(this, src, masked, flag, createParent, replication,
            blockSize, progress, buffersize,
            dfsClientConf.createChecksum(checksumOpt), favoredNodeStrs, policy, isStoreSmallFilesInDB(),
            getDBFileMaxSize());
    beginFileLease(src, result);
    return result;
  }

  public DFSOutputStream sendBlock(String src, LocatedBlock block,
      Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    checkOpen();
    HdfsFileStatus stat = getFileInfo(src);
    if (stat == null) { // No file found
      throw new FileNotFoundException(
          "failed to append to non-existent file " + src + " on client " +
              clientName);
    }
    final DFSOutputStream result = DFSOutputStream
        .newStreamForSingleBlock(this, src, dfsClientConf.ioBufferSize,
            progress, block, dfsClientConf.createChecksum(checksumOpt), stat);
    return result;
  }

  /**
   * Append to an existing file if {@link CreateFlag#APPEND} is present
   */
  private DFSOutputStream primitiveAppend(String src, EnumSet<CreateFlag> flag,
      int buffersize, Progressable progress) throws IOException {
    if (flag.contains(CreateFlag.APPEND)) {
      HdfsFileStatus stat = getFileInfo(src);
      if (stat == null) { // No file to append to
        // New file needs to be created if create option is present
        if (!flag.contains(CreateFlag.CREATE)) {
          throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientName);
        }
        return null;
      }
      return callAppend(src, buffersize, progress);
    }
    return null;
  }
  
  /**
   * Same as {{@link #create(String, FsPermission, EnumSet, short, long,
   * Progressable, int, ChecksumOpt)} except that the permission
   * is absolute (ie has already been masked with umask.
   */
  public DFSOutputStream primitiveCreate(String src, FsPermission absPermission,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize, Progressable progress, int buffersize,
      ChecksumOpt checksumOpt) throws IOException, UnresolvedLinkException {
    checkOpen();
    CreateFlag.validate(flag);
    DFSOutputStream result = primitiveAppend(src, flag, buffersize, progress);
    if (result == null) {
      DataChecksum checksum = dfsClientConf.createChecksum(checksumOpt);
      result = DFSOutputStream
          .newStreamForCreate(this, src, absPermission, flag, createParent,
              replication, blockSize, progress, buffersize, checksum, isStoreSmallFilesInDB(), getDBFileMaxSize());
    }
    beginFileLease(src, result);
    return result;
  }
  
  /**
   * Creates a symbolic link.
   *
   * @see ClientProtocol#createSymlink(String, String, FsPermission, boolean)
   */
  public void createSymlink(final String target, final String link,
      final boolean createParent) throws IOException {
    try {
      final FsPermission dirPerm =
          FsPermission.getDefault().applyUMask(dfsClientConf.uMask);
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.createSymlink(target, link, dirPerm, createParent);
          return null;
        }
      };
      doClientActionWithRetry(handler, "createSymlink");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileAlreadyExistsException.class, FileNotFoundException.class,
          ParentNotDirectoryException.class, NSQuotaExceededException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }

  /**
   * Resolve the *first* symlink, if any, in the path.
   *
   * @see ClientProtocol#getLinkTarget(String)
   */
  public String getLinkTarget(final String path) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getLinkTarget(path);
        }
      };
      return (String) doClientActionWithRetry(handler, "getLinkTarget");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class);
    }
  }

  /**
   * Method to get stream returned by append call
   */
  private DFSOutputStream callAppend(final String src,
      int buffersize, Progressable progress) throws IOException {
    LocatedBlock lastBlock = null;
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.append(src, clientName);
        }
      };
      lastBlock = (LocatedBlock) doClientActionWithRetry(handler, "callAppend");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          DSQuotaExceededException.class, UnsupportedOperationException.class,
          UnresolvedPathException.class);
    }

    HdfsFileStatus stat = getFileInfo(src);
    if (stat == null) { // No file found
      throw new FileNotFoundException(
          "failed to append to non-existent file " + src + " on client " +
              clientName);
    }

    return DFSOutputStream.newStreamForAppend(this, src, buffersize, progress, lastBlock, stat,
            dfsClientConf.createChecksum(), isStoreSmallFilesInDB(), getDBFileMaxSize());
  }
  
  /**
   * Append to an existing HDFS file.
   *
   * @param src
   *     file name
   * @param buffersize
   *     buffer size
   * @param progress
   *     for reporting write-progress; null is acceptable.
   * @param statistics
   *     file system statistics; null is acceptable.
   * @return an output stream for writing into the file
   * @see ClientProtocol#append(String, String)
   */
  public HdfsDataOutputStream append(final String src, final int buffersize,
      final Progressable progress, final FileSystem.Statistics statistics)
      throws IOException {
    final DFSOutputStream out = append(src, buffersize, progress);
    return new HdfsDataOutputStream(out, statistics, out.getInitialLen());
  }

  private DFSOutputStream append(String src, int buffersize,
      Progressable progress) throws IOException {
    checkOpen();
    final DFSOutputStream result = callAppend(src, buffersize, progress);
    beginFileLease(src, result);
    return result;
  }

  /**
   * Set replication for an existing file.
   *
   * @param src
   *     file name
   * @param replication
   * @see ClientProtocol#setReplication(String, short)
   */
  public boolean setReplication(final String src, final short replication)
      throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.setReplication(src, replication);
        }
      };
      return (Boolean) doClientActionWithRetry(handler, "setReplication");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }

  /**
   * Set storage policy for an existing file/directory
   * @param src file/directory name
   * @param policyName name of the storage policy
   */
  public void setStoragePolicy(final String src, final String policyName)
      throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setStoragePolicy(src, policyName);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setStoragePolicy");

    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class,
          SafeModeException.class,
          NSQuotaExceededException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * @return All the existing storage policies
   */
  public BlockStoragePolicy getStoragePolicy(final byte storagePolicyID) throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getStoragePolicy(storagePolicyID);
        }
      };
      return (BlockStoragePolicy) doClientActionWithRetry(handler, "getStoragePolicy");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }

  /**
   * @return All the existing storage policies
   */
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getStoragePolicies();
        }
      };
      return (BlockStoragePolicy[]) doClientActionWithRetry(handler, "getStoragePolicies");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }

  public void setMetaEnabled(final String src, final boolean metaEnabled)
      throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setMetaEnabled(src, metaEnabled);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setMetaEnabled");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Rename file or directory.
   *
   * @see ClientProtocol#rename(String, String)
   * @deprecated Use {@link #rename(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  public boolean rename(final String src, final String dst) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.rename(src, dst);
        }
      };
      return (Boolean) doClientActionWithRetry(handler, "rename");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          NSQuotaExceededException.class, DSQuotaExceededException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Move blocks from src to trg and delete src
   * See {@link ClientProtocol#concat(String, String[])}.
   */
  public void concat(final String trg, final String[] srcs) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.concat(trg, srcs);
          return null;
        }
      };
      doClientActionWithRetry(handler, "concat");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Rename file or directory.
   *
   * @see ClientProtocol#rename2(String, String, Options.Rename...)
   */
  public void rename(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.rename2(src, dst, options);
          return null;
        }
      };
      doClientActionWithRetry(handler, "rename");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          DSQuotaExceededException.class, FileAlreadyExistsException.class,
          FileNotFoundException.class, ParentNotDirectoryException.class,
          SafeModeException.class, NSQuotaExceededException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String, boolean)}.
   */
  @Deprecated
  public boolean delete(final String src) throws IOException {
    checkOpen();
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.delete(src, true);
      }
    };

    if(getServerDefaults().getQuotaEnabled()){
      return (Boolean) doClientActionOnLeader(handler, "delete");
    } else {
      return (Boolean) doClientActionWithRetry(handler, "delete");
    }
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive
   * set to true
   *
   * @see ClientProtocol#delete(String, boolean)
   */
  public boolean delete(final String src, final boolean recursive)
      throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.delete(src, recursive);
        }
      };
      if(getServerDefaults().getQuotaEnabled()){
        return (Boolean) doClientActionOnLeader(handler, "delete");
      } else {
        return (Boolean) doClientActionWithRetry(handler, "delete");
      }
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          UnresolvedPathException.class);
    }
  }
  
  /**
   * Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /**
   * Get a partial listing of the indicated directory
   * No block locations need to be fetched
   */
  public DirectoryListing listPaths(String src, byte[] startAfter)
      throws IOException {
    return listPaths(src, startAfter, false);
  }
  
  /**
   * Get a partial listing of the indicated directory
   * <p/>
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @see ClientProtocol#getListing(String, byte[], boolean)
   */
  public DirectoryListing listPaths(final String src, final byte[] startAfter,
      final boolean needLocation) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getListing(src, startAfter, needLocation);
        }
      };
      return (DirectoryListing) doClientActionWithRetry(handler, "listPaths");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory.
   *
   * @param src
   *     The string representation of the path to the file
   * @return object containing information regarding the file
   * or null if file not found
   * @see ClientProtocol#getFileInfo(String) for description of exceptions
   */
  public HdfsFileStatus getFileInfo(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getFileInfo(src);
        }
      };
      return (HdfsFileStatus) doClientActionWithRetry(handler, "getFileInfo");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * Close status of a file
   * @return true if file is already closed
   */
  public boolean isFileClosed(final String src) throws IOException{
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.isFileClosed(src);
        }
      };
      return (boolean) doClientActionWithRetry(handler, "isFileClosed");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory. If src
   * refers to a symlink then the FileStatus of the link is returned.
   *
   * @param src
   *     path to a file or directory.
   *     <p/>
   *     For description of exceptions thrown
   * @see ClientProtocol#getFileLinkInfo(String)
   */
  public HdfsFileStatus getFileLinkInfo(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getFileLinkInfo(src);
        }
      };
      return (HdfsFileStatus) doClientActionWithRetry(handler,
          "getFileLinkInfo");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Get the checksum of a file.
   *
   * @param src
   *     The file path
   * @return The checksum
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  public MD5MD5CRC32FileChecksum getFileChecksum(String src)
      throws IOException {
    checkOpen();
    return getFileChecksum(src, clientName, /*HOPnamenode*/
        namenodeSelector.getNextNamenode().getRPCHandle(), socketFactory,
        dfsClientConf.socketTimeout, getDataEncryptionKey(),
        dfsClientConf.connectToDnViaHostname);
  }
  
  @InterfaceAudience.Private
  public void clearDataEncryptionKey() {
    LOG.debug("Clearing encryption key");
    synchronized (this) {
      encryptionKey = null;
    }
  }
  
  /**
   * @return true if data sent between this client and DNs should be encrypted,
   * false otherwise.
   * @throws IOException
   *     in the event of error communicating with the NN
   */
  boolean shouldEncryptData() throws IOException {
    FsServerDefaults d = getServerDefaults();
    return d == null ? false : d.getEncryptDataTransfer();
  }
  
  @InterfaceAudience.Private
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    if (shouldEncryptData()) {
      synchronized (this) {
        if (encryptionKey == null || encryptionKey.expiryDate < Time.now()) {
          LOG.debug("Getting new encryption token from NN");
          ClientActionHandler handler = new ClientActionHandler() {
            @Override
            public Object doAction(ClientProtocol namenode)
                throws RemoteException, IOException {
              return namenode.getDataEncryptionKey();
            }
          };
          encryptionKey = (DataEncryptionKey) doClientActionWithRetry(handler,
              "getDataEncryptionKey");
        }
        return encryptionKey;
      }
    } else {
      return null;
    }
  }

  /**
   * Get the checksum of a file.
   *
   * @param src
   *     The file path
   * @param clientName
   *     the name of the client requesting the checksum.
   * @param namenode
   *     the RPC proxy for the namenode
   * @param socketFactory
   *     to create sockets to connect to DNs
   * @param socketTimeout
   *     timeout to use when connecting and waiting for a response
   * @param encryptionKey
   *     the key needed to communicate with DNs in this cluster
   * @param connectToDnViaHostname whether the client should use hostnames instead of IPs
   * @return The checksum
   */
  private static MD5MD5CRC32FileChecksum getFileChecksum(String src, String clientName,
      ClientProtocol namenode, SocketFactory socketFactory, int socketTimeout,
      DataEncryptionKey encryptionKey, boolean connectToDnViaHostname)
      throws IOException {
    LOG.debug("SocketFactory is: " + socketFactory.getClass().getName());
    //get all block locations
    LocatedBlocks blockLocations =
        callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE);
    if (null == blockLocations) {
      throw new FileNotFoundException("File does not exist: " + src);
    }
    List<LocatedBlock> locatedblocks = blockLocations.getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = -1;
    DataChecksum.Type crcType = DataChecksum.Type.DEFAULT;
    long crcPerBlock = 0;
    boolean refetchBlocks = false;
    int lastRetriedIndex = -1;

    //get block checksum for each block
    for (int i = 0; i < locatedblocks.size(); i++) {
      if (refetchBlocks) {  // refetch to get fresh tokens
        blockLocations =
            callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE);
        if (null == blockLocations) {
          throw new FileNotFoundException("File does not exist: " + src);
        }
        locatedblocks = blockLocations.getLocatedBlocks();
        refetchBlocks = false;
      }
      LocatedBlock lb = locatedblocks.get(i);
      final ExtendedBlock block = lb.getBlock();
      final DatanodeInfo[] datanodes = lb.getLocations();
      
      //try each datanode location of the block
      final int timeout = 3000 * datanodes.length + socketTimeout;
      boolean done = false;
      for (int j = 0; !done && j < datanodes.length; j++) {
        DataOutputStream out = null;
        DataInputStream in = null;
        
        try {
          //connect to a datanode
          IOStreamPair pair =
              connectToDN(socketFactory, connectToDnViaHostname, encryptionKey,
                  datanodes[j], timeout);
          out = new DataOutputStream(new BufferedOutputStream(pair.out,
              HdfsConstants.SMALL_BUFFER_SIZE));
          in = new DataInputStream(pair.in);

          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j] + ": " + Op.BLOCK_CHECKSUM +
                ", block=" + block);
          }
          // get block MD5
          new Sender(out).blockChecksum(block, lb.getBlockToken());

          final BlockOpResponseProto reply =
              BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));

          if (reply.getStatus() != Status.SUCCESS) {
            if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException();
            } else {
              throw new IOException(
                  "Bad response " + reply + " for block " + block +
                      " from datanode " + datanodes[j]);
            }
          }
          
          OpBlockChecksumResponseProto checksumData =
              reply.getChecksumResponse();

          //read byte-per-checksum
          final int bpc = checksumData.getBytesPerCrc();
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          } else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc +
                " but bytesPerCRC=" + bytesPerCRC);
          }
          
          //read crc-per-block
          final long cpb = checksumData.getCrcPerBlock();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = new MD5Hash(checksumData.getMd5().toByteArray());
          md5.write(md5out);
          
          // read crc-type
          final DataChecksum.Type ct;
          if (checksumData.hasCrcType()) {
            ct = PBHelper.convert(checksumData.getCrcType());
          } else {
            LOG.debug("Retrieving checksum from an earlier-version DataNode: " +
                "inferring checksum by reading first byte");
            ct = inferChecksumTypeByReading(clientName, socketFactory,
                socketTimeout, lb, datanodes[j], encryptionKey,
                connectToDnViaHostname);
          }

          if (i == 0) { // first block
            crcType = ct;
          } else if (crcType != DataChecksum.Type.MIXED && crcType != ct) {
            // if crc types are mixed in a file
            crcType = DataChecksum.Type.MIXED;
          }

          done = true;

          if (LOG.isDebugEnabled()) {
            if (i == 0) {
              LOG.debug("set bytesPerCRC=" + bytesPerCRC + ", crcPerBlock=" +
                  crcPerBlock);
            }
            LOG.debug("got reply from " + datanodes[j] + ": md5=" + md5);
          }
        } catch (InvalidBlockTokenException ibte) {
          if (i > lastRetriedIndex) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  "Got access token error in response to OP_BLOCK_CHECKSUM " +
                      "for file " + src + " for block " + block +
                      " from datanode " + datanodes[j] +
                      ". Will retry the block once.");
            }
            lastRetriedIndex = i;
            done = true; // actually it's not done; but we'll retry
            i--; // repeat at i-th block
            refetchBlocks = true;
            break;
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes[" + j + "]=" + datanodes[j], ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
        }
      }

      if (!done) {
        throw new IOException("Fail to get block MD5 for " + block);
      }
    }

    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData());
    switch (crcType) {
      case CRC32:
        return new MD5MD5CRC32GzipFileChecksum(bytesPerCRC, crcPerBlock,
            fileMD5);
      case CRC32C:
        return new MD5MD5CRC32CastagnoliFileChecksum(bytesPerCRC, crcPerBlock,
            fileMD5);
      default:
        // If there is no block allocated for the file,
        // return one with the magic entry that matches what previous
        // hdfs versions return.
        if (locatedblocks.size() == 0) {
          return new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
        }

        // we should never get here since the validity was checked
        // when getCrcType() was called above.
        return null;
    }
  }

  /**
   * Connect to the given datanode's datantrasfer port, and return
   * the resulting IOStreamPair. This includes encryption wrapping, etc.
   */
  private static IOStreamPair connectToDN(SocketFactory socketFactory,
      boolean connectToDnViaHostname, DataEncryptionKey encryptionKey,
      DatanodeInfo dn, int timeout) throws IOException {
    boolean success = false;
    Socket sock = null;
    try {
      sock = socketFactory.createSocket();
      String dnAddr = dn.getXferAddr(connectToDnViaHostname);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to datanode " + dnAddr);
      }
      NetUtils.connect(sock, NetUtils.createSocketAddr(dnAddr), timeout);
      sock.setSoTimeout(timeout);

      OutputStream unbufOut = NetUtils.getOutputStream(sock);
      InputStream unbufIn = NetUtils.getInputStream(sock);
      IOStreamPair ret;
      if (encryptionKey != null) {
        ret = DataTransferEncryptor
            .getEncryptedStreams(unbufOut, unbufIn, encryptionKey);
      } else {
        ret = new IOStreamPair(unbufIn, unbufOut);
      }
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeSocket(sock);
      }
    }
  }
  
  /**
   * Infer the checksum type for a replica by sending an OP_READ_BLOCK
   * for the first byte of that replica. This is used for compatibility
   * with older HDFS versions which did not include the checksum type in
   * OpBlockChecksumResponseProto.
   * @return the inferred checksum type
   */
  private static Type inferChecksumTypeByReading(String clientName,
      SocketFactory socketFactory, int socketTimeout, LocatedBlock lb,
      DatanodeInfo dn, DataEncryptionKey encryptionKey,
      boolean connectToDnViaHostname) throws IOException {
    IOStreamPair pair =
        connectToDN(socketFactory, connectToDnViaHostname, encryptionKey, dn,
            socketTimeout);

    try {
      DataOutputStream out = new DataOutputStream(
          new BufferedOutputStream(pair.out, HdfsConstants.SMALL_BUFFER_SIZE));
      DataInputStream in = new DataInputStream(pair.in);

      new Sender(out)
          .readBlock(lb.getBlock(), lb.getBlockToken(), clientName, 0, 1, true, CachingStrategy.newDefaultStrategy());
      final BlockOpResponseProto reply =
          BlockOpResponseProto.parseFrom(PBHelper.vintPrefixed(in));
      
      if (reply.getStatus() != Status.SUCCESS) {
        if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN) {
          throw new InvalidBlockTokenException();
        } else {
          throw new IOException(
              "Bad response " + reply + " trying to read " + lb.getBlock() +
                  " from datanode " + dn);
        }
      }
      
      return PBHelper
          .convert(reply.getReadOpChecksumInfo().getChecksum().getType());
    } finally {
      IOUtils.cleanup(null, pair.in, pair.out);
    }
  }

  /**
   * Set permissions to a file or directory.
   *
   * @param src
   *     path name.
   * @param permission
   * @see ClientProtocol#setPermission(String, FsPermission)
   */
  public void setPermission(final String src, final FsPermission permission)
      throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setPermission(src, permission);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setPermission");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * Set file or directory owner.
   *
   * @param src
   *     path name.
   * @param username
   *     user id.
   * @param groupname
   *     user group.
   * @see ClientProtocol#setOwner(String, String, String)
   */
  public void setOwner(final String src, final String username,
      final String groupname) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setOwner(src, username, groupname);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setOwner");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, SafeModeException.class,
          UnresolvedPathException.class);
    }
  }

  /**
   * @see ClientProtocol#getStats()
   */
  public FsStatus getDiskStatus() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getStats();
      }
    };
    long rawNums[] = (long[]) doClientActionWithRetry(handler, "getDiskStatus");
    return new FsStatus(rawNums[0], rawNums[1], rawNums[2]);
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   *
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
      }
    };
    return (Long) doClientActionWithRetry(handler, "getMissingBlocksCount");
  }
  
  /**
   * Returns count of blocks with one of more replica missing.
   *
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode
            .getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
      }
    };
    return (Long) doClientActionWithRetry(handler,
        "getUnderReplicatedBlocksCount");
  }
  
  /**
   * Returns count of blocks with at least one replica marked corrupt.
   *
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
      }
    };
    return (Long) doClientActionWithRetry(handler, "getCorruptBlocksCount");
  }
  
  /**
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  public CorruptFileBlocks listCorruptFileBlocks(final String path,
      final String cookie) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.listCorruptFileBlocks(path, cookie);
      }
    };
    return (CorruptFileBlocks) doClientActionWithRetry(handler,
        "listCorruptFileBlocks");
  }

  public DatanodeInfo[] datanodeReport(final DatanodeReportType type)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getDatanodeReport(type);
      }
    };
    return (DatanodeInfo[]) doClientActionWithRetry(handler, "datanodeReport");
  }

  /**
   * Enter, leave or get safe mode.
   *
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return setSafeMode(action, false);
  }
  
  /**
   * Enter, leave or get safe mode.
   *
   * @param action
   *     One of SafeModeAction.GET, SafeModeAction.ENTER and
   *     SafeModeActiob.LEAVE
   * @param isChecked
   *     If true, then check only active namenode's safemode status, else
   *     check first namenode's status.
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public boolean setSafeMode(final SafeModeAction action,
      final boolean isChecked) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.setSafeMode(action, isChecked);
      }
    };
    return (Boolean) doClientActionWithRetry(handler, "setSafeMode");
  }
  
  @VisibleForTesting
  ExtendedBlock getPreviousBlock(String file) {
    return filesBeingWritten.get(file).getBlock();
  }

  /**
   * Refresh the hosts and exclude files.  (Rereads them.)
   * See {@link ClientProtocol#refreshNodes()}
   * for more details.
   *
   * @see ClientProtocol#refreshNodes()
   */
  public void refreshNodes() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.refreshNodes();
        return null;
      }
    };
    doClientActionWithRetry(handler, "refreshNodes");
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.balance.bandwidthPerSec.
   * See {@link ClientProtocol#setBalancerBandwidth(long)}
   * for more details.
   *
   * @see ClientProtocol#setBalancerBandwidth(long)
   */
  public void setBalancerBandwidth(final long bandwidth) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.setBalancerBandwidth(bandwidth);
        return null;
      }
    };
    doClientActionWithRetry(handler, "setBalancerBandwidth");
  }

  /**
   */
  @Deprecated
  public boolean mkdirs(String src) throws IOException {
    return mkdirs(src, null, true);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src
   *     The path of the directory being created
   * @param permission
   *     The permission of the directory being created.
   *     If permission == null, use {@link FsPermission#getDefault()}.
   * @param createParent
   *     create missing parent directory if true
   * @return True if the operation success.
   * @see ClientProtocol#mkdirs(String, FsPermission, boolean)
   */
  public boolean mkdirs(String src, FsPermission permission,
      boolean createParent) throws IOException {
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    return primitiveMkdir(src, masked, createParent);
  }

  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission)
      throws IOException {
    return primitiveMkdir(src, absPermission, true);
  }

  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   */
  public boolean primitiveMkdir(final String src, FsPermission absPermission,
      final boolean createParent) throws IOException {
    checkOpen();
    final FsPermission finalPermission;
    if (absPermission == null) {
      finalPermission =
          FsPermission.getDefault().applyUMask(dfsClientConf.uMask);
    } else {
      finalPermission = absPermission;
    }


    if (LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + finalPermission);
    }
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.mkdirs(src, finalPermission, createParent);
        }
      };
      return (Boolean) doClientActionWithRetry(handler, "primitiveMkdir");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          InvalidPathException.class, FileAlreadyExistsException.class,
          FileNotFoundException.class, ParentNotDirectoryException.class,
          SafeModeException.class, NSQuotaExceededException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }
  
  /**
   * Get {@link ContentSummary} rooted at the specified directory.
   *
   * @param src
   *     The string representation of the path
   * @see ClientProtocol#getContentSummary(String)
   */
  ContentSummary getContentSummary(final String src) throws IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getContentSummary(src);
        }
      };
      return (ContentSummary) doClientActionWithRetry(handler,
          "getContentSummary");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   *
   * @see ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(final String src, final long namespaceQuota,
      final long diskspaceQuota) throws IOException {
    // sanity check
    if ((namespaceQuota <= 0 &&
        namespaceQuota != HdfsConstants.QUOTA_DONT_SET &&
        namespaceQuota != HdfsConstants.QUOTA_RESET) || (diskspaceQuota <= 0 &&
        diskspaceQuota != HdfsConstants.QUOTA_DONT_SET &&
        diskspaceQuota != HdfsConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Invalid values for quota : " +
          namespaceQuota + " and " +
          diskspaceQuota);

    }
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setQuota(src, namespaceQuota, diskspaceQuota);
          return null;
        }
      };
      doClientActionOnLeader(handler, "setQuota");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, NSQuotaExceededException.class,
          DSQuotaExceededException.class, UnresolvedPathException.class);
    }
  }

  /**
   * set the modification and access time of a file
   *
   * @see ClientProtocol#setTimes(String, long, long)
   */
  public void setTimes(final String src, final long mtime, final long atime)
      throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          namenode.setTimes(src, mtime, atime);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setTimes");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  /**
   * @deprecated use {@link HdfsDataInputStream} instead.
   */
  @Deprecated
  public static class DFSDataInputStream extends HdfsDataInputStream {

    public DFSDataInputStream(DFSInputStream in) throws IOException {
      super(in);
    }
  }
  
  void reportChecksumFailure(String file, ExtendedBlock blk, DatanodeInfo dn) {
    DatanodeInfo[] dnArr = {dn};
    LocatedBlock[] lblocks = {new LocatedBlock(blk, dnArr)};
    reportChecksumFailure(file, lblocks);
  }

  // just reports checksum failure and ignores any exception during the report.
  void reportChecksumFailure(String file, LocatedBlock lblocks[]) {
    try {
      reportBadBlocks(lblocks);
    } catch (IOException ie) {
      LOG.info("Found corruption while reading " + file +
          ". Error repairing corrupt blocks. Bad blocks remain.", ie);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName + ", ugi=" +
        ugi + "]";
  }

  public DomainSocketFactory getDomainSocketFactory() {
    return domainSocketFactory;
  }

  public void disableLegacyBlockReaderLocal() {
    shouldUseLegacyBlockReaderLocal = false;
  }

  public boolean useLegacyBlockReaderLocal() {
    return shouldUseLegacyBlockReaderLocal;
  }

  /**
   * Action Handler to encapsualte the client requests to the namenode.
   */
  private interface ClientActionHandler {
    Object doAction(ClientProtocol namenode)
        throws RemoteException, IOException;
  }

  /**
   * Return the next namenode to be used by the client. It also make sure
   * that this namenode doesn't belong to the blacklisted namenodes. A
   * namenode is blacklisted if it doesn't respond to a request
   * for @link{MAX_RPC_RETRIES} times.
   */
  private interface NameNodeFetcher {
    public NamenodeHandle getNextNameNode(List<ActiveNode> blackList)
        throws IOException;
  }

  /**
   * Default NameNodeFetcher which returns a namenode from the active
   * namenode list. It delegates the process of selecting nextNamenode to the
   * NameNodeSelector according to the specified policy in the selector.
   * @see NamenodeSelector#getNextNamenode()
   */
  private final NameNodeFetcher defaultNameNodeFetcher = new NameNodeFetcher() {
    public NamenodeHandle getNextNameNode(List<ActiveNode> blackList)
        throws IOException {
      NamenodeSelector.NamenodeHandle handle = null;
      for (int i = 0; i < 10; i++) {
        handle = namenodeSelector.getNextNamenode();
        if (!blackList.contains(handle.getNamenode())) {
          return handle;
        }
      }
      return handle;
    }
  };

  /**
   * Returns the leader namenode if it's not blacklisted, otherwise it will
   * return null.
   */
  private final NameNodeFetcher leaderNameNodeFetcher = new NameNodeFetcher() {
    public NamenodeHandle getNextNameNode(List<ActiveNode> blackList)
        throws IOException {
      NamenodeHandle leader = namenodeSelector.getLeadingNameNode();
      if (blackList.contains(leader)) {
        return null;
      }
      return leader;
    }
  };

  /**
   * A client request encapsulated in @link{ClientActionHandler} run on the
   * leader namenode. If the client failed to connect to the namenode, it will
   * keep retying for @link{MAX_RPC_RETRIES} untill either it succeeds or fails.
   * @param handler
   *      encapsualted client request
   * @param callerID
   *      requested operation
   * @return Object result of the operation is any, otherwise return null
   * @throws RemoteException
   * @throws IOException
   */
  private Object doClientActionOnLeader(ClientActionHandler handler,
      String callerID) throws RemoteException, IOException {
    return doClientActionWithRetry(handler, callerID, leaderNameNodeFetcher);
  }

  /**
   * A client request encapsulated in @link{ClientActionHandler} run on a
   * namenode. If the client failed to connect to the namenode, it will
   * keep retying for @link{MAX_RPC_RETRIES} untill either it succeeds or fails.
   * @param handler
   *      encapsualted client request
   * @param callerID
   *      requested operation
   * @return Object result of the operation is any, otherwise return null
   * @throws RemoteException
   * @throws IOException
   */
  private Object doClientActionWithRetry(ClientActionHandler handler,
      String callerID) throws RemoteException, IOException {
    return doClientActionWithRetry(handler, callerID, defaultNameNodeFetcher);
  }

  private static AtomicLong fnID = new AtomicLong(); // for debugging purpose
  
  /**
   * A client request encapsulated in @link{ClientActionHandler} will run on
   * a namenode specified by the nameNodeFetcher. If the client failed to
   * connect to the namenode, it will keep retying for @link{MAX_RPC_RETRIES}
   * times untill either it succeeds or fails. If the request failed, the
   * namenode will be blacklisted. @link{NamenodeSector} will handle the
   * revocation of the blacklisted namenodes.
   * @param handler
   *      encapsualted client request
   * @param callerID
   *      requested operation
   * @param nameNodeFetcher
   *      fetcher used to get namenode to be used for the request
   * @return Object result of the operation is any, otherwise return null
   * @throws RemoteException
   * @throws IOException
   */
  private Object doClientActionWithRetry(ClientActionHandler handler,
      String callerID, NameNodeFetcher nameNodeFetcher)
      throws RemoteException, IOException {
    callerID = callerID.toUpperCase();
    long thisFnID = fnID.incrementAndGet();
    //When a RPC call to NN fails then the client will put the NamenodeSelector.java
    //will put the NN address in blacklist and send an RPC to NN to get a fresh list of NNs.
    //After obtaining a fresh list from server, the NamenodeSector will wipe the NN balacklist.
    //It is quite possible that the refesh list of NNs may again contain the descriptors
    //for dead Namenodes (depends on the convergence rate of Leader Election).
    //To avoid contacting a dead node a list of black listed namenodes is also maintained on the
    //client side to avoid contacting dead NNs
    List<ActiveNode> blackListedNamenodes = new ArrayList<>();

    IOException exception = null;
    NamenodeSelector.NamenodeHandle handle = null;
    int waitTime = dfsClientConf.dfsClientInitialWaitOnRetry;
    for (int i = 0; i <= MAX_RPC_RETRIES;
         i++) { // min value of MAX_RPC_RETRIES is 0
      try {
        handle = nameNodeFetcher.getNextNameNode(blackListedNamenodes);

        LOG.debug(thisFnID + ") " + callerID + " sending RPC to " +
            handle.getNamenode() + " tries left (" + (MAX_RPC_RETRIES - i) +
            ")");
        Object obj = handler.doAction(handle.getRPCHandle());
        //no exception
        return obj;
      } catch (IOException e) {
        exception = e;
        if (ExceptionCheck.isLocalConnectException(e)) {
          //black list the namenode
          //so that it is not used again
          if (handle != null) {
            LOG.debug(thisFnID + ") " + callerID + " RPC failed. NN used was " +
                handle.getNamenode() + ", retries left (" +
                (MAX_RPC_RETRIES - (i)) + ")", e);
            namenodeSelector.blackListNamenode(handle);
            blackListedNamenodes.add(handle.getNamenode());
          } else {
            LOG.debug(thisFnID + ") " + callerID +
                " RPC failed. NN was NULL, retries left (" +
                (MAX_RPC_RETRIES - (i)) + ")", e);
          }

          try {
            LOG.debug(thisFnID + ") RPC failed. Sleep " + waitTime);
            Thread.sleep(waitTime);
            waitTime *= 2;
          } catch (InterruptedException ex) {
          }
          continue;
        } else if(getWrappedRetriableException(e) != null) {
          try {
            Thread.sleep(waitTime);
            waitTime *= 2;
          } catch (InterruptedException ex) {
          }
        } else {
          break;
        }
      }
    }
    LOG.warn(thisFnID + ") " + callerID + " RPC failed", exception);
    throw exception; // Did not return so RPC failed
  }

  private static RetriableException getWrappedRetriableException(Exception e) {
    if (!(e instanceof RemoteException)) {
      return null;
    }
    Exception unwrapped = ((RemoteException)e).unwrapRemoteException(
        RetriableException.class);
    return unwrapped instanceof RetriableException ? 
        (RetriableException) unwrapped : null;
  }
  
  /**
   * Ping the name node to see if there is a connection
   * -- A connection won't exist if it gives an IOException of type
   * ConnectException or SocketTimeout
   */
  public boolean pingNamenode() {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.ping();
          return null;
        }
      };
      doClientActionWithRetry(handler, "pingNamenode");
    } catch (Exception ex) {
      ex.printStackTrace();
      LOG.warn("Ping to Namenode " + ex);
      return false;
    }
    // There is a connection
    return true;
  }

  public SortedActiveNodeList getActiveNodes() throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getActiveNamenodesForClient();
      }
    };
    return (SortedActiveNodeList) doClientActionWithRetry(handler,
        "getActiveNodes");
  }

  public LocatedBlock getAdditionalDatanode(final String src,
      final ExtendedBlock blk, final DatanodeInfo[] existings,
      final String[] existingStorages, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.getAdditionalDatanode(src, blk, existings, existingStorages,
            excludes, numAdditionalNodes, clientName);
      }
    };
    return (LocatedBlock) doClientActionWithRetry(handler,
        "getAdditionalDatanode");
  }

  public LocatedBlock updateBlockForPipeline(final ExtendedBlock block,
      final String clientName) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.updateBlockForPipeline(block, clientName);
      }
    };
    return (LocatedBlock) doClientActionWithRetry(handler,
        "updateBlockForPipeline");
  }

  public void updatePipeline(final String clientName,
      final ExtendedBlock oldBlock, final ExtendedBlock newBlock,
      final DatanodeID[] newNodes, final String[] newStorages) throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.updatePipeline(clientName, oldBlock, newBlock, newNodes,
            newStorages);
        return null;
      }
    };
    doClientActionWithRetry(handler, "updatePipeline");
  }

  public void abandonBlock(final ExtendedBlock b, final String src,
      final String holder) throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.abandonBlock(b, src, holder);
        return null;
      }
    };
    doClientActionWithRetry(handler, "abandonBlock");
  }

  public LocatedBlock addBlock(final String src, final String clientName,
      final ExtendedBlock previous, final DatanodeInfo[] excludeNodes, final long fileId, final String[] favoredNodes)
      throws AccessControlException, FileNotFoundException,
      NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.addBlock(src, clientName, previous, excludeNodes, fileId, favoredNodes);
      }
    };
    return (LocatedBlock) doClientActionWithRetry(handler, "addBlock");
  }

  public HdfsFileStatus create(final String src, final FsPermission masked,
      final String clientName, final EnumSetWritable<CreateFlag> flag,
      final boolean createParent, final short replication, final long blockSize)
      throws AccessControlException, AlreadyBeingCreatedException,
      DSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode
            .create(src, masked, clientName, flag, createParent, replication,
                blockSize);
        return null;
      }
    };
    return (HdfsFileStatus) doClientActionWithRetry(handler, "create");
  }

  public HdfsFileStatus create(final String src, final FsPermission masked,
      final String clientName, final EnumSetWritable<CreateFlag> flag,
      final boolean createParent, final short replication, final long blockSize,
      final EncodingPolicy policy)
      throws AccessControlException, AlreadyBeingCreatedException,
      DSQuotaExceededException, FileAlreadyExistsException,
      FileNotFoundException, NSQuotaExceededException,
      ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
      IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode
            .create(src, masked, clientName, flag, createParent, replication,
                blockSize, policy);
      }
    };
    return (HdfsFileStatus) doClientActionWithRetry(handler, "create");
  }

  public void fsync(final String src, final String client,
      final long lastBlockLength)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.fsync(src, client, lastBlockLength);
        return null;
      }
    };
    doClientActionWithRetry(handler, "fsync");
  }

  public boolean complete(final String src, final String clientName,
      final ExtendedBlock last, final long fileId, final byte[] data)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return namenode.complete(src, clientName, last, fileId, data);
      }
    };
    return (Boolean) doClientActionWithRetry(handler, "complete");
  }

  public EncodingStatus getEncodingStatus(final String filePath)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode) throws IOException {
        try {
          return namenode.getEncodingStatus(filePath);
        } catch (RemoteException e) {
          throw e.unwrapRemoteException();
        }
      }
    };
    return (EncodingStatus) doClientActionWithRetry(handler, "EncodingStatus");
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#encodeFile
   */
  public void encodeFile(final String filePath, final EncodingPolicy policy)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode) throws IOException {
        namenode.encodeFile(filePath, policy);
        return null;
      }
    };
    doClientActionWithRetry(handler, "encodeFile");
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#revokeEncoding
   */
  public void revokeEncoding(final String filePath, final short replication)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode) throws IOException {
        namenode.revokeEncoding(filePath, replication);
        return null;
      }
    };
    doClientActionWithRetry(handler, "revokeEncoding");
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getRepairedBlockLocations
   */
  public LocatedBlock getRepairedBlockLocations(final String sourcePath,
      final String parityPath, final LocatedBlock block, final boolean isParity)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {
      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        return callGetRepairedBlockLocations(namenode, sourcePath, parityPath,
            block, isParity);
      }
    };
    return (LocatedBlock) doClientActionWithRetry(handler,
        "getMissingLocatedBlocks");
  }

  static LocatedBlock callGetRepairedBlockLocations(ClientProtocol namenode,
      String sourcePath, String parityPath, LocatedBlock block,
      boolean isParity) throws IOException {
    try {
      return namenode
          .getRepairedBlockLocations(sourcePath, parityPath, block, isParity);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  public void changeConf(final List<String> props, final List<String> newVals)
      throws IOException {
    ClientActionHandler handler = new ClientActionHandler() {

      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.changeConf(props, newVals);
        return null;
      }
    };
    doClientActionToAll(handler, "changeConf");
  }

  public void flushCache(final String userName, final String groupName)
      throws IOException{
    ClientActionHandler handler = new ClientActionHandler() {

      @Override
      public Object doAction(ClientProtocol namenode)
          throws RemoteException, IOException {
        namenode.flushCache(userName, groupName);
        return null;
      }
    };
    doClientActionToAll(handler, "flushCache");
  }

  public void checkAccess(final String src, final FsAction mode)
      throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.checkAccess(src, mode);
          return null;
        }
      };
      doClientActionWithRetry(handler, "checkAccess");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class,
          UnresolvedPathException.class);
    }
  }

  void modifyAclEntries(final String src, final List<AclEntry> aclSpec)
          throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.modifyAclEntries(src, aclSpec);
          return null;
        }
      };
      doClientActionWithRetry(handler, "modifyAclEntries");
    } catch(RemoteException re) {
          throw re.unwrapRemoteException(AccessControlException.class,
                  AclException.class,
                  FileNotFoundException.class,
                  NSQuotaExceededException.class,
                  SafeModeException.class,
                  UnresolvedPathException.class);
    }
  }

  void removeAclEntries(final String src, final List<AclEntry> aclSpec)
          throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.removeAclEntries(src, aclSpec);
          return null;
        }
      };
      doClientActionWithRetry(handler, "removeAclEntries");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              AclException.class,
              FileNotFoundException.class,
              NSQuotaExceededException.class,
              SafeModeException.class,
              UnresolvedPathException.class);
    }
  }

  void removeDefaultAcl(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.removeDefaultAcl(src);
          return null;
        }
      };
      doClientActionWithRetry(handler, "removeDefaultAcl");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              AclException.class,
              FileNotFoundException.class,
              NSQuotaExceededException.class,
              SafeModeException.class,
              UnresolvedPathException.class);
    }
  }

  void removeAcl(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.removeAcl(src);
          return null;
        }
      };
      doClientActionWithRetry(handler, "removeAcl");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              AclException.class,
              FileNotFoundException.class,
              NSQuotaExceededException.class,
              SafeModeException.class,
              UnresolvedPathException.class);
    }
  }

  void setAcl(final String src, final List<AclEntry> aclSpec) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          namenode.setAcl(src, aclSpec);
          return null;
        }
      };
      doClientActionWithRetry(handler, "setAcl");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              AclException.class,
              FileNotFoundException.class,
              NSQuotaExceededException.class,
              SafeModeException.class,
              UnresolvedPathException.class);
    }
  }

  AclStatus getAclStatus(final String src) throws IOException {
    checkOpen();
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode) throws IOException {
          return namenode.getAclStatus(src);
        }
      };
      return (AclStatus) doClientActionWithRetry(handler, "getAclStatus");
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
              AclException.class,
              FileNotFoundException.class,
              UnresolvedPathException.class);
    }
  }


  /**
   * Send the client request to all namenodes in the cluster
   * @param handler
   *      encapsulated client request
   * @param callerID
   *      requested operation
   * @return Object result of the operation if any, otherwise return null
   * @throws RemoteException
   * @throws IOException
   */
  private Object doClientActionToAll(ClientActionHandler handler,
      String callerID) throws RemoteException, IOException {
    callerID = callerID.toUpperCase();
    long thisFnID = fnID.incrementAndGet();

    Exception exception = null;
    boolean success = false;
    for (NamenodeSelector.NamenodeHandle handle : namenodeSelector
        .getAllNameNode()) {
      try {
        LOG.debug(thisFnID + ") " + callerID + " sending RPC to " +
            handle.getNamenode());
        Object obj = handler.doAction(handle.getRPCHandle());
        success = true;
        //no exception
        return obj;
      } catch (Exception e) {
        exception = e;
        if (ExceptionCheck.isLocalConnectException(e)) {
          //black list the namenode
          //so that it is not used again
          if (handle != null) {
            LOG.warn(thisFnID + ") " + callerID + " RPC faild. NN used was " +
                handle.getNamenode() + ", Exception " + e);
            namenodeSelector.blackListNamenode(handle);
          } else {
            LOG.warn(thisFnID + ") " + callerID +
                " RPC faild. NN was NULL,  Exception " + e);
          }
          continue;
        } else {
          break;
        }
      }
    }
    if (!success) {
      //print the fn call trace to figure out with RPC failed
      for (int j = 0; j < Thread.currentThread().getStackTrace().length; j++) {
        LOG.debug(thisFnID + ") " + callerID + " Failed RPC Trace, " +
            Thread.currentThread().getStackTrace()[j]);
      }

      LOG.warn(thisFnID + ") " + callerID + " Exception was " + exception);
      exception.printStackTrace();
      if (exception != null) {
        if (exception instanceof RemoteException) {
          throw (RemoteException) exception;
        } else {
          throw (IOException) exception;
        }
      }
    }
    return null;
  }

  public int getNameNodesCount() throws IOException {
    return namenodeSelector.getNameNodesCount();
  }

  /**
   * Get {@link ContentSummary} rooted at the specified directory.
   *
   * @param src
   *     The string representation of the path
   * @see ClientProtocol#getLastUpdatedContentSummary(String)
   */
  LastUpdatedContentSummary getLastUpdatedContentSummary(final String src) throws
      IOException {
    try {
      ClientActionHandler handler = new ClientActionHandler() {
        @Override
        public Object doAction(ClientProtocol namenode)
            throws RemoteException, IOException {
          return namenode.getLastUpdatedContentSummary(src);
        }
      };
      return (LastUpdatedContentSummary) doClientActionWithRetry(handler,
          "getLastUpdatedContentSummary");
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
          FileNotFoundException.class, UnresolvedPathException.class);
    }
  }

  public CachingStrategy getDefaultReadCachingStrategy() {
    return defaultReadCachingStrategy;
  }

  public CachingStrategy getDefaultWriteCachingStrategy() {
    return defaultWriteCachingStrategy;
  }
  
  @VisibleForTesting
  public ClientMmapManager getMmapManager() {
    return mmapManager;
  }
}
