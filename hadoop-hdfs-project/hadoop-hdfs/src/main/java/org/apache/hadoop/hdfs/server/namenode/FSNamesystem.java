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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.common.IDsGeneratorFactory;
import io.hops.common.IDsMonitor;
import io.hops.common.INodeUtil;
import io.hops.erasure_coding.Codec;
import io.hops.erasure_coding.ErasureCodingManager;
import io.hops.exception.*;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.CacheDirectiveDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.resolvingcache.Cache;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.RootINodeCache;
import io.hops.transaction.handler.EncodingStatusOperationType;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.*;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastUpdatedContentSummary;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.HashBuckets;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.top.TopAuditLogger;
import org.apache.hadoop.hdfs.server.namenode.top.TopConf;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.*;

import static io.hops.transaction.lock.LockFactory.BLK;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.util.Slicer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DIR_DELETE_BATCH_SIZE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DIR_DELETE_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_MAX_OBJECTS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUBTREE_EXECUTOR_LIMIT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUBTREE_EXECUTOR_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SUPPORT_APPEND_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCacheDistributed.CacheEntryWithPayload;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.util.Time.now;
import org.apache.hadoop.util.Timer;
import org.apache.log4j.Appender;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.Logger;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * ************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 * <p/>
 * It tracks several important tables.
 * <p/>
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 * *************************************************
 */
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSNamesystemMBean,
  NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  private static final ThreadLocal<StringBuilder> auditBuffer =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  @VisibleForTesting
  boolean isAuditEnabled() {
    return !isDefaultAuditLogger || auditLog.isInfoEnabled();
  }

  private HdfsFileStatus getAuditFileInfo(String path, boolean resolveSymlink)
      throws IOException {
    return dir.getAuditFileInfo(path, resolveSymlink);
  }

  private void logAuditEvent(boolean succeeded, String cmd, String src)
      throws IOException {
    logAuditEvent(succeeded, cmd, src, null, null);
  }

  private void logAuditEvent(boolean succeeded, String cmd, String src,
      String dst, HdfsFileStatus stat) throws IOException {
    if (isAuditEnabled() && isExternalInvocation()) {
      logAuditEvent(succeeded, getRemoteUser(), getRemoteIp(), cmd, src, dst,
          stat);
    }
  }

  private void logAuditEvent(boolean succeeded, UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      HdfsFileStatus stat) throws IOException {
    FileStatus status = null;
    if (stat != null) {
      Path symlink = stat.isSymlink() ? new Path(stat.getSymlink()) : null;
      Path path = dst != null ? new Path(dst) : new Path(src);
      status =
          new FileStatus(stat.getLen(), stat.isDir(), stat.getReplication(),
              stat.getBlockSize(), stat.getModificationTime(),
              stat.getAccessTime(), stat.getPermission(), stat.getOwner(),
              stat.getGroup(), symlink, path);
    }
    for (AuditLogger logger : auditLoggers) {
      if (logger instanceof HdfsAuditLogger) {
        HdfsAuditLogger hdfsLogger = (HdfsAuditLogger) logger;
        hdfsLogger.logAuditEvent(succeeded, ugi.toString(), addr, cmd, src, dst,
            status, ugi, dtSecretManager);
      } else {
        logger.logAuditEvent(succeeded, ugi.toString(), addr,
            cmd, src, dst, status);
      }
    }
  }

  /**
   * Logger for audit events, noting successful FSNamesystem operations. Emits
   * to FSNamesystem.audit at INFO. Each event causes a set of tab-separated
   * <code>key=value</code> pairs to be written for the following properties:
   * <code>
   * ugi=&lt;ugi in RPC&gt;
   * ip=&lt;remote IP&gt;
   * cmd=&lt;command&gt;
   * src=&lt;src path&gt;
   * dst=&lt;dst path (optional)&gt;
   * perm=&lt;permissions (optional)&gt;
   * </code>
   */
  public static final Log auditLog =
      LogFactory.getLog(FSNamesystem.class.getName() + ".audit");

  static final int DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED = 100;
  static int BLOCK_DELETION_INCREMENT = 1000;

  private final boolean isPermissionEnabled;
  private final UserGroupInformation fsOwner;
  private final String superGroup;

  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  private final DelegationTokenSecretManager dtSecretManager;
  private final boolean alwaysUseDelegationTokensForTests;

  private static final Step STEP_AWAITING_REPORTED_BLOCKS =
    new Step(StepType.AWAITING_REPORTED_BLOCKS);

  // Tracks whether the default audit logger is the only configured audit
  // logger; this allows isAuditEnabled() to return false in case the
  // underlying logger is disabled, and avoid some unnecessary work.
  private final boolean isDefaultAuditLogger;
  private final List<AuditLogger> auditLoggers;

  /**
   * The namespace tree.
   */
  FSDirectory dir;
  private final BlockManager blockManager;
  private final CacheManager cacheManager;
  private final DatanodeStatistics datanodeStatistics;

  // whether setStoragePolicy is allowed.
  private final boolean isStoragePolicyEnabled;

  // Block pool ID used by this namenode
  //HOP made it final and now its value is read from the config file. all
  // namenodes should have same block pool id
  private final String blockPoolId;


  final LeaseManager leaseManager = new LeaseManager(this);

  volatile private Daemon smmthread = null;  // SafeModeMonitor thread

  private Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  private Daemon retryCacheCleanerThread = null;

  private volatile boolean hasResourcesAvailable = true;
  private volatile boolean fsRunning = true;

  /**
   * The start time of the namesystem.
   */
  private final long startTime = now();

  /**
   * The interval of namenode checking for the disk space availability
   */
  private final long resourceRecheckInterval;

  // The actual resource checker instance.
  NameNodeResourceChecker nnResourceChecker;

  private final int maxDBTries;
  
  private final FsServerDefaults serverDefaults;
  private final boolean supportAppends;
  private final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

  private AtomicBoolean inSafeMode = new AtomicBoolean(false); // safe mode information
  
  private final long maxFsObjects;          // maximum number of fs objects

  private final long minBlockSize;         // minimum block size
  private final long maxBlocksPerFile;     // maximum # of blocks per file

  // precision of access times.
  private final long accessTimePrecision;

  private NameNode nameNode;
  private final Configuration conf;
  private final QuotaUpdateManager quotaUpdateManager;

  private final ExecutorService fsOperationsExecutor;
  private final boolean erasureCodingEnabled;
  private final ErasureCodingManager erasureCodingManager;

  private final boolean storeSmallFilesInDB;
  private static int DB_ON_DISK_FILE_MAX_SIZE;
  private static int DB_ON_DISK_SMALL_FILE_MAX_SIZE;
  private static int DB_ON_DISK_MEDIUM_FILE_MAX_SIZE;
  private static int DB_ON_DISK_LARGE_FILE_MAX_SIZE;
  private static int DB_IN_MEMORY_FILE_MAX_SIZE;
  private final long BIGGEST_DELETABLE_DIR;

  /** flag indicating whether replication queues have been initialized */
  boolean initializedReplQueues = false;
  
  boolean shouldPopulateReplicationQueue = false;
  
  /**
   * Whether the namenode is in the middle of starting the active service
   */
  private volatile boolean startingActiveService = false;

  private volatile boolean imageLoaded = false;
  
  private final AclConfigFlag aclConfigFlag;

  private final RetryCacheDistributed retryCache;
  private final boolean isRetryCacheEnabled;
  
  //Add delay for file system operations. Used only for testing
  private boolean isTestingSTO = false;
  private ThreadLocal<Times> delays = new ThreadLocal<Times>();
  long delayBeforeSTOFlag = 0; //This parameter can not be more than TxInactiveTimeout: 1.2 sec
  long delayAfterBuildingTree=0;

  int slicerBatchSize;
  int slicerNbThreads;
  
  private volatile AtomicBoolean forceReadTheSafeModeFromDB = new AtomicBoolean(true);
  
  /**
   * Notify that loading of this FSDirectory is complete, and
   * it is imageLoaded for use
   */
  void imageLoadComplete() {
    Preconditions.checkState(!imageLoaded, "FSDirectory already loaded");
    setImageLoaded();
  }
  void setImageLoaded() {
    if(imageLoaded) return;
      setImageLoaded(true);
      dir.markNameCacheInitialized();
  }
  //This is for testing purposes only
  @VisibleForTesting
  boolean isImageLoaded() {
    return imageLoaded;
  }
  // exposed for unit tests
  protected void setImageLoaded(boolean flag) {
    imageLoaded = flag;
  }
  
  /**
   * Clear all loaded data
   */
  void clear() throws IOException {
    dir.reset();
    dtSecretManager.reset();
    leaseManager.removeAllLeases();
    setImageLoaded(false);
  }

  @VisibleForTesting
  LeaseManager getLeaseManager() {
    return leaseManager;
  }

  /**
   * Instantiates an FSNamesystem loaded from the image and edits
   * directories specified in the passed Configuration.
   *
   * @param conf
   *     the Configuration which specifies the storage directories
   *     from which to load
   * @return an FSNamesystem which contains the loaded namespace
   * @throws IOException
   *     if loading fails
   */
  static FSNamesystem loadFromDisk(Configuration conf, NameNode namenode) throws IOException {

    FSNamesystem namesystem = new FSNamesystem(conf, namenode, false);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.RECOVER) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }
    
    if (RollingUpgradeStartupOption.ROLLBACK.matches(startOpt)) {
        rollBackRollingUpgradeTX();
    }
    
    long loadStart = now();
    switch(startOpt) {
    case UPGRADE:
      StorageInfo sinfo = StorageInfo.getStorageInfoFromDB();
      StorageInfo.updateStorageInfoToDB(sinfo, Time.now());
    case REGULAR:
    default:
      // just load the image
    }
    namesystem.imageLoadComplete();     //HOP: this function was called inside the  namesystem.loadFSImage(...) which is commented out

    long timeTakenToLoadFSImage = now() - loadStart;
    LOG.debug("Finished loading FSImage in " + timeTakenToLoadFSImage + " ms");
    NameNodeMetrics nnMetrics = NameNode.getNameNodeMetrics();
    if (nnMetrics != null) {
      nnMetrics.setFsImageLoadTime((int) timeTakenToLoadFSImage);
    }
    return namesystem;
  }

  FSNamesystem(Configuration conf, NameNode namenode) throws IOException {
    this(conf, namenode, false);
  }

  /**
   * Create an FSNamesystem.
   *
   * @param conf
   *     configuration
   * @param namenode
   *     the namenode
   * @param ignoreRetryCache Whether or not should ignore the retry cache setup
   *                         step. For Secondary NN this should be set to true.
   * @throws IOException
   *      on bad configuration
   */
  private FSNamesystem(Configuration conf, NameNode namenode, boolean ignoreRetryCache) throws IOException {
    try {
      if (conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY,
          DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT)) {
        LOG.info("Enabling async auditlog");
        enableAsyncAuditLog();
      }
      this.conf = conf;
      this.nameNode = namenode;
      resourceRecheckInterval =
          conf.getLong(DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
              DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

      this.blockManager = new BlockManager(this, conf);
      this.erasureCodingEnabled =
          ErasureCodingManager.isErasureCodingEnabled(conf);
      this.erasureCodingManager = new ErasureCodingManager(this, conf);
      this.storeSmallFilesInDB =
          conf.getBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
              DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_DEFAULT);
      DB_ON_DISK_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY,
              DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_DEFAULT);
      DB_ON_DISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY,
              DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      DB_ON_DISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY,
              DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      DB_ON_DISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY,
              DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      DB_IN_MEMORY_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY,
          DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);

      if (!(DB_IN_MEMORY_FILE_MAX_SIZE < DB_ON_DISK_SMALL_FILE_MAX_SIZE &&
      DB_ON_DISK_SMALL_FILE_MAX_SIZE < DB_ON_DISK_MEDIUM_FILE_MAX_SIZE &&
      DB_ON_DISK_MEDIUM_FILE_MAX_SIZE < DB_ON_DISK_LARGE_FILE_MAX_SIZE &&
      DB_ON_DISK_LARGE_FILE_MAX_SIZE == DB_ON_DISK_FILE_MAX_SIZE)){
        throw new IllegalArgumentException("The size for the database files is not correctly set");
      }

      this.datanodeStatistics =
          blockManager.getDatanodeManager().getDatanodeStatistics();

      this.isStoragePolicyEnabled =
          conf.getBoolean(DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY,
                          DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_DEFAULT);

      this.fsOwner = UserGroupInformation.getCurrentUser();
      this.superGroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
          DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
          DFS_PERMISSIONS_ENABLED_DEFAULT);

      blockPoolId = StorageInfo.getStorageInfoFromDB().getBlockPoolId();
      blockManager.setBlockPoolId(blockPoolId);
      hopSpecificInitialization(conf);
      this.quotaUpdateManager = new QuotaUpdateManager(this, conf);
      fsOperationsExecutor = Executors.newFixedThreadPool(
          conf.getInt(DFS_SUBTREE_EXECUTOR_LIMIT_KEY,
              DFS_SUBTREE_EXECUTOR_LIMIT_DEFAULT));
      BIGGEST_DELETABLE_DIR = conf.getLong(DFS_DIR_DELETE_BATCH_SIZE,
              DFS_DIR_DELETE_BATCH_SIZE_DEFAULT);

      LOG.info("fsOwner             = " + fsOwner);
      LOG.info("superGroup          = " + superGroup);
      LOG.info("isPermissionEnabled = " + isPermissionEnabled);

      // Get the checksum type from config
      String checksumTypeStr =
          conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
      DataChecksum.Type checksumType;
      try {
        checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
      } catch (IllegalArgumentException iae) {
        throw new IOException(
            "Invalid checksum type in " + DFS_CHECKSUM_TYPE_KEY + ": " +
                checksumTypeStr);
      }

      this.serverDefaults = new FsServerDefaults(
          conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
          conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
              DFS_BYTES_PER_CHECKSUM_DEFAULT),
          conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
              DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
          (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
          conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
          conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
              DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
          conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
          checksumType, conf.getBoolean(DFS_NAMENODE_QUOTA_ENABLED_KEY,
              DFS_NAMENODE_QUOTA_ENABLED_DEFAULT));

      this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY,
          DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

      this.minBlockSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT);
      this.maxBlocksPerFile = conf.getLong(DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY,
          DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT);
      this.accessTimePrecision =
          conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);
      this.supportAppends =
          conf.getBoolean(DFS_SUPPORT_APPEND_KEY, DFS_SUPPORT_APPEND_DEFAULT);
      LOG.info("Append Enabled: " + supportAppends);

      this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);


      // For testing purposes, allow the DT secret manager to be started regardless
      // of whether security is enabled.
      alwaysUseDelegationTokensForTests =
          conf.getBoolean(DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
              DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT);

      this.dtSecretManager = createDelegationTokenSecretManager(conf);
      this.dir = new FSDirectory(this, conf);
      this.cacheManager = new CacheManager(this, conf, blockManager);
      this.auditLoggers = initAuditLoggers(conf);
      this.isDefaultAuditLogger = auditLoggers.size() == 1 &&
          auditLoggers.get(0) instanceof DefaultAuditLogger;
      this.aclConfigFlag = new AclConfigFlag(conf);
      this.isRetryCacheEnabled = conf.getBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY,
          DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT);
      this.retryCache = ignoreRetryCache ? null : initRetryCache(conf);
      this.slicerBatchSize = conf.getInt(DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE,
          DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE_DEFAULT);

      this.slicerNbThreads = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS,
          DFSConfigKeys.DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS_DEFAULT);
      
      this.maxDBTries = conf.getInt(DFSConfigKeys.DFS_NAMENODE_DB_CHECK_MAX_TRIES,
          DFSConfigKeys.DFS_NAMENODE_DB_CHECK_MAX_TRIES_DEFAULT);
    } catch (IOException | RuntimeException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
  }

  @VisibleForTesting
  public List<AuditLogger> getAuditLoggers() {
    return auditLoggers;
  }

  @VisibleForTesting
  public RetryCacheDistributed getRetryCache() {
    return retryCache;
  }

  /** Whether or not retry cache is enabled */
  boolean hasRetryCache() {
    return retryCache != null;
  }

  void addCacheEntryWithPayload(byte[] clientId, int callId, Object payload) {
    if (retryCache != null) {
      retryCache.addCacheEntryWithPayload(clientId, callId, payload);
    }
  }

  void addCacheEntry(byte[] clientId, int callId) {
    if (retryCache != null) {
      retryCache.addCacheEntry(clientId, callId);
    }
  }

  @VisibleForTesting
  static RetryCacheDistributed initRetryCache(Configuration conf) {
    boolean enable = conf.getBoolean(DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY,
        DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT);
    LOG.info("Retry cache on namenode is " + (enable ? "enabled" : "disabled"));
    if (enable) {
      float heapPercent = conf.getFloat(
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY,
          DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT);
      long entryExpiryMillis = conf.getLong(
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);
      LOG.info("Retry cache will use " + heapPercent
          + " of total heap and retry cache entry expiry time is "
          + entryExpiryMillis + " millis");
      return new RetryCacheDistributed("NameNodeRetryCache", heapPercent,
          entryExpiryMillis);
    }
    return null;
  }

  private List<AuditLogger> initAuditLoggers(Configuration conf) {
    // Initialize the custom access loggers if configured.
    Collection<String> alClasses =
        conf.getStringCollection(DFS_NAMENODE_AUDIT_LOGGERS_KEY);
    List<AuditLogger> auditLoggers = Lists.newArrayList();
    if (alClasses != null && !alClasses.isEmpty()) {
      for (String className : alClasses) {
        try {
          AuditLogger logger;
          if (DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
            logger = new DefaultAuditLogger();
          } else {
            logger = (AuditLogger) Class.forName(className).newInstance();
          }
          logger.initialize(conf);
          auditLoggers.add(logger);
        } catch (RuntimeException re) {
          throw re;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Make sure there is at least one logger installed.
    if (auditLoggers.isEmpty()) {
      auditLoggers.add(new DefaultAuditLogger());
    }

    // Add audit logger to calculate top users
    if (conf.getBoolean(DFSConfigKeys.NNTOP_ENABLED_KEY,
        DFSConfigKeys.NNTOP_ENABLED_DEFAULT)) {
      String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
      TopConf nntopConf = new TopConf(conf);
      TopMetrics.initSingleton(conf, "NAMENODE", sessionId,
          nntopConf.nntopReportingPeriodsMs);
      auditLoggers.add(new TopAuditLogger());
    }

    return Collections.unmodifiableList(auditLoggers);
  }

  private void startSecretManager() {
    if (dtSecretManager != null) {
      try {
        dtSecretManager.startThreads();
      } catch (IOException e) {
        // Inability to start secret manager
        // can't be recovered from.
        throw new RuntimeException(e);
      }
    }
  }

  private void startSecretManagerIfNecessary() throws IOException {
    boolean shouldRun = shouldUseDelegationTokens() && !isInSafeMode();
    boolean running = dtSecretManager.isRunning();
    if (shouldRun && !running) {
      startSecretManager();
    }
  }

  private void stopSecretManager() {
    if (dtSecretManager != null) {
      dtSecretManager.stopThreads();
    }
  }

  /**
   * Start services common
   *      configuration
   *
   * @param conf
   * @throws IOException
   */
  void startCommonServices(Configuration conf) throws IOException {
    this.registerMBean(); // register the MBean for the FSNamesystemState
    IDsMonitor.getInstance().start();
    RootINodeCache.start();
    nnResourceChecker = new NameNodeResourceChecker(conf);
    checkAvailableResources();
    if (isLeader()) {
      HdfsVariables.setSafeModeInfo(new SafeModeInfo(conf), -1);
      inSafeMode.set(true);
      assert safeMode() != null && !isPopulatingReplQueues();
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginPhase(Phase.SAFEMODE);
      prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
          getCompleteBlocksTotal());
      setBlockTotal();
    }
    shouldPopulateReplicationQueue = true;
    blockManager.activate(conf);
    if (dir.isQuotaEnabled()) {
      quotaUpdateManager.activate();
    }

    registerMXBean();
    DefaultMetricsSystem.instance().register(this);
  }

  /**
   * Stop services common
   *
   */
  private void stopCommonServices() {
    if (blockManager != null) {
      blockManager.close();
    }
    if (quotaUpdateManager != null) {
      quotaUpdateManager.close();
    }
    RootINodeCache.stop();
    RetryCacheDistributed.clear(retryCache);
  }

  /**
   * Start services required in active state
   *
   * @throws IOException
   */
  void startActiveServices() throws IOException {
    startingActiveService = true;
    LOG.info("Starting services required for active state");
    LOG.info("Catching up to latest edits from old active before " + "taking over writer role in edits logs");
    try {
      blockManager.getDatanodeManager().markAllDatanodesStale();

      if (isLeader()) {
        // the node is starting and directly leader, this means that no NN was alive before
        // Only need to re-process the queue, If not in SafeMode.
        if (!isInSafeMode()) {
          LOG.info("Reprocessing replication and invalidation queues");
          initializeReplQueues();
        } else {
          HdfsVariables.resetMisReplicatedIndex();
        }
      }

      leaseManager.startMonitor();
      startSecretManagerIfNecessary();

      //ResourceMonitor required only at ActiveNN. See HDFS-2914
      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
      nnrmthread.start();

      if(isRetryCacheEnabled) {
        this.retryCacheCleanerThread = new Daemon(new RetryCacheCleaner());
        retryCacheCleanerThread.start();
      }

      if (erasureCodingEnabled) {
        erasureCodingManager.activate();
      }
      
      cacheManager.startMonitorThread();
//      blockManager.getDatanodeManager().setShouldSendCachingCommands(true);
    } finally {
      startingActiveService = false;
    }
  }
  
  /**
   * Initialize replication queues.
   */
  private void initializeReplQueues() throws IOException {
    LOG.info("initializing replication queues");
    blockManager.processMisReplicatedBlocks();
    initializedReplQueues = true;
  }

  /**
   * @return Whether the namenode is transitioning to active state and is in the
   *         middle of the {@link #startActiveServices()}
   */
  public boolean inTransitionToActive() {
    return startingActiveService;
  }

  private boolean shouldUseDelegationTokens() {
    return UserGroupInformation.isSecurityEnabled() ||
        alwaysUseDelegationTokensForTests;
  }

  /**
   * Stop services required in active state
   *
   * @throws InterruptedException
   */
  void stopActiveServices() {
    LOG.info("Stopping services started for active state");
    stopSecretManager();
    leaseManager.stopMonitor();
    if (nnrmthread != null) {
      ((NameNodeResourceMonitor) nnrmthread.getRunnable()).stopMonitor();
      nnrmthread.interrupt();
    }

    if(retryCacheCleanerThread !=null){
      ((RetryCacheCleaner)retryCacheCleanerThread.getRunnable()).stopMonitor();
      retryCacheCleanerThread.interrupt();
    }

    if (erasureCodingManager != null) {
      erasureCodingManager.close();
    }
    
    if (cacheManager != null) {
      cacheManager.stopMonitorThread();
      //HOPS as we are distributed we may stop one NN without stopping all of them. In this case we should not clear
      //the information used by the other NN.
      //TODO: check if there is some case where we really need to do the clear.
//      cacheManager.clearDirectiveStats();
//      blockManager.getDatanodeManager().clearPendingCachingCommands();
//      blockManager.getDatanodeManager().setShouldSendCachingCommands(false);
//      blockManager.clearQueues();
//      initializedReplQueues = false;
    }
  }

  /**
   * @throws RetriableException
   *           If 1) The NameNode is in SafeMode, 2) HA is enabled, and 3)
   *           NameNode is in active state
   * @throws SafeModeException
   *           Otherwise if NameNode is in SafeMode.
   */
  private void checkNameNodeSafeMode(String errorMsg)
      throws RetriableException, SafeModeException, IOException {
    if (isInSafeMode()) {
      SafeModeInfo safeMode = safeMode();
      SafeModeException se = new SafeModeException(errorMsg, safeMode);
      if (shouldRetrySafeMode(safeMode)) {
        throw new RetriableException(se);
      } else {
        throw se;
      }
    }
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }

  /**
   * We already know that the safemode is on. We will throw a RetriableException
   * if the safemode is not manual or caused by low resource.
   */
  private boolean shouldRetrySafeMode(SafeModeInfo safeMode) {
    if (safeMode == null) {
      return false;
    } else {
      return !safeMode.isManual() && !safeMode.areResourcesLow();
    }
  }

  NamespaceInfo getNamespaceInfo() throws IOException {
    return unprotectedGetNamespaceInfo();
  }

  /**
   * Version of @see #getNamespaceInfo() that is not protected by a lock.
   */
  private NamespaceInfo unprotectedGetNamespaceInfo() throws IOException {

    StorageInfo storageInfo = StorageInfo.getStorageInfoFromDB();

    return new NamespaceInfo(storageInfo.getNamespaceID(), getClusterId(),
        getBlockPoolId(), storageInfo.getCTime());
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  void close() {
    fsRunning = false;
    try {
      stopCommonServices();
      if (smmthread != null) {
        smmthread.interrupt();
      }
      fsOperationsExecutor.shutdownNow();
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        stopActiveServices();
        if (dir != null) {
          dir.close();
        }
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  @Override
  public boolean isRunning() {
    return fsRunning;
  }

  FsServerDefaults getServerDefaults() {
    return serverDefaults;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   *
   * @throws IOException
   */
  void setPermissionSTO(final String src1, final FsPermission permission)
      throws
      IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    boolean txFailed = true;
    //we just want to lock the subtree the permission are checked in setPermissionSTOInt
    final INodeIdentifier inode = lockSubtreeAndCheckOwnerAndParentPermission(src, true,
             null, SubTreeOperation.Type.SET_PERMISSION_STO);
    final boolean isSTO = inode != null;
    try {
      new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_SETPERMISSION, src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          INodeLock il = lf.getINodeLock( INodeLockType.WRITE, INodeResolveType.PATH,  src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(!dir.isQuotaEnabled());
          if (isSTO) {
            il.setIgnoredSTOInodes(inode.getInodeId());
          }
          locks.add(il).add(lf.getBlockLock());
          locks.add(lf.getAcesLock());
        }

        @Override
        public Object performTask() throws IOException {
          try {
            setPermissionSTOInt(src, permission, isSTO);
          } catch (AccessControlException e) {
            logAuditEvent(false, "setPermission", src);
            throw e;
          }
          return null;
        }
      }.handle(this);
      txFailed = false;
    } finally {
      if(txFailed){
        if(inode!=null){
          unlockSubtree(src, inode.getInodeId());
        }
      }
    }

  }

  private void setPermissionSTOInt(String src, FsPermission permission,boolean isSTO)
      throws
      IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set permission for " + src);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    dir.checkOwner(pc, iip);
    dir.setPermission(src, permission);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setPermission", src, null, resultingStat);

    //remove sto from
    if(isSTO){
      INodesInPath inodesInPath = dir.getINodesInPath(src, false);
      INode[] nodes = inodesInPath.getINodes();
      INode inode = nodes[nodes.length - 1];
      if (inode != null && inode.isSTOLocked()) {
        inode.setSubtreeLocked(false);
        EntityManager.update(inode);
      }
      EntityManager.remove(new SubTreeOperation(getSubTreeLockPathPrefix(src)));
    }
  }

  /**
   * Set permissions for an existing file.
   *
   * @throws IOException
   */
  void setPermission(final String src1, final FsPermission permission)
      throws
      IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_PERMISSION, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        try {
          setPermissionInt(src, permission);
        } catch (AccessControlException e) {
          logAuditEvent(false, "setPermission", src);
          throw e;
        }
        return null;
      }
    }.handle(this);
  }

  private void setPermissionInt(String src, FsPermission permission)
      throws IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set permission for " + src);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    dir.checkOwner(pc, iip);
    dir.setPermission(src, permission);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setPermission", src, null, resultingStat);
  }

  /**
   * Set owner for an existing file.
   *
   * @throws IOException
   */
  void setOwnerSTO(final String src1, final String username, final String group)
      throws
      IOException {
    //only for testing STO
    saveTimes();

    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents);
    boolean txFailed = true;
    //we just want to lock the subtree the permission are checked in setOwnerSTOInt
    final INodeIdentifier inode =  lockSubtreeAndCheckOwnerAndParentPermission(src, true,
             null, SubTreeOperation.Type.SET_OWNER_STO);
    final boolean isSTO = inode != null;
    try{
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_OWNER_SUBTREE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock( INodeLockType.WRITE, INodeResolveType.PATH,  src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!dir.isQuotaEnabled());
        if (isSTO){
          il.setIgnoredSTOInodes(inode.getInodeId());
        }
        locks.add(il).add(lf.getBlockLock()).add(lf.getAcesLock());
      }

        @Override
        public Object performTask() throws IOException {
          try {
            setOwnerSTOInt(src, username, group,isSTO);
          } catch (AccessControlException e) {
            logAuditEvent(false, "setOwner", src);
            throw e;
          }
          return null;
        }
      }.handle(this);
      txFailed = false;
    }finally{
      if(txFailed){
        if(inode!=null){
          unlockSubtree(src,inode.getInodeId());
        }
      }
    }
  }

  private void setOwnerSTOInt(String src, String username, String group, boolean isSTO)
      throws
      IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set owner for " + src);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    dir.checkOwner(pc, iip);
    if (!pc.isSuperUser()) {
      if (username != null && !pc.getUser().equals(username)) {
        throw new AccessControlException("Non-super user cannot change owner");
      }
      if (group != null && !pc.containsGroup(group)) {
        throw new AccessControlException("User does not belong to " + group);
      }
    }
    dir.setOwner(src, username, group);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setOwner", src, null, resultingStat);
    if(isSTO){
      INodesInPath inodesInPath = dir.getINodesInPath(src, false);
      INode[] nodes = inodesInPath.getINodes();
      INode inode = nodes[nodes.length - 1];
      if (inode != null && inode.isSTOLocked()) {
        inode.setSubtreeLocked(false);
        EntityManager.update(inode);
      }
      EntityManager.remove(new SubTreeOperation(getSubTreeLockPathPrefix(src)));
    }
  }

  /**
   * Set owner for an existing file.
   *
   * @throws IOException
   */
  void setOwner(final String src, final String username, final String group)
      throws
      IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_OWNER, src) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        try {
          setOwnerInt(src, username, group);
        } catch (AccessControlException e) {
          logAuditEvent(false, "setOwner", src);
          throw e;
        }
        return null;
      }
    }.handle(this);
  }

  private void setOwnerInt(String src, String username, String group)
      throws
      IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    checkNameNodeSafeMode("Cannot set owner for " + src);
    src = dir.resolvePath(src, pathComponents);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    dir.checkOwner(pc, iip);
    if (!pc.isSuperUser()) {
      if (username != null && !pc.getUser().equals(username)) {
        throw new AccessControlException("Non-super user cannot change owner");
      }
      if (group != null && !pc.containsGroup(group)) {
        throw new AccessControlException("User does not belong to " + group);
      }
    }
    dir.setOwner(src, username, group);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setOwner", src, null, resultingStat);
  }

  /**
   * Get block locations within the specified range.
   *
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(final String clientMachine, final String src,
      final long offset, final long length) throws IOException {

    // First try the operation using shared lock.
    // Upgrade the lock to exclusive lock if LockUpgradeException is encountered.
    // This operation tries to update the inode access time once every hr.
    // The lock upgrade exception is thrown when the inode access time stamp is
    // updated while holding shared lock on the inode. In this case retry the operation
    // using an exclusive lock.
    try{
      return getBlockLocationsWithLock(clientMachine, src, offset, length, INodeLockType.READ);
    }catch(LockUpgradeException e){
      LOG.debug("Encountered LockUpgradeException while reading "+src+". Retrying the operation using exclusive locks");
      return getBlockLocationsWithLock(clientMachine, src, offset, length, INodeLockType.WRITE);
    }
  }

  LocatedBlocks getBlockLocationsWithLock(final String clientMachine, final String src1,
                                          final long offset, final long length, final INodeLockType lockType) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents);
    HopsTransactionalRequestHandler getBlockLocationsHandler =
            new HopsTransactionalRequestHandler(
                    HDFSOperationType.GET_BLOCK_LOCATIONS, src) {
              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
                LockFactory lf = getInstance();
                INodeLock il = lf.getINodeLock( lockType, INodeResolveType.PATH, src)
                        .setNameNodeID(nameNode.getId())
                        .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                        .skipReadingQuotaAttr(!dir.isQuotaEnabled());
                locks.add(il).add(lf.getBlockLock())
                        .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.CA));
                locks.add(lf.getAcesLock());
              }

          @Override
          public Object performTask() throws IOException {
            LocatedBlocks blocks =
                getBlockLocationsInternal(src, offset, length, true, true,
                    true);
            if (blocks != null && !blocks
                .hasPhantomBlock()) { // no need to sort phantom datanodes
              blockManager.getDatanodeManager()
                  .sortLocatedBlocks(clientMachine, blocks.getLocatedBlocks());

             // lastBlock is not part of getLocatedBlocks(), might need to sort it too
              LocatedBlock lastBlock = blocks.getLastLocatedBlock();
              if (lastBlock != null) {
                ArrayList<LocatedBlock> lastBlockList = Lists.newArrayListWithCapacity(1);
                lastBlockList.add(lastBlock);
                blockManager.getDatanodeManager()
                    .sortLocatedBlocks(clientMachine, lastBlockList);
                  }
                }
                return blocks;
              }
            };
    LocatedBlocks locatedBlocks = (LocatedBlocks) getBlockLocationsHandler.handle(this);
    logAuditEvent(true, "open", src);
    return locatedBlocks;
  }

  /**
   * Get block locations within the specified range.
   *
   * @throws FileNotFoundException,
   *     UnresolvedLinkException, IOException
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(final String src1, final long offset,
      final long length, final boolean doAccessTime,
      final boolean needBlockToken, final boolean checkSafeMode)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler getBlockLocationsHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_BLOCK_LOCATIONS, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.CA));
          }

          @Override
          public Object performTask() throws IOException {
            return getBlockLocationsInternal(src, offset, length, doAccessTime,
                needBlockToken, checkSafeMode);
          }
        };
    return (LocatedBlocks) getBlockLocationsHandler.handle(this);
  }

  /**
   * Get block locations within the specified range.
   *
   * @throws FileNotFoundException,
   *     UnresolvedLinkException, IOException
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  private LocatedBlocks getBlockLocationsInternal(String src, long offset,
      long length,
      boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
      throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    try {
      return getBlockLocationsInt(pc, src, offset, length, doAccessTime,
          needBlockToken, checkSafeMode);
    } catch (AccessControlException e) {
      logAuditEvent(false, "open", src);
      throw e;
    }
  }


  private LocatedBlocks getBlockLocationsInt(FSPermissionChecker pc, String src,
      long offset, long length, boolean doAccessTime, boolean needBlockToken,
      boolean checkSafeMode)
      throws IOException {
    final INodesInPath iip = dir.getINodesInPath(src, true);
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.READ);
    }

    if (offset < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new HadoopIllegalArgumentException(
          "Negative length is not supported. File: " + src);
    }

    LocatedBlocks ret;
    final INodeFile inodeFile = INodeFile.valueOf(dir.getINode(src), src);
    if (inodeFile.isFileStoredInDB()) {
      LOG.debug("SMALL_FILE The file is stored in the database. Returning Phantom Blocks");
      ret = getPhantomBlockLocationsUpdateTimes(src, offset, length, doAccessTime, needBlockToken);
    } else {
      ret = getBlockLocationsUpdateTimes(src, offset, length, doAccessTime, needBlockToken);
    }

    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.getLocatedBlocks()) {
        // if safe mode & no block locations yet then throw SafeModeException
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          SafeModeException se = new SafeModeException(
              "Zero blocklocations for " + src, safeMode());
          throw new RetriableException(se);
        }
      }
    }
    return ret;
  }

  /**
   * Get phantom block location for the file stored in the database and
   * access times if necessary.
   */

  private LocatedBlocks getPhantomBlockLocationsUpdateTimes(String src,
      long offset, long length, boolean doAccessTime, boolean needBlockToken)
      throws IOException {
    for (int attempt = 0; attempt < 2; attempt++) {
      // if the namenode is in safe mode, then do not update access time
      if (isInSafeMode()) {
        doAccessTime = false;
      }

      long now = now();
      final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
      if (doAccessTime && isAccessTimeSupported()) {
        if (now <= inode.getAccessTime() + getAccessTimePrecision()) {
          // if we have to set access time but we only have a read lock, then
          // restart this entire operation with the writeLock.
          if (attempt == 0) {
            continue;
          }
        }
        dir.setTimes(inode, -1, now, false);
      }

      return blockManager
          .createPhantomLocatedBlocks(inode, inode.getFileDataInDB(),
              inode.isUnderConstruction(), needBlockToken);
    }
    return null; // can never reach here
  }

  /*
   * Get block locations within the specified range, updating the
   * access times if necessary.
   */
  private LocatedBlocks getBlockLocationsUpdateTimes(String src, long offset,
      long length, boolean doAccessTime, boolean needBlockToken)
      throws IOException {
      // if the namenode is in safe mode, then do not update access time
      if (isInSafeMode()) {
        doAccessTime = false;
      }
      long now = now();
      final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
      if (doAccessTime && isAccessTimeSupported()) {
        dir.setTimes(inode, -1, now, false);
      }
      final long fileSize = inode.computeFileSizeNotIncludingLastUcBlock();
      boolean isUc = inode.isUnderConstruction();

    LocatedBlocks blocks = blockManager.createLocatedBlocks(inode.getBlocks(), fileSize,
        isUc, offset, length, needBlockToken);
    // Set caching information for the located blocks.
    for (LocatedBlock lb : blocks.getLocatedBlocks()) {
      cacheManager.setCachedLocations(lb, inode.getId());
    }
    return blocks;      
  }

  /**
   * Moves all the blocks from srcs and appends them to trg
   * To avoid rollbacks we will verify validity of ALL of the args
   * before we start actual move.
   *
   * This does not support ".inodes" relative path
   * @param target
   * @param srcs
   * @throws IOException
   */
  void concat(final String target, final String[] srcs) throws IOException {

    checkNameNodeSafeMode("Cannot concat " + target);
    HdfsFileStatus stat = null;
    boolean success = false;
    try {
      stat = FSDirConcatOp.concat(dir, target, srcs);
      success = true;
    } finally {
      logAuditEvent(success, "concat", Arrays.toString(srcs), target, stat);
    }
  }

  /**
   * stores the modification and access time for this inode.
   * The access time is precise up to an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(final String src1, final long mtime, final long atime)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents);
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_TIMES, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock());
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        try {
          setTimesInt(src, mtime, atime);
        } catch (AccessControlException e) {
          logAuditEvent(false, "setTimes", src);
          throw e;
        }
        return null;
      }
    }.handle(this);
  }

  private void setTimesInt(String src, long mtime, long atime)
      throws IOException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
          " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY +
          " configuration parameter.");
    }
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    // Write access is required to set access and modification times
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.WRITE);
    }
    final INode inode = iip.getLastINode();
    if (inode != null) {
      dir.setTimes(inode, mtime, atime, true);
      resultingStat = getAuditFileInfo(src, false);
    } else {
      throw new FileNotFoundException(
          "File/Directory " + src + " does not exist.");
    }
    logAuditEvent(true, "setTimes", src, null, resultingStat);
  }

  /**
   * Create a symbolic link.
   */
  void createSymlink(final String target, final String link1,
      final PermissionStatus dirPerms, final boolean createParent)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(link1);
    final String link = dir.resolvePath(link1, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.CREATE_SYM_LINK,
        link) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH,  link)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getAcesLock());
        if(isRetryCacheEnabled) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        final CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        if (!FileSystem.areSymlinksEnabled()) {
          throw new UnsupportedOperationException("Symlinks not supported");
        }
        if (!DFSUtil.isValidName(link)) {
          throw new InvalidPathException("Invalid link name: " + link);
        }
        if (FSDirectory.isReservedName(target)) {
          throw new InvalidPathException("Invalid target name: " + target);
        }
        boolean success = false;
        try {
          createSymlinkInt(target, link, dirPerms, createParent);
          success = true;
        } catch (AccessControlException e) {
          logAuditEvent(false, "createSymlink", link, target, null);
          throw e;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle(this);
  }

  private void createSymlinkInt(String target, String link,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot create symlink " + link);
    final INodesInPath iip = dir.getINodesInPath4Write(link, false);
    if (!createParent) {
      dir.verifyParentDir(iip, link);
    }
    if (!dir.isValidToCreate(link)) {
      throw new IOException("failed to create link " + link +
          " either because the filename is invalid or the file exists");
    }
    if (isPermissionEnabled) {
      dir.checkAncestorAccess(pc, iip, FsAction.WRITE);
    }
    // validate that we have enough inodes.
    checkFsObjectLimit();

    // add symbolic link to namespace
    addSymlink(link, target, dirPerms, createParent);
    resultingStat = getAuditFileInfo(link, false);
    logAuditEvent(true, "createSymlink", link, target, resultingStat);
  }

  /**
   * Set replication for an existing file.
   * <p/>
   * The NameNode sets new replication and schedules either replication of
   * under-replicated data blocks or removal of the excessive block copies
   * if the blocks are over-replicated.
   *
   * @param src1
   *     file name
   * @param replication
   *     new replication
   * @return true if successful;
   * false if file does not exist or is a directory
   * @see ClientProtocol#setReplication(String, short)
   */
  boolean setReplication(final String src1, final short replication)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler setReplicationHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.SET_REPLICATION,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il).add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.UR, BLK.IV)).add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            try {
              return setReplicationInt(src, replication);
            } catch (AccessControlException e) {
              logAuditEvent(false, "setReplication", src);
              throw e;
            }
          }
        };
    return (Boolean) setReplicationHandler.handle(this);
  }

  private boolean setReplicationInt(final String src, final short replication)
      throws IOException {
    blockManager.verifyReplication(src, replication, null);
    final boolean isFile;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set replication for " + src);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.WRITE);
    }

    INode targetNode = getINode(src);

    final short[] oldReplication = new short[1];
    final Block[] blocks = dir.setReplication(src, replication, oldReplication);
    isFile = blocks != null;
    // [s] for the files stored in the database setting the replication level does not make
    // any sense. For now we will just set the replication level as requested by the user
    if (isFile && !((INodeFile) targetNode).isFileStoredInDB()) {
      blockManager.setReplication(oldReplication[0], replication, src, blocks);
    }

    if (isFile) {
      logAuditEvent(true, "setReplication", src);
    }
    return isFile;
  }

  void setMetaEnabled(final String srcArg, final boolean metaEnabled)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    INodeIdentifier stoRootINode = null;
    try {
      //we just want to lock the subtree, the access check is done in the perform task.
      stoRootINode = lockSubtree(src, SubTreeOperation.Type.META_ENABLE_STO);
      final AbstractFileTree.FileTree fileTree = buildTreeForLogging(stoRootINode,
          metaEnabled);
      new HopsTransactionalRequestHandler(HDFSOperationType.SET_META_ENABLED,
          src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          long stoRootINodeId = (Long) getParams()[0];
          LockFactory lf = getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            il.setIgnoredSTOInodes(stoRootINodeId);
          locks.add(il);
          locks.add(lf.getAcesLock());
        }

        @Override
        public Object performTask() throws IOException {
          try {
            FSPermissionChecker pc = getPermissionChecker();
            final INodesInPath iip = dir.getINodesInPath4Write(src);
            dir.checkPathAccess(pc, iip, FsAction.WRITE);
            if(metaEnabled) {
              logMetadataEvents(fileTree, MetadataLogEntry.Operation.ADD);
            }
            setMetaEnabledInt(src, metaEnabled);
          } catch (AccessControlException e) {
            logAuditEvent(false, "setMetaEnabled", src);
            throw e;
          }
          return null;
        }
      }.setParams(stoRootINode.getInodeId()).handle(this);
    } finally {
      if(stoRootINode != null) {
        unlockSubtree(src, stoRootINode.getInodeId());
      }
    }
  }

  private AbstractFileTree.FileTree buildTreeForLogging(INodeIdentifier
      inode, boolean metaEnabled) throws IOException {
    if(!metaEnabled)
      return null;
    
    final AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(
        FSNamesystem.this, inode);
    fileTree.buildUp();
    return fileTree;
  }
  
  private void setMetaEnabledInt(final String src, final boolean metaEnabled)
      throws IOException {
    checkNameNodeSafeMode("Cannot set metaEnabled for " + src);

    INode targetNode = getINode(src);
    if (!targetNode.isDirectory()) {
      throw new FileNotFoundException(src + ": Is not a directory");
    } else {
      INodeDirectory dirNode = (INodeDirectory) targetNode;
      dirNode.setMetaEnabled(metaEnabled);
      EntityManager.update(dirNode);
    }
  }

  private void logMetadataEvents(AbstractFileTree.FileTree fileTree,
      MetadataLogEntry.Operation operation) throws IOException {
    ProjectedINode dataSetDir = fileTree.getSubtreeRoot();
    Collection<MetadataLogEntry> logEntries = new ArrayList<>(fileTree
        .getAllChildren().size());
    for (ProjectedINode node : fileTree.getAllChildren()) {
      node.incrementLogicalTime();
      MetadataLogEntry logEntry = new MetadataLogEntry(dataSetDir.getId(),
          node.getId(), node.getPartitionId(), node.getParentId(), node
          .getName(), node.getLogicalTime(), operation);
      logEntries.add(logEntry);
      EntityManager.add(logEntry);
    }
    AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime
        (logEntries);
  }

  /**
   * Set the storage policy for a file or a directory.
   *
   * @param src file/directory path
   * @param policyName storage policy name
   */
  void setStoragePolicy(String src, final String policyName)
      throws IOException {
    try {
      setStoragePolicyInt(src, policyName);
    } catch (AccessControlException e) {
      logAuditEvent(false, "setStoragePolicy", src);
      throw e;
    }
  }

  private void setStoragePolicyInt(final String srcArg, final String policyName)
      throws IOException, UnresolvedLinkException, AccessControlException {

    if (!isStoragePolicyEnabled) {
      throw new IOException("Failed to set storage policy since "
          + DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY + " is set to false.");
    }

    final BlockStoragePolicy policy =  blockManager.getStoragePolicy(policyName);
    if (policy == null) {
      throw new HadoopIllegalArgumentException("Cannot find a block policy with the name " + policyName);
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = FSDirectory.resolvePath(srcArg, pathComponents, dir);
    HopsTransactionalRequestHandler setStoragePolicyHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.SET_STORAGE_POLICY, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il);
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            final INodesInPath iip = dir.getINodesInPath4Write(src);
            checkNameNodeSafeMode("Cannot set metaEnabled for " + src);
            if (isPermissionEnabled) {
              dir.checkPermission(pc, iip, false, null, null, FsAction.WRITE, null, false);
            }

            dir.setStoragePolicy(iip, policy);

            return null;
          }
        };

    setStoragePolicyHandler.handle();
  }


  BlockStoragePolicy getDefaultStoragePolicy() throws IOException {
    return blockManager.getDefaultStoragePolicy();
  }


  BlockStoragePolicy getStoragePolicy(byte storagePolicyID) throws IOException {
    return blockManager.getStoragePolicy(storagePolicyID);
  }

  /**
   * @return All the existing block storage policies
   */
  BlockStoragePolicy[] getStoragePolicies() throws IOException {
    return blockManager.getStoragePolicies();
  }

  long getPreferredBlockSize(final String filenameArg) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(filenameArg);
    final String filename = dir.resolvePath(filenameArg, pathComponents, dir);
    HopsTransactionalRequestHandler getPreferredBlockSizeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_PREFERRED_BLOCK_SIZE, filename) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il =lf.getINodeLock(INodeLockType.READ_COMMITTED, INodeResolveType.PATH, filename)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il);
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            final INodesInPath iip = dir.getINodesInPath(filename, true);
            if (isPermissionEnabled) {
              dir.checkTraverse(pc, iip);
            }
            return dir.getPreferredBlockSize(filename);
          }
        };
    return (Long) getPreferredBlockSizeHandler.handle(this);
  }

  /**
   * Create a new file entry in the namespace.
   * <p/>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create(String, FsPermission, String, EnumSetWritable, boolean, short, long)} , except it returns valid file status upon
   * success
   *
   * For retryCache handling details see -
   * {@link #getFileStatus(boolean, CacheEntryWithPayload)}
   *
   */
  HdfsFileStatus startFile(final String src1, final PermissionStatus permissions,
      final String holder, final String clientMachine,
      final EnumSet<CreateFlag> flag, final boolean createParent,
      final short replication, final long blockSize) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
      return (HdfsFileStatus) new HopsTransactionalRequestHandler(
          HDFSOperationType.START_FILE, src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(!dir.isQuotaEnabled());
          locks.add(il)
                  .add(lf.getBlockLock())
                  .add(lf.getLeaseLock(LockType.WRITE, holder))
                  .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(
                  lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR,
                      BLK.PE, BLK.IV));

          locks.add(lf.getAllUsedHashBucketsLock());

          if(isRetryCacheEnabled) {
            locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
                Server.getCallId()));
          }

        if (flag.contains(CreateFlag.OVERWRITE) && dir.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(src));
        }
        if (flag.contains(CreateFlag.OVERWRITE) && erasureCodingEnabled) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
        }
        locks.add(lf.getAcesLock());
      }

        @Override
        public Object performTask() throws IOException {
        CacheEntryWithPayload cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache,
            null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          HdfsFileStatus test = PBHelper.convert(HdfsProtos.HdfsFileStatusProto.parseFrom(cacheEntry.getPayload()));
          return test;
        }
        HdfsFileStatus status = null;
        try {
           status = startFileInt(src, permissions, holder, clientMachine, flag,
              createParent, replication, blockSize);
           return status;
        } catch (AccessControlException e) {
          logAuditEvent(false, "create", src);
          throw e;
        } finally {
          byte[] statusArray = status == null ? null : PBHelper.convert(status).toByteArray();
          RetryCacheDistributed.setState(cacheEntry, status != null, statusArray);
        }
      }
    }.handle(this);
  }

  private HdfsFileStatus startFileInt(String src, PermissionStatus permissions,
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws
      IOException {
    FSPermissionChecker pc = getPermissionChecker();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.startFile: src=" + src + ", holder=" + holder +
              ", clientMachine=" + clientMachine + ", createParent=" +
              createParent + ", replication=" + replication + ", createFlag=" +
              flag.toString());
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }

    if (blockSize < minBlockSize) {
      throw new IOException("Specified block size is less than configured" + " minimum value ("
          + DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY
          + "): " + blockSize + " < " + minBlockSize);
    }

    boolean create = flag.contains(CreateFlag.CREATE);
    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    startFileInternal(pc, iip, permissions, holder, clientMachine, create, overwrite,
        createParent, replication, blockSize);
    final HdfsFileStatus stat = FSDirStatAndListingOp.getFileInfo(dir, src, false, true);
    logAuditEvent(true, "create", src, null,
        (isAuditEnabled() && isExternalInvocation()) ? stat : null);
    return stat;
  }

  /**
   * Create a new file or overwrite an existing file<br>
   *
   * Once the file is create the client then allocates a new block with the next
   * call using {@link ClientProtocol#addBlock(String, String, ExtendedBlock, DatanodeInfo[], long, String[])} ()}.
   * <p>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}
   */
  private void startFileInternal(FSPermissionChecker pc, INodesInPath iip,
      PermissionStatus permissions, String holder, String clientMachine,
      boolean create, boolean overwrite, boolean createParent, short replication,
      long blockSize) throws
      IOException {

    // Verify that the destination does not exist as a directory already.
    final INode inode = iip.getLastINode();
    final String src = iip.getPath();
    if (inode != null && inode.isDirectory()) {
      throw new FileAlreadyExistsException(src +
          " already exists as a directory");
    }
    
    final INodeFile myFile = INodeFile.valueOf(inode, src, true);
    if (isPermissionEnabled) {
      if (overwrite && myFile != null) {
        dir.checkPathAccess(pc, iip, FsAction.WRITE);
      }
      /*
       * To overwrite existing file, need to check 'w' permission 
       * of parent (equals to ancestor in this case)
       */
      dir.checkAncestorAccess(pc, iip, FsAction.WRITE);
    }
    if (!createParent) {
      dir.verifyParentDir(iip, src);
    }

    if (!createParent) {
      dir.verifyParentDir(iip, src);
    }

    try {
      
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException("Can't overwrite non-existent " +
              src + " for client " + clientMachine);
        }
      } else {
        if (overwrite) {
          try {
            deleteInt(src, true); // File exists - delete if overwrite
          } catch (AccessControlException e) {
            logAuditEvent(false, "delete", src);
            throw e;
          }
        } else {
          // If lease soft limit time is expired, recover the lease
          recoverLeaseInternal(myFile, src, holder, clientMachine, false);
          throw new FileAlreadyExistsException(src + " for client " +
              clientMachine + " already exists");
        }
      }

      checkFsObjectLimit();
      
      INodeFile newNode = null;
      // Always do an implicit mkdirs for parent directory tree.
      Path parent = new Path(src).getParent();
      if (parent != null && FSDirMkdirOp.mkdirsRecursively(dir, parent.toString(),
              permissions, true, now())) {
        newNode = dir.addFile(src, permissions, replication, blockSize,
                holder, clientMachine);
      }
      
      if (newNode == null) {
        throw new IOException("Unable to add " + src +  " to namespace");
      }
      leaseManager.addLease(newNode.getFileUnderConstructionFeature()
          .getClientName(), src);

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: added " +
            src + " inode " + newNode.getId() + " " + holder);
      }
    } catch (IOException ie) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: " + src + " " +
          ie.getMessage());
      throw ie;
    }
  }

  /**
   * Append to an existing file for append.
   * <p>
   *
   * The method returns the last block of the file if this is a partial block,
   * which can still be used for writing more data. The client uses the returned
   * block locations to form the data pipeline for this block.<br>
   * The method returns null if the last block is full. The client then
   * allocates a new block with the next call using {@link ClientProtocol#addBlock(String, String, ExtendedBlock, DatanodeInfo[], long, String[])}}.
   * <p>
   *
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#append(String, String)}
   *
   * @return the last block locations if the block is partial or null otherwise
   */
  private LocatedBlock appendFileInternal(FSPermissionChecker pc, INodesInPath iip,
      String holder, String clientMachine) throws AccessControlException,
      UnresolvedLinkException, FileNotFoundException, IOException {
    final INode inode = iip.getLastINode();
    final String src = iip.getPath();
    if (inode != null && inode.isDirectory()) {
      throw new FileAlreadyExistsException("Cannot append to directory " + src
          + "; already exists as a directory.");
    }
    if (isPermissionEnabled) {
      dir.checkPathAccess(pc, iip, FsAction.WRITE);
    }

    try {
      if (inode == null) {
        throw new FileNotFoundException("failed to append to non-existent file "
          + src + " for client " + clientMachine);
      }
      INodeFile myFile = INodeFile.valueOf(inode, src, true);

      // Opening an existing file for write - may need to recover lease.
      recoverLeaseInternal(myFile, src, holder, clientMachine, false);

      // recoverLeaseInternal may create a new InodeFile via 
      // finalizeINodeFileUnderConstruction so we need to refresh 
      // the referenced file.  
      myFile = INodeFile.valueOf(dir.getINode(src), src, true);
      final BlockInfo lastBlock = myFile.getLastBlock();
      // Check that the block has at least minimum replication.
      if (lastBlock != null && lastBlock.isComplete() && !getBlockManager().isSufficientlyReplicated(lastBlock)) {
        throw new IOException("append: lastBlock=" + lastBlock + " of src=" + src
            + " is not sufficiently replicated yet.");
      }
      
      return prepareFileForWrite(src, myFile, holder, clientMachine);
    } catch (IOException ie) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.append: " +ie.getMessage());
      throw ie;
    }
   }

  /**
   * Replace current node with a INodeUnderConstruction.
   * Recreate lease record.
   *
   * @param src
   *     path to the file
   * @param file
   *     existing file object
   * @param leaseHolder
   *     identifier of the lease holder on this file
   * @param clientMachine
   *     identifier of the client machine
   * @return the last block locations if the block is partial or null otherwise
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  private LocatedBlock prepareFileForWrite(String src, INodeFile file,
      String leaseHolder, String clientMachine)
      throws IOException {
    INodeFile cons =
        file.toUnderConstruction(leaseHolder, clientMachine);
    Lease lease = leaseManager.addLease(cons.getFileUnderConstructionFeature()
        .getClientName(), src);
    if(cons.isFileStoredInDB()){
      LOG.debug("Stuffed Inode:  prepareFileForWrite stored in database. " +
          "Returning phantom block");
      return blockManager.createPhantomLocatedBlocks(cons,cons.getFileDataInDB(),true,false).getLocatedBlocks().get(0);
    } else {
      LocatedBlock ret = blockManager.convertLastBlockToUnderConstruction(cons);
      if (ret != null && dir.isQuotaEnabled()) {
        // update the quota: use the preferred block size for UC block
        final long diff = file.getPreferredBlockSize() - ret.getBlockSize();
        dir.updateSpaceConsumed(src, 0, diff * file.getFileReplication());
      }
      lease.updateLastTwoBlocksInLeasePath(src, file.getLastBlock(), file.getPenultimateBlock());
      return ret;
    }
  }

  /**
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   *
   * @param src1
   *     the srcArg of the file to start lease recovery
   * @param holder
   *     the lease holder's name
   * @param clientMachine
   *     the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(final String srcArg, final String holder,
      final String clientMachine) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    HopsTransactionalRequestHandler recoverLeaseHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.RECOVER_LEASE,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il)
                    .add(lf.getLeaseLock(LockType.WRITE, holder))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR))
                    .add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            if (isInSafeMode()) {
              checkNameNodeSafeMode("Cannot recover the lease of " + src);
            }
            if (!DFSUtil.isValidName(src)) {
              throw new IOException("Invalid file name: " + src);
            }

            final INodesInPath iip = dir.getINodesInPath4Write(src);
            final INodeFile inode = INodeFile.valueOf(iip.getLastINode(), src);
            if (!inode.isUnderConstruction()) {
              return true;
            }
            if (isPermissionEnabled) {
              dir.checkPathAccess(pc, iip, FsAction.WRITE);
            }

            recoverLeaseInternal(inode, src, holder, clientMachine, true);
            return false;
          }
        };

    return (Boolean) recoverLeaseHandler.handle(this);
  }

  private void recoverLeaseInternal(INodeFile fileInode, String src, String holder,
      String clientMachine, boolean force)
      throws IOException {
    if (fileInode != null && fileInode.isUnderConstruction()) {
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseByPath(src);
        if (leaseFile != null && leaseFile.equals(lease)) {
          throw new AlreadyBeingCreatedException(
              "failed to create file " + src + " for " + holder +
                  " for client " + clientMachine +
                  " because current leaseholder is trying to recreate file.");
        }
      }
      //
      // Find the original holder.
      //
      FileUnderConstructionFeature uc = fileInode.getFileUnderConstructionFeature();
      String clientName = uc.getClientName();
      lease = leaseManager.getLease(clientName);
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
            "failed to create file " + src + " for " + holder +
                " for client " + clientMachine +
                " because pendingCreates is non-null but no leases found.");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and
        // close only the file src
        LOG.info("recoverLease: " + lease + ", src=" + src +
            " from client " + clientName);
        internalReleaseLease(lease, src, holder);
      } else {
        assert lease.getHolder().equals(clientName) :
            "Current lease holder " + lease.getHolder() +
                " does not match file creator " + clientName;
        //
        // If the original holder has not renewed in the last SOFTLIMIT
        // period, then start lease recovery.
        //
        if (leaseManager.expiredSoftLimit(lease)) {
          LOG.info("startFile: recover " + lease + ", src=" + src + " client " +
              clientName);
          boolean isClosed = internalReleaseLease(lease, src, null);
          if (!isClosed) {
            throw new RecoveryInProgressException.NonAbortingRecoveryInProgressException(
                "Failed to close file " + src +
                    ". Lease recovery is in progress. Try again later.");
          }
        } else {
          final BlockInfo lastBlock = fileInode.getLastBlock();
          if (lastBlock != null &&
              lastBlock.getBlockUCState() == BlockUCState.UNDER_RECOVERY) {
            throw new RecoveryInProgressException(
                "Recovery in progress, file [" + src + "], " + "lease owner [" +
                    lease.getHolder() + "]");
          } else {
            throw new AlreadyBeingCreatedException(
                "Failed to create file [" + src + "] for [" + holder +
                    "] on client [" + clientMachine +
                    "], because this file is already being created by [" +
                    clientName + "] on [" +
                    uc.getClientMachine() + "]");
          }
        }
      }
    }
  }

  /**
   * Append to an existing file in the namespace.
   */
  LastBlockWithStatus appendFile(final String src, final String holder,
                          final String clientMachine) throws IOException {
    try{
      return appendFileHopFS(src, holder, clientMachine);
    } catch(HDFSClientAppendToDBFileException e){
      LOG.debug(e);
      return moveToDNsAndAppend(src, holder, clientMachine);
    }
  }

  /*
  HDFS clients can not append to a file stored in the database.
  To support the HDFS Clients the files stored in the database are first
  moved to the datanodes.
   */
  private LastBlockWithStatus moveToDNsAndAppend(final String src, final String holder,
      final String clientMachine) throws IOException {
    // open the small file
    FileSystem hopsFSClient1 = FileSystem.newInstance(conf);
    byte[] data = new byte[conf.getInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_DEFAULT)];
    FSDataInputStream is = hopsFSClient1.open(new Path(src));
    int dataRead = is.read(data,0,data.length);
    is.close();

    //now overwrite the in-memory file
    Configuration newConf = copyConfiguration(conf);
    newConf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, false);
    FileSystem hopsFSClient2 = FileSystem.newInstance(newConf);
    FSDataOutputStream os = hopsFSClient2.create(new Path(src), true );
    os.write(data,0,dataRead);
    os.close();

    // now append the file
    return appendFileHopFS(src,holder,clientMachine);
  }

  private Configuration copyConfiguration(Configuration conf){
    Configuration newConf = new HdfsConfiguration();

    for (Map.Entry<String, String> entry : conf) {
      newConf.set(entry.getKey(), entry.getValue());
    }
    return newConf;
  }

  private LastBlockWithStatus appendFileHopFS(final String srcArg, final String holder,
      final String clientMachine) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    HopsTransactionalRequestHandler appendFileHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.APPEND_FILE,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il).add(lf.getBlockLock())
                    .add(lf.getLeaseLock(LockType.WRITE, holder))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR, BLK.IV, BLK.PE))
                    .add(lf.getLastBlockHashBucketsLock());
            if(isRetryCacheEnabled) {
              locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
                  Server.getCallId()));
            }
            
            if(erasureCodingEnabled) {
              locks.add(lf.getEncodingStatusLock(LockType.READ_COMMITTED, src));
            }
            locks.add(lf.getAcesLock());
          }

      @Override
      public Object performTask() throws IOException {
        LastBlockWithStatus lastBlockWithStatus = null;
        CacheEntryWithPayload cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache,
            null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          ClientNamenodeProtocolProtos.AppendResponseProto proto = ClientNamenodeProtocolProtos.AppendResponseProto.
              parseFrom(cacheEntry.getPayload());

          LocatedBlock locatedBlock = proto.hasBlock() ? PBHelper.convert(proto.getBlock()) : null;
          if (locatedBlock.isPhantomBlock()) {
            //we did not store the data in the cache as it was too big, we should fetch it again.
            INode inode = dir.getINode(src);
            final INodeFile file = INodeFile.valueOf(inode, src);
            if (file.isFileStoredInDB()) {
              LOG.debug("Stuffed Inode: appendFileHopFS  stored in database. " + "Returning phantom block");
              locatedBlock.setData(file.getFileDataInDB());
            }
          }
          HdfsFileStatus stat = (proto.hasStat()) ? PBHelper.convert(proto.getStat()) : null;
          return new LastBlockWithStatus(locatedBlock, stat);
        }

        boolean success = false;
        try {
          INode target = getINode(src);
          if (erasureCodingEnabled && 
              target != null && target instanceof INodeFile && !((INodeFile) target).isFileStoredInDB()) {
            EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByInodeId, target.getId());
            if (status != null) {
              throw new IOException("Cannot append to erasure-coded file");
            }
          }

          if (target != null && target instanceof INodeFile && ((INodeFile) target).isFileStoredInDB() && !holder.
              contains("HopsFS")) {
            throw new HDFSClientAppendToDBFileException(
                "HDFS can not directly append to a file stored in the database");
          }

          lastBlockWithStatus = appendFileInt(src, holder, clientMachine);

          if (lastBlockWithStatus != null && lastBlockWithStatus.getLastBlock() != null && !lastBlockWithStatus.
              getLastBlock().isPhantomBlock()) {
            for (String storageID : lastBlockWithStatus.getLastBlock().getStorageIDs()) {

              int sId = blockManager.getDatanodeManager().getSid(storageID);
              BlockInfo blockInfo = EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
                  lastBlockWithStatus.getLastBlock().getBlock().getBlockId(), target.getId());
              Block undoBlock = new Block(blockInfo);
              undoBlock.setGenerationStampNoPersistance(undoBlock
                  .getGenerationStamp() - 1);
              HashBuckets.getInstance().undoHash(sId, HdfsServerConstants.ReplicaState.FINALIZED, undoBlock);
            }
          }
          success = true;
          return lastBlockWithStatus;
        } catch (AccessControlException e) {
          logAuditEvent(false, "append", src);
          throw e;
        } finally {
          //do not put data in the cache as it may be too big.
          //recover the data value if needed when geting from cache.
          LocatedBlock lb = null;
          if (lastBlockWithStatus!= null && lastBlockWithStatus.getLastBlock() != null) {
            lb = new LocatedBlock(lastBlockWithStatus.getLastBlock().getBlock(), lastBlockWithStatus.getLastBlock().
                getLocations(), lastBlockWithStatus.getLastBlock().
                    getStorageIDs(), lastBlockWithStatus.getLastBlock().getStorageTypes(), lastBlockWithStatus.
                getLastBlock().getStartOffset(), lastBlockWithStatus.getLastBlock().isCorrupt(),
                lastBlockWithStatus.getLastBlock().getBlockToken(), lastBlockWithStatus.getLastBlock().
                getCachedLocations());
          }

          ClientNamenodeProtocolProtos.AppendResponseProto.Builder builder
              = ClientNamenodeProtocolProtos.AppendResponseProto.newBuilder();
          if (lb != null) {
            builder.setBlock(PBHelper.convert(lb));
          }
          if (lastBlockWithStatus!= null && lastBlockWithStatus.getFileStatus() != null) {
            builder.setStat(PBHelper.convert(lastBlockWithStatus.getFileStatus()));
          }
          byte[] toCache = builder.build().toByteArray();
          RetryCacheDistributed.setState(cacheEntry, success, toCache);
        }
      }
    };
    return (LastBlockWithStatus) appendFileHandler.handle(this);
  }

  private LastBlockWithStatus appendFileInt(String src, String holder,
      String clientMachine) throws
      IOException {

    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
              DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }
    LocatedBlock lb;
    HdfsFileStatus stat = null;
    FSPermissionChecker pc = getPermissionChecker();
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    lb = appendFileInternal(pc, iip, holder, clientMachine);
    stat = FSDirStatAndListingOp.getFileInfo(dir, src, false, true);
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* NameSystem.appendFile: file " + src + " for " + holder +
                " at " + clientMachine + " block " + lb.getBlock() +
                " block size " + lb.getBlock().getNumBytes());
      }
    }
    logAuditEvent(true, "append", src);
    return new LastBlockWithStatus(lb, stat);
  }

  private ExtendedBlock getExtendedBlock(Block blk) {
    return new ExtendedBlock(blockPoolId, blk);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   * <p/>
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  LocatedBlock getAdditionalBlock(final String src1, final long fileId, final String clientName,
      final ExtendedBlock previous, final Set<Node> excludedNodes,
      final List<String> favoredNodes) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler additionalBlockHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_BLOCK, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il)
                  .add(lf.getLastTwoBlocksLock(src));
            } else {
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, fileId)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il)
                  .add(lf.getLastTwoBlocksLock(fileId));
            }
            locks.add(lf.getLeaseLock(LockType.READ, clientName))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC));
          }

          @Override
          public Object performTask() throws IOException {
            long blockSize;
            int replication;
            Node clientNode;
            String clientMachine = null;

            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: "
                  + src + " inodeId " + fileId + " for " + clientName);
            }

            // Part I. Analyze the state of the file with respect to the input data.
            LocatedBlock[] onRetryBlock = new LocatedBlock[1];
            FileState fileState = analyzeFileState(src, fileId, clientName, previous, onRetryBlock);
            INodeFile pendingFile = fileState.inode;
            String src2 = fileState.path;

            if (onRetryBlock[0] != null && onRetryBlock[0].getLocations().length > 0) {
              // This is a retry. Just return the last block if having locations.
              return onRetryBlock[0];
            }

            if (pendingFile.getBlocks().length >= maxBlocksPerFile) {
              throw new IOException("File has reached the limit on maximum number of"
                  + " blocks (" + DFSConfigKeys.DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY
                  + "): " + pendingFile.getBlocks().length + " >= "
                  + maxBlocksPerFile);
            }
            blockSize = pendingFile.getPreferredBlockSize();
            clientMachine = pendingFile.getFileUnderConstructionFeature()
                .getClientMachine();
            clientNode = blockManager.getDatanodeManager().getDatanodeByHost(
              clientMachine);

            replication = pendingFile.getFileReplication();

            // Get the storagePolicyID of this file
            byte storagePolicyID = pendingFile.getStoragePolicyID();

            if (clientNode == null) {
              clientNode = getClientNode(clientMachine);
            }
            // choose targets for the new block to be allocated.
            final DatanodeStorageInfo targets[] = getBlockManager().chooseTarget4NewBlock(
                src2, replication, clientNode, excludedNodes, blockSize,
                favoredNodes, storagePolicyID);

            // Part II.
            // Allocate a new block, add it to the INode and the BlocksMap.
            Block newBlock;
            long offset;
            onRetryBlock = new LocatedBlock[1];
            fileState =
                analyzeFileState(src2, fileId, clientName, previous, onRetryBlock);
            pendingFile = fileState.inode;
            src2 = fileState.path;
            if (onRetryBlock[0] != null) {
              if (onRetryBlock[0].getLocations().length > 0) {
                // This is a retry. Just return the last block if having locations.
                return onRetryBlock[0];
              } else {
                // add new chosen targets to already allocated block and return
                BlockInfo lastBlockInFile = pendingFile.getLastBlock();
                ((BlockInfoUnderConstruction) lastBlockInFile)
                    .setExpectedLocations(targets);
                offset = pendingFile.computeFileSize();
                return makeLocatedBlock(lastBlockInFile, targets, offset);
              }
            }

            // commit the last block and complete it if it has minimum replicas
            commitOrCompleteLastBlock(pendingFile,
                ExtendedBlock.getLocalBlock(previous));

            // allocate new block, record block locations in INode.
            newBlock = createNewBlock(pendingFile);
            INodesInPath inodesInPath = INodesInPath.fromINode(pendingFile);
            saveAllocatedBlock(src2, inodesInPath, newBlock, targets);


            persistNewBlock(src2, pendingFile);
            offset = pendingFile.computeFileSize(true);

            Lease lease = leaseManager.getLease(clientName);
            lease.updateLastTwoBlocksInLeasePath(src2, newBlock,
                ExtendedBlock.getLocalBlock(previous));


            // Return located block
            LocatedBlock lb =  makeLocatedBlock(newBlock, targets, offset);
            if(pendingFile.isFileStoredInDB()){

              //appending to a file stored in the database
              //delete the file data from database and unset the flag

              pendingFile.setFileStoredInDB(false);
              pendingFile.deleteFileDataStoredInDB();
              LOG.debug("Stuffed Inode:  appending to a file stored in the database. In the current implementation there is" +
                  " potential for data loss if the client fails");
              //the data has been deleted. if the client fails to write the data on the datanodes then the data will
              // be lost. Solution. instead of deleting the data mark it and recover the data in the lease recovery
              // process.
            }
            return lb;
          }
        };
    return (LocatedBlock) additionalBlockHandler.handle(this);
  }

  /*
   * Resolve clientmachine address to get a network location path
   */
  private Node getClientNode(String clientMachine) {
    List<String> hosts = new ArrayList<String>(1);
    hosts.add(clientMachine);
    List<String> rName = getBlockManager().getDatanodeManager()
        .resolveNetworkLocation(hosts);
    Node clientNode = null;
    if (rName != null) {
      // Able to resolve clientMachine mapping.
      // Create a temp node to findout the rack local nodes
      clientNode = new NodeBase(rName.get(0) + NodeBase.PATH_SEPARATOR_STR
          + clientMachine);
    }
    return clientNode;
  }

  static class FileState {
    public final INodeFile inode;
    public final String path;
  
    public FileState(INodeFile inode, String fullPath) {
      this.inode = inode;
      this.path = fullPath;
    }
  }

  FileState analyzeFileState(String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
      throws IOException {

    checkBlock(previous);
    onRetryBlock[0] = null;
    checkNameNodeSafeMode("Cannot add block to " + src);

    // have we exceeded the configured limit of fs objects.
    checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    INode inode;
    if (fileId == INode.ROOT_PARENT_ID) {
      // Older clients may not have given us an inode ID to work with.
      // In this case, we have to try to resolve the path and hope it
      // hasn't changed or been deleted since the file was opened for write.
      final INodesInPath iip = dir.getINodesInPath4Write(src);
      inode = iip.getLastINode();
    } else {
      // Newer clients pass the inode ID, so we can just get the inode
      // directly.
      inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
      if (inode != null) src = inode.getFullPathName();
    }
    final INodeFile pendingFile = checkLease(src, clientName, inode, fileId, true);
    BlockInfo lastBlockInFile = pendingFile.getLastBlock();
    if (!Block.matchingIdAndGenStamp(previousBlock, lastBlockInFile)) {
      // The block that the client claims is the current last block
      // doesn't match up with what we think is the last block. There are
      // four possibilities:
      // 1) This is the first block allocation of an append() pipeline
      //    which started appending exactly at a block boundary.
      //    In this case, the client isn't passed the previous block,
      //    so it makes the allocateBlock() call with previous=null.
      //    We can distinguish this since the last block of the file
      //    will be exactly a full block.
      // 2) This is a retry from a client that missed the response of a
      //    prior getAdditionalBlock() call, perhaps because of a network
      //    timeout, or because of an HA failover. In that case, we know
      //    by the fact that the client is re-issuing the RPC that it
      //    never began to write to the old block. Hence it is safe to
      //    to return the existing block.
      // 3) This is an entirely bogus request/bug -- we should error out
      //    rather than potentially appending a new block with an empty
      //    one in the middle, etc
      // 4) This is a retry from a client that timed out while
      //    the prior getAdditionalBlock() is still being processed,
      //    currently working on chooseTarget().
      //    There are no means to distinguish between the first and
      //    the second attempts in Part I, because the first one hasn't
      //    changed the namesystem state yet.
      //    We run this analysis again in Part II where case 4 is impossible.

      BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
      if (previous == null &&
          lastBlockInFile != null &&
          lastBlockInFile.getNumBytes() ==
              pendingFile.getPreferredBlockSize() &&
          lastBlockInFile.isComplete()) {
        // Case 1
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.allocateBlock: handling block allocation" +
                  " writing to a file with a complete previous block: src=" +
                  src + " lastBlock=" + lastBlockInFile);
        }
      } else if (Block.matchingIdAndGenStamp(penultimateBlock, previousBlock)) {
        if (lastBlockInFile.getNumBytes() != 0) {
          throw new IOException(
              "Request looked like a retry to allocate block " +
                  lastBlockInFile + " but it already contains " +
                  lastBlockInFile.getNumBytes() + " bytes");
        }

        // Case 2
        // Return the last block.
        NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +
            "caught retry for allocation of a new block in " +
            src + ". Returning previously allocated block " + lastBlockInFile);
        long offset = pendingFile.computeFileSize(true);
        onRetryBlock[0] = makeLocatedBlock(lastBlockInFile,
            ((BlockInfoUnderConstruction) lastBlockInFile).getExpectedStorageLocations(
                getBlockManager().getDatanodeManager()),
            offset);
        return new FileState(pendingFile, src);
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }

    // Check if the penultimate block is minimally replicated
    if (!checkFileProgress(src, pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src +
              " block " + pendingFile.getPenultimateBlock());
    }
    return new FileState(pendingFile, src);
  }

  private LocatedBlock makeLocatedBlock(Block blk, DatanodeStorageInfo[] locs, long offset)
      throws IOException {
    LocatedBlock lBlk = LocatedBlock.createLocatedBlock(
        getExtendedBlock(blk), locs, offset, false);
    getBlockManager().setBlockToken(lBlk,
        BlockTokenSecretManager.AccessMode.WRITE);
    return lBlk;
  }

  /**
   * @see ClientProtocol#getAdditionalDatanode(String, ExtendedBlock, DatanodeInfo[], String[], DatanodeInfo[], int, String)
   */
  LocatedBlock getAdditionalDatanode(final String src1, final long fileId, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final String[] storageIDs,
      final HashSet<Node> excludes, final int numAdditionalNodes,
      final String clientName) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler getAdditionalDatanodeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_DATANODE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il);
            } else {
              INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, fileId)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il);
            }

            locks.add(lf.getLeaseLock(LockType.READ, clientName));
          }

          @Override
          public Object performTask() throws IOException {
            //check if the feature is enabled
            dtpReplaceDatanodeOnFailure.checkEnabled();

            Node clientnode = null;
            String clientMachine;
            final long preferredBlockSize;
            final List<DatanodeStorageInfo> chosen;
            //check safe mode
            checkNameNodeSafeMode("Cannot add datanode; src=" + src + ", blk=" + blk);
            String src2=src;
            //check lease
            final INode inode;
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              inode = dir.getINode(src);
            } else {
              inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
              if (inode != null) {
                src2 = inode.getFullPathName();
              }
            }
            final INodeFile file = checkLease(src2, clientName, inode, fileId, false);
            clientMachine = file.getFileUnderConstructionFeature().getClientMachine();
            clientnode = blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
            preferredBlockSize = file.getPreferredBlockSize();

            byte storagePolicyID = file.getStoragePolicyID();

            //find datanode storages
            final DatanodeManager dm = blockManager.getDatanodeManager();
            if(existings.length!=0){
              chosen = Arrays.asList(dm.getDatanodeStorageInfos(existings, storageIDs));
            } else {
              chosen = Collections.<DatanodeStorageInfo>emptyList();
            }

            if (clientnode == null) {
              clientnode = getClientNode(clientMachine);
            }
            
            // choose new datanodes.
            final DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
                src2, numAdditionalNodes, clientnode, chosen,
                excludes, preferredBlockSize, storagePolicyID);

            final LocatedBlock lb = new LocatedBlock(blk, targets);
            blockManager.setBlockToken(lb, AccessMode.COPY);
            return lb;
          }
        };
    return (LocatedBlock) getAdditionalDatanodeHandler.handle(this);
  }

  /**
   * The client would like to let go of the given block
   */
  boolean abandonBlock(final ExtendedBlock b, final long fileId, final String src1,
      final String holder) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler abandonBlockHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.ABANDON_BLOCK,
            src) {

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il);
            } else {
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, fileId)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
              locks.add(il);
            }
            locks.add(lf.getLeaseLock(LockType.READ))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED, src))
                    .add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR));
            locks.add(lf.getAllUsedHashBucketsLock());
          }


          @Override
          public Object performTask() throws IOException {
            //
            // Remove the block from the pending creates list
            //
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.abandonBlock: " + b + "of file " + src);
            }
            checkNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
            String src2 = src;
            final INode inode;
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              inode = dir.getINode(src);
            } else {
              inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
              if (inode != null) {
                src2 = inode.getFullPathName();
              }
            }
            final INodeFile file = checkLease(src2, holder, inode, fileId, false);

            boolean removed = dir.removeBlock(src2, file, ExtendedBlock.getLocalBlock(b));
            if (!removed) {
              return true;
            }
            leaseManager.getLease(holder).updateLastTwoBlocksInLeasePath(src2,
                file.getLastBlock(), file.getPenultimateBlock());

            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.abandonBlock: " + b +
                      " is removed from pendingCreates");
            }
            persistBlocks(src2, file);
            file.recomputeFileSize();

            return true;
          }
        };
    return (Boolean) abandonBlockHandler.handle(this);
  }

  private INodeFile checkLease(String src, String holder,
      INode inode, long fileId, boolean updateLastTwoBlocksInFile) throws
      LeaseExpiredException, StorageException,
      TransactionContextException, FileNotFoundException {
    final String ident = src + " (inode " + fileId + ")";
    if (inode == null) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": File does not exist. " +
              (lease != null ? lease.toString() :
                  "Holder " + holder + " does not have any open files."));
    }
    if (!inode.isFile()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": INode is not a regular file. "
          + (lease != null ? lease.toString()
              : "Holder " + holder + " does not have any open files."));
    }
    final INodeFile file = inode.asFile();
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + ident + ": File is not open for writing. " +
              (lease != null ? lease.toString() :
                  "Holder " + holder + " does not have any open files."));
    }
    String clientName = file.getFileUnderConstructionFeature().getClientName();
    if (holder != null && !clientName.equals(holder)) {
            throw new LeaseExpiredException("Lease mismatch on " + ident +
          " owned by " + clientName + " but is accessed by " + holder);
    }

    if(updateLastTwoBlocksInFile) {
      file.getFileUnderConstructionFeature().updateLastTwoBlocks(leaseManager.getLease(holder), src);
    }
    return file;
  }

  /**
   * Complete in-progress write to the given file.
   *
   * @return true if successful, false if the client should continue to retry
   * (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException
   *     on error (eg lease mismatch, file not open, file deleted)
   */
  boolean completeFile(final String src1, final String holder,
      final ExtendedBlock last, final long fileId, final byte[] data) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler completeFileHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.COMPLETE_FILE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            if (fileId == INode.ROOT_PARENT_ID) {
              // Older clients may not have given us an inode ID to work with.
              // In this case, we have to try to resolve the path and hope it
              // hasn't changed or been deleted since the file was opened for write.
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(!dir.isQuotaEnabled());
              locks.add(il);
            } else {
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, fileId)
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                  .skipReadingQuotaAttr(!dir.isQuotaEnabled());
              locks.add(il);
            }
            locks.add(lf.getLeaseLock(LockType.WRITE, holder))
                .add(lf.getLeasePathLock(LockType.WRITE))
                .add(lf.getBlockLock());

            if (data == null) { // the data is stored on the datanodes.
              locks.add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR, BLK.IV));
            }
          }

          @Override
          public Object performTask() throws IOException {
            checkBlock(last);
            return completeFileInternal(src, holder,
                ExtendedBlock.getLocalBlock(last), fileId, data);
          }
        };

    return (Boolean) completeFileHandler.handle(this);
  }

  private boolean completeFileInternal(String src, String holder, Block last, long fileId,
      final byte[] data)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
          src + " for " + holder);
    }
    checkNameNodeSafeMode("Cannot complete file " + src);

    if (data != null) {
      if (last != null) {
        throw new IllegalStateException(
            "Trying to store the file data in the database. Last block of the file should" +
                " have been null");
      }
      return completeFileStoredInDataBase(src, holder,fileId, data);
    } else {
      return completeFileStoredOnDataNodes(src, holder, last, fileId);
    }
  }

  private boolean completeFileStoredOnDataNodes(String src, String holder,
      Block last, long fileId)
      throws IOException {
    INodeFile pendingFile;
    try {
      final INode inode;
      if (fileId == INode.ROOT_PARENT_ID) {
        // Older clients may not have given us an inode ID to work with.
        // In this case, we have to try to resolve the path and hope it
        // hasn't changed or been deleted since the file was opened for write.
        final INodesInPath iip = dir.getLastINodeInPath(src);
        inode = iip.getINode(0);
      } else {
        inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
        if (inode != null) {
          src = inode.getFullPathName();
        }
      }
      pendingFile = checkLease(src, holder, inode, fileId, true);
    } catch (LeaseExpiredException lee) {
      final INode inode = dir.getINode(src);
      if (inode != null && inode instanceof INodeFile &&
          !inode.isUnderConstruction()) {
        // This could be a retry RPC - i.e the client tried to close
        // the file, but missed the RPC response. Thus, it is trying
        // again to close the file. If the file still exists and
        // the client's view of the last block matches the actual
        // last block, then we'll treat it as a successful close.
        // See HDFS-3031.
        final Block realLastBlock = ((INodeFile) inode).getLastBlock();
        if (Block.matchingIdAndGenStamp(last, realLastBlock)) {
          NameNode.stateChangeLog.info("DIR* completeFile: " +
              "request from " + holder + " to complete inode " + fileId +
              "(" + src + ") which is already closed. But, it appears to be " +
              "an RPC retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    // Check the state of the penultimate block. It should be completed
    // before attempting to complete the last one.
    if (!checkFileProgress(src, pendingFile, false)) {
      return false;
    }

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(src, pendingFile, true)) {
      return false;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog
        .info("DIR* completeFile: " + src + " is closed by " + holder);
    return true;
  }


  private boolean completeFileStoredInDataBase(String src, String holder, long fileId,
      final byte[] data)
      throws IOException {
    INodeFile pendingFile;
    final INode inode;
        if (fileId == INode.ROOT_PARENT_ID) {
          // Older clients may not have given us an inode ID to work with.
          // In this case, we have to try to resolve the path and hope it
          // hasn't changed or been deleted since the file was opened for write.
          inode = dir.getINode(src);
        } else {
          inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
          if (inode != null) {
            src = inode.getFullPathName();
          }
        }
        
    pendingFile = checkLease(src, holder, inode, fileId, true);

    //in case of appending to small files. we might have to migrate the file from
    //in-memory to on disk
    if(pendingFile.isFileStoredInDB()){
      pendingFile.deleteFileDataStoredInDB();
    }

    pendingFile.setFileStoredInDB(true);

    long oldSize = pendingFile.getSize();

    pendingFile.setSize(data.length);

    pendingFile.storeFileDataInDB(data);

    //update quota
    if (dir.isQuotaEnabled()) {
      //account for only newly added data
      long spaceConsumed = (data.length - oldSize) * pendingFile
          .getFileReplication();
      dir.updateSpaceConsumed(src, 0, spaceConsumed);
    }


    finalizeINodeFileUnderConstructionStoredInDB(src, pendingFile);

    NameNode.stateChangeLog
        .info("DIR* completeFile: " + src + " is closed by " + holder);
    return true;
  }


  /**
   * Save allocated block at the given pending filename
   *
   * @param src
   *     path to the file
   * @param inodesInPath representing each of the components of src.
   *                     The last INode is the INode for the file.
   * @throws QuotaExceededException
   *     If addition of block exceeds space quota
   */
  private BlockInfo saveAllocatedBlock(String src, INodesInPath inodesInPath,
      Block newBlock,
      DatanodeStorageInfo targets[]) throws IOException, StorageException {
    BlockInfo b = dir.addBlock(src, inodesInPath, newBlock, targets);
    NameNode.stateChangeLog.info("BLOCK* allocate " + b + " for " + src);
    DatanodeStorageInfo.incrementBlocksScheduled(targets);
    return b;
  }

  /**
   * Create new block with a unique block id and a new generation stamp.
   */
  private Block createNewBlock(INodeFile pendingFile)
      throws IOException {
    Block b = new Block(nextBlockId()
        , 0, 0);
    // Increment the generation stamp for every new block.
    b.setGenerationStampNoPersistance(pendingFile.nextGenerationStamp());
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkAll is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  private boolean checkFileProgress(String src, INodeFile v, boolean checkAll)
      throws IOException {
    if (checkAll) {
      // check all blocks of the file.
      for (BlockInfo block : v.getBlocks()) {
        if (!isCompleteBlock(src, block, blockManager.minReplication, v, blockManager)) {
          return false;
        }
      }
    } else {
      // check the penultimate block of this file
      BlockInfo b = v.getPenultimateBlock();
      if (b != null
            && !isCompleteBlock(src, b, blockManager.minReplication, v, blockManager)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isCompleteBlock(String src, BlockInfo b, int minRepl, INodeFile v,
      BlockManager blockManager) throws IOException {
    if (!b.isComplete()) {
      BlockInfo cBlock = blockManager
          .tryToCompleteBlock(v, b.getBlockIndex());
      if (cBlock != null) {
        b = cBlock;
      }
      if (!b.isComplete()) {
        final BlockInfoUnderConstruction uc = (BlockInfoUnderConstruction) b;
        final int numNodes = b.getStorages(blockManager.getDatanodeManager()).length;
        LOG.info("BLOCK* " + b + " is not COMPLETE (ucState = "
            + uc.getBlockUCState() + ", replication# = " + numNodes
            + (numNodes < minRepl ? " < " : " >= ")
            + " minimum = " + minRepl + ") in file " + src);
        return false;
      }
    }
    return true;
  }
    
  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////

  /** 
   * Change the indicated filename. 
   * @deprecated Use {@link #renameTo(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  boolean renameTo(String src, String dst)
      throws IOException {
    //only for testing
    saveTimes();
    boolean ret = false;
    try {
      checkNameNodeSafeMode("Cannot rename " + src);
      ret = FSDirRenameOp.renameToInt(dir, src, dst);
    } catch (AccessControlException e)  {
      logAuditEvent(false, "rename", src, dst, null);
      throw e;
    }

    return ret;
  }

  void renameTo(final String src, final String dst,
                Options.Rename... options)
      throws IOException {
    //only for testing
    saveTimes();
    Map.Entry<BlocksMapUpdateInfo, HdfsFileStatus> res = null;
    try {
      checkNameNodeSafeMode("Cannot rename " + src);
      res = FSDirRenameOp.renameToInt(dir, src, dst, options);
    } catch (AccessControlException e) {
      logAuditEvent(false, "rename (options=" + Arrays.toString(options) +
          ")", src, dst, null);
      throw e;
    }

    HdfsFileStatus auditStat = res.getValue();

    logAuditEvent(true, "rename (options=" + Arrays.toString(options) +
        ")", src, dst, auditStat);
  }

  /**
   * Remove the indicated file from namespace.
   *
   * @see ClientProtocol#delete(String, boolean) for detailed description and
   * description of exceptions
   */
  public boolean deleteWithTransaction(final String src1,
      final boolean recursive) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = dir.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler deleteHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.DELETE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                    INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il).add(lf.getLeaseLock(LockType.WRITE))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR,
                        BLK.PE, BLK.IV));
            if(isRetryCacheEnabled) {
              locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
                  Server.getCallId()));
            }

            locks.add(lf.getAllUsedHashBucketsLock());

            if (dir.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src));
            }
            if (erasureCodingEnabled) {
              locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
            }
          }

          @Override
          public Object performTask() throws IOException {
            CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
            if (cacheEntry != null && cacheEntry.isSuccess()) {
              return true; // Return previous response
            }
            boolean ret = false;
            try {
              ret = delete(src, recursive);
              return ret;
            } finally {
              RetryCacheDistributed.setState(cacheEntry, ret);
            }
          }
        };
    return (Boolean) deleteHandler.handle(this);
  }

  private boolean delete(String src, boolean recursive)
      throws
      IOException {
    try {
      return deleteInt(src, recursive);
    } catch (AccessControlException e) {
      logAuditEvent(false, "delete", src);
      throw e;
    }
  }

  private boolean deleteInt(String src, boolean recursive)
      throws
      IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }
    boolean status = deleteInternal(src, recursive, true);
    if (status) {
      logAuditEvent(true, "delete", src);
    }
    return status;
  }

  FSPermissionChecker getPermissionChecker()
      throws AccessControlException {
    return dir.getPermissionChecker();
  }

  /**
   * Remove a file/directory from the namespace.
   * <p/>
   * For large directories, deletion is incremental. The blocks under
   * the directory are collected and deleted a small number at a time.
   * <p/>
   * For small directory or file the deletion is done in one shot.
   *
   * @see ClientProtocol#delete(String, boolean) for description of exceptions
   */
  private boolean deleteInternal(String src, boolean recursive,
      boolean enforcePermission)
      throws
      IOException {
    BlocksMapUpdateInfo collectedBlocks = new BlocksMapUpdateInfo();
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot delete " + src);
    final INodesInPath iip = dir.getINodesInPath4Write(src);
    if (!recursive && dir.isNonEmptyDirectory(iip)) {
       throw new PathIsNotEmptyDirectoryException(src + " is non empty");
    }
    if (enforcePermission && isPermissionEnabled) {
      dir.checkPermission(pc, iip, false, null, FsAction.WRITE, null, FsAction.ALL, true);
    }
    long mtime = now();
    // Unlink the target directory from directory tree
    long filesRemoved = dir.delete(src, collectedBlocks,mtime);
    if (filesRemoved < 0) {      
      return false;
    }
    incrDeletedFileCount(filesRemoved);
    // Blocks/INodes will be handled later
    removePathAndBlocks(src, null);
    removeBlocks(collectedBlocks); // Incremental deletion of blocks
    collectedBlocks.clear();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* Namesystem.delete: " + src + " is removed");
    }
    return true;
  }

  /**
   * From the given list, incrementally remove the blocks from blockManager.
   * Write lock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
   * ensure that other waiters on the lock can get in. See HDFS-2938
   *
   * @param blocks
   *          An instance of {@link BlocksMapUpdateInfo} which contains a list
   *          of blocks that need to be removed from blocksMap
   */
  private void removeBlocks(BlocksMapUpdateInfo blocks)
      throws StorageException, TransactionContextException, IOException {
    List<Block> toDeleteList = blocks.getToDeleteList();
    Iterator<Block> iter = toDeleteList.iterator();
    while (iter.hasNext()) {
      for (int i = 0; i < BLOCK_DELETION_INCREMENT && iter.hasNext(); i++) {
          blockManager.removeBlock(iter.next());
      }
    }
  }

  /**
   * Remove leases and blocks related to a given path
   *
   * @param src The given path
   * @param blocks Containing the list of blocks to be deleted from blocksMap
   */
  void removePathAndBlocks(String src, BlocksMapUpdateInfo blocks) throws IOException {
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    removeBlocksAndUpdateSafemodeTotal(blocks);
  }
  
  /**
   * Removes the blocks from blocksmap and updates the safemode blocks total
   * 
   * @param blocks
   *          An instance of {@link BlocksMapUpdateInfo} which contains a list
   *          of blocks that need to be removed from blocksMap
   */
  void removeBlocksAndUpdateSafemodeTotal(BlocksMapUpdateInfo blocks) throws IOException{
    int numRemovedComplete = 0;
    List<Block> removedSafe = new ArrayList<>();
    
    for (Block b : blocks.getToDeleteList()) {
      BlockInfo bi = getStoredBlock(b);
      if (bi.isComplete()) {
        numRemovedComplete++;
        removedSafe.add(b);
      }
      blockManager.removeBlock(b);
    }
    adjustSafeModeBlockTotals(removedSafe,-numRemovedComplete);
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src1
   *     The string representation of the path to the file
   * @param resolveLink
   *     whether to throw UnresolvedLinkException
   *     if src refers to a symlink
   * @return object containing information regarding the file
   * or null if file not found
   * @throws AccessControlException
   *     if access is denied
   * @throws UnresolvedLinkException
   *     if a symlink is encountered.
   */
  public HdfsFileStatus getFileInfo(final String src, final boolean resolveLink)
      throws IOException {
    HdfsFileStatus stat = null;
    try {
      stat = FSDirStatAndListingOp.getFileInfo(dir, src, resolveLink);
    } catch (AccessControlException e) {
      logAuditEvent(false, "getfileinfo", src);
      throw e;
    } 
    logAuditEvent(true, "getfileinfo", src);
    return stat;
  }

  /**
   * Returns true if the file is closed
   */
  boolean isFileClosed(final String src)
      throws AccessControlException, UnresolvedLinkException,
      StandbyException, IOException {
    try {
      return FSDirStatAndListingOp.isFileClosed(dir, src);
    } catch (AccessControlException e) {
      logAuditEvent(false, "isFileClosed", src);
      throw e;
    } 
  }
    
  /**
   * Create all the necessary directories
   */
  boolean mkdirs(String src, PermissionStatus permissions,
      boolean createParent) throws IOException {
    HdfsFileStatus auditStat = null;
    try {
      checkNameNodeSafeMode("Cannot create directory " + src);
      auditStat = FSDirMkdirOp.mkdirs(this, src, permissions, createParent);
    } catch (AccessControlException e) {
      logAuditEvent(false, "mkdirs", src);
      throw e;
    }
    logAuditEvent(true, "mkdirs", src, null, auditStat);
    return true;
  }

  ContentSummary getContentSummary(final String src)
      throws
      IOException {
    boolean success = true;
    try {
      return FSDirStatAndListingOp.getContentSummary(dir, src);
    } catch (AccessControlException ace) {
      success = false;
      throw ace;
    } finally {
      logAuditEvent(success, "contentSummary", src);
    }
  }

  /**
   * Persist all metadata about this file.
   *
   * @param src
   *     The string representation of the path
   * @param fileId The inode ID that we're fsyncing.  Older clients will pass
   *               INodeId.GRANDFATHER_INODE_ID here.
   * @param clientName
   *     The string representation of the client
   * @param lastBlockLength
   *     The length of the last block
   *     under construction reported from client.
   * @throws IOException
   *     if path does not exist
   */
  void fsync(final String src, final long fileId, final String clientName,
      final long lastBlockLength) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.FSYNC, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        if (fileId == INode.ROOT_PARENT_ID) {
          // Older clients may not have given us an inode ID to work with.
          // In this case, we have to try to resolve the path and hope it
          // hasn't changed or been deleted since the file was opened for write.
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
              .setNameNodeID(nameNode.getId())
              .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
          locks.add(il);
        } else {
          INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, fileId)
              .setNameNodeID(nameNode.getId())
              .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
          locks.add(il);
        }
        locks.add(lf.getLeaseLock(LockType.READ, clientName))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
            .add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        NameNode.stateChangeLog
            .info("BLOCK* fsync: " + src + " for " + clientName);
        checkNameNodeSafeMode("Cannot fsync file " + src);
        final INode inode;
        String src2 = src;
        if (fileId == INode.ROOT_PARENT_ID) {
          // Older clients may not have given us an inode ID to work with.
          // In this case, we have to try to resolve the path and hope it
          // hasn't changed or been deleted since the file was opened for write.
          inode = dir.getINode(src);
        } else {
          inode = EntityManager.find(INode.Finder.ByINodeIdFTIS, fileId);
          if (inode != null) {
            src2 = inode.getFullPathName();
          }
        }
        final INodeFile pendingFile = checkLease(src2, clientName, inode, fileId, true);
        if (lastBlockLength > 0) {
          pendingFile.getFileUnderConstructionFeature().updateLengthOfLastBlock(pendingFile, lastBlockLength);
        }
        persistBlocks(src2, pendingFile);
        pendingFile.recomputeFileSize();
        return null;
      }
    }.handle(this);
  }

  /**
   * Move a file that is being written to be immutable.
   *
   * @param src
   *     The filename
   * @param lease
   *     The lease for the client creating the file
   * @param recoveryLeaseHolder
   *     reassign lease to this holder if the last block
   *     needs recovery; keep current holder if null.
   * @return true  if file has been successfully finalized and closed or
   * false if block recovery has been initiated. Since the lease owner
   * has been changed and logged, caller should call logSync().
   * @throws AlreadyBeingCreatedException
   *     if file is waiting to achieve minimal
   *     replication;<br>
   *     RecoveryInProgressException if lease recovery is in progress.<br>
   *     IOException in case of an error.
   */
  boolean internalReleaseLease(Lease lease, String src,
      String recoveryLeaseHolder)
      throws IOException {
    LOG.info("Recovering " + lease + ", src=" + src);
    assert !isInSafeMode();

    final INodeFile pendingFile = INodeFile.valueOf(dir.getINode(src), src);
    int nrBlocks = pendingFile.numBlocks();
    BlockInfo[] blocks = pendingFile.getBlocks();

    int nrCompleteBlocks;
    BlockInfo curBlock = null;
    for (nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks;
         nrCompleteBlocks++) {
      curBlock = blocks[nrCompleteBlocks];
      if (!curBlock.isComplete()) {
        break;
      }
      assert blockManager.checkMinReplication(curBlock) :
          "A COMPLETE block is not minimally replicated in " + src;
    }

    // If there are no incomplete blocks associated with this file,
    // then reap lease immediately and close the file.
    if (nrCompleteBlocks == nrBlocks) {
      finalizeINodeFileUnderConstruction(src, pendingFile);
      NameNode.stateChangeLog.warn("BLOCK*" +
          " internalReleaseLease: All existing blocks are COMPLETE," +
          " lease removed, file closed.");
      return true;  // closed!
    }

    // Only the last and the penultimate blocks may be in non COMPLETE state.
    // If the penultimate block is not COMPLETE, then it must be COMMITTED.
    if (nrCompleteBlocks < nrBlocks - 2 || nrCompleteBlocks == nrBlocks - 2 &&
        curBlock != null &&
        curBlock.getBlockUCState() != BlockUCState.COMMITTED) {
      final String message = "DIR* NameSystem.internalReleaseLease: " +
          "attempt to release a create lock on " + src +
          " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    // The last block is not COMPLETE, and
    // that the penultimate block if exists is either COMPLETE or COMMITTED
    final BlockInfo lastBlock = pendingFile.getLastBlock();
    BlockUCState lastBlockState = lastBlock.getBlockUCState();
    BlockInfo penultimateBlock = pendingFile.getPenultimateBlock();
    // If penultimate block doesn't exist then its minReplication is met
    boolean penultimateBlockMinReplication = penultimateBlock == null ? true :
          blockManager.checkMinReplication(penultimateBlock);
    
    switch (lastBlockState) {
      case COMPLETE:
        assert false : "Already checked that the last block is incomplete";
        break;
      case COMMITTED:
        // Close file if committed blocks are minimally replicated
        if (penultimateBlockMinReplication &&
            blockManager.checkMinReplication(lastBlock)) {
          finalizeINodeFileUnderConstruction(src, pendingFile);
          NameNode.stateChangeLog.warn("BLOCK*" +
              " internalReleaseLease: Committed blocks are minimally replicated," +
              " lease removed, file closed.");
          return true;  // closed!
        }
        // Cannot close file right now, since some blocks
        // are not yet minimally replicated.
        // This may potentially cause infinite loop in lease recovery
        // if there are no valid replicas on data-nodes.
        String message = "DIR* NameSystem.internalReleaseLease: " +
            "Failed to release lease for file " + src +
            ". Committed blocks are waiting to be minimally replicated." +
            " Try again later.";
        NameNode.stateChangeLog.warn(message);
        throw new AlreadyBeingCreatedException(message);
      case UNDER_CONSTRUCTION:
      case UNDER_RECOVERY:
        final BlockInfoUnderConstruction uc =
            (BlockInfoUnderConstruction) lastBlock;
        // setup the last block locations from the blockManager if not known
        if (uc.getNumExpectedLocations() == 0) {
          uc.setExpectedLocations(blockManager.getStorages(lastBlock));
        }

        if (uc.getNumExpectedLocations() == 0 && uc.getNumBytes() == 0) {
          // There is no datanode reported to this block.
          // may be client have crashed before writing data to pipeline.
          // This blocks doesn't need any recovery.
          // We can remove this block and close the file.
          pendingFile.removeLastBlock(lastBlock);
          finalizeINodeFileUnderConstruction(src, pendingFile);
          NameNode.stateChangeLog.warn("BLOCK* internalReleaseLease: "
              + "Removed empty last block and closed file.");
          return true;
        }
        // start recovery of the last block for this file
        long blockRecoveryId = pendingFile.nextGenerationStamp();
        lease = reassignLease(lease, src, recoveryLeaseHolder, pendingFile);
        uc.initializeBlockRecovery(blockRecoveryId,
            getBlockManager().getDatanodeManager());
        leaseManager.renewLease(lease);
        // Cannot close file right now, since the last block requires recovery.
        // This may potentially cause infinite loop in lease recovery
        // if there are no valid replicas on data-nodes.
        NameNode.stateChangeLog.warn("DIR* NameSystem.internalReleaseLease: " +
            "File " + src + " has not been closed." +
            " Lease recovery is in progress. " +
            "RecoveryId = " + blockRecoveryId + " for block " + lastBlock);
        break;
    }
    return false;
  }

  private Lease reassignLease(Lease lease, String src, String newHolder,
      INodeFile pendingFile)
      throws StorageException, TransactionContextException {
    if (newHolder == null) {
      return lease;
    }
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }

  private Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
      INodeFile pendingFile)
      throws StorageException, TransactionContextException {
    pendingFile.getFileUnderConstructionFeature().setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }

  private void commitOrCompleteLastBlock(
      final INodeFile fileINode, final Block commitBlock)
      throws IOException {
    Preconditions.checkArgument(fileINode.isUnderConstruction());
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
      return;
    }

    fileINode.recomputeFileSize();

    if (dir.isQuotaEnabled()) {
      final long diff = fileINode.getPreferredBlockSize()
          - commitBlock.getNumBytes();
      if (diff > 0) {
        // Adjust disk space consumption if required
        String path = fileINode.getFullPathName();
        dir.updateSpaceConsumed(path, 0,
            -diff * fileINode.getFileReplication());
      }
    }
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INodeFile pendingFile)
      throws IOException {
    finalizeINodeFileUnderConstructionInternal(src, pendingFile, false);
  }

  private void finalizeINodeFileUnderConstructionStoredInDB(String src,
      INodeFile pendingFile)
      throws IOException {
    finalizeINodeFileUnderConstructionInternal(src, pendingFile, true);
  }

  private void finalizeINodeFileUnderConstructionInternal(String src,
      INodeFile pendingFile, boolean skipReplicationChecks)
      throws IOException {
    FileUnderConstructionFeature uc = pendingFile.getFileUnderConstructionFeature();
    Preconditions.checkArgument(uc != null);
    leaseManager.removeLease(uc.getClientName(), src);
    
    // close file and persist block allocations for this file
    INodeFile newFile = pendingFile.toCompleteFile(now());
    closeFile(src, newFile);

    if (!skipReplicationChecks) {
      blockManager.checkReplication(newFile);
    }
  }

  void commitBlockSynchronization(final ExtendedBlock lastBlock,
      final long newGenerationStamp, final long newLength,
      final boolean closeFile, final boolean deleteBlock,
      final DatanodeID[] newTargets, final String[] newTargetStorages)
      throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier =
            INodeUtil.resolveINodeFromBlock(lastBlock.getLocalBlock());
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
                .add(lf.getLeaseLock(LockType.WRITE))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                .add(lf.getBlockLock(lastBlock.getBlockId(), inodeIdentifier))
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        String src;
        // If a DN tries to commit to the standby, the recovery will
        // fail, and the next retry will succeed on the new NN.

        checkNameNodeSafeMode(
          "Cannot commitBlockSynchronization while in safe mode");
        LOG.info("commitBlockSynchronization(lastBlock=" + lastBlock +
            ", newGenerationStamp=" + newGenerationStamp + ", newLength=" +
            newLength + ", newTargets=" + Arrays.asList(newTargets) +
            ", closeFile=" + closeFile + ", deleteBlock=" + deleteBlock + ")");
        final BlockInfo storedBlock =
            getStoredBlock(ExtendedBlock.getLocalBlock(lastBlock));
        if (storedBlock == null) {
          if (deleteBlock) {
            // This may be a retry attempt so ignore the failure
            // to locate the block.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Block (=" + lastBlock + ") not found");
            }
            return null;
          } else {
            throw new IOException("Block (=" + lastBlock + ") not found");
          }
        }
        //
        // The implementation of delete operation (see @deleteInternal method)
        // first removes the file paths from namespace, and delays the removal
        // of blocks to later time for better performance. When
        // commitBlockSynchronization (this method) is called in between, the
        // blockCollection of storedBlock could have been assigned to null by
        // the delete operation, throw IOException here instead of NPE; if the
        // file path is already removed from namespace by the delete operation,
        // throw FileNotFoundException here, so not to proceed to the end of
        // this method to add a CloseOp to the edit log for an already deleted
        // file (See HDFS-6825).
        //
        
        BlockCollection blockCollection = storedBlock.getBlockCollection();
        if (blockCollection == null) {
          throw new IOException("The blockCollection of " + storedBlock
              + " is null, likely because the file owning this block was"
              + " deleted and the block removal is delayed");
        }
        INodeFile iFile = ((INode)blockCollection).asFile();
        if (inodeIdentifier==null) {
          throw new FileNotFoundException("File not found: "
              + iFile.getFullPathName() + ", likely due to delayed block"
              + " removal");
        }
        if (!iFile.isUnderConstruction() || storedBlock.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Unexpected block (=" + lastBlock
                + ") since the file (=" + iFile.getLocalName()
                + ") is not under construction");
          }
          return null;
        }
        
        long recoveryId =
            ((BlockInfoUnderConstruction) storedBlock).getBlockRecoveryId();
        if (recoveryId != newGenerationStamp) {
          throw new IOException("The recovery id " + newGenerationStamp +
              " does not match current recovery id " + recoveryId +
              " for block " + lastBlock);
        }

        if (deleteBlock) {
          Block blockToDel = ExtendedBlock.getLocalBlock(lastBlock);
          boolean remove = iFile.removeLastBlock(blockToDel);
          if (remove) {
            blockManager.removeBlockFromMap(storedBlock);
          }
        } else {
          // update last block
          storedBlock.setGenerationStamp(newGenerationStamp);
          storedBlock.setNumBytes(newLength);
          iFile.recomputeFileSize();
          // find the DatanodeDescriptor objects
          // There should be no locations in the blockManager till now because the
          // file is underConstruction
          ArrayList<DatanodeDescriptor> trimmedTargets = new ArrayList<>(newTargets.length);
          ArrayList<String> trimmedStorages = new ArrayList<>(newTargets.length);

          for (int i = 0; i < newTargets.length; i++) {
            DatanodeDescriptor targetNode = blockManager.getDatanodeManager().getDatanode(newTargets[i]);
            if (targetNode != null) {
              trimmedTargets.add(targetNode);
              trimmedStorages.add(newTargetStorages[i]);
            } else if (LOG.isDebugEnabled()) {
              LOG.debug("DatanodeDescriptor (=" + newTargets[i] + ") not found");
            }
          }
          if ((closeFile) && !trimmedTargets.isEmpty()) {
            // the file is getting closed. Insert block locations into blockManager.
            // Otherwise fsck will report these blocks as MISSING, especially if the
            // blocksReceived from Datanodes take a long time to arrive.
            for (int i = 0; i < trimmedTargets.size(); i++) {
              DatanodeStorageInfo storageInfo = trimmedTargets.get(i).getStorageInfo(trimmedStorages.get(i));
              if (storageInfo != null) {
                storageInfo.addBlock(storedBlock);
              }
            }
          }
          // add pipeline locations into the INodeUnderConstruction
          DatanodeStorageInfo[] trimmedStorageInfos =
              blockManager.getDatanodeManager().getDatanodeStorageInfos(
                  trimmedTargets.toArray(new DatanodeID[trimmedTargets.size()]),
                  trimmedStorages.toArray(new String[trimmedStorages.size()]));
          iFile.setLastBlock(storedBlock, trimmedStorageInfos);
        }

        if (closeFile) {
          src = closeFileCommitBlocks(iFile, storedBlock);
        } else {
          // If this commit does not want to close the file, persist blocks
          src = iFile.getFullPathName();
          persistBlocks(src, iFile);
        }
        if (closeFile) {
          LOG.info(
              "commitBlockSynchronization(newBlock=" + lastBlock + ", file=" +
                  src + ", newGenerationStamp=" + newGenerationStamp +
                  ", newLength=" + newLength + ", newTargets=" +
                  Arrays.asList(newTargets) +
                  ") successful");
        } else {
          LOG.info("commitBlockSynchronization(" + lastBlock + ") successful");
        }
        return null;
      }

    }.handle(this);
  }

  /**
   *
   * @param pendingFile
   * @param storedBlock
   * @return Path of the file that was closed.
   * @throws IOException
   */
  @VisibleForTesting
  String closeFileCommitBlocks(INodeFile pendingFile,
      BlockInfo storedBlock)
      throws IOException {

    String src = pendingFile.getFullPathName();

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, storedBlock);

    //remove lease, close file
    finalizeINodeFileUnderConstruction(src, pendingFile);

    return src;
  }

  @VisibleForTesting
  BlockInfo getStoredBlock(Block block) throws StorageException, TransactionContextException{
    return blockManager.getStoredBlock(block);
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(final String holder) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RENEW_LEASE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getLeaseLock(LockType.WRITE, holder));
      }

      @Override
      public Object performTask() throws IOException {
        checkNameNodeSafeMode("Cannot renew lease for " + holder);
        leaseManager.renewLease(holder);
        return null;
      }
    }.handle(this);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src1
   *     the directory name
   * @param startAfter1
   *     the name to start after
   * @param needLocation
   *     if blockLocations need to be returned
   * @return a partial listing starting after startAfter
   * @throws AccessControlException
   *     if access is denied
   * @throws UnresolvedLinkException
   *     if symbolic link is encountered
   * @throws IOException
   *     if other I/O error occurred
   */
  DirectoryListing getListing(final String src, byte[] startAfter,
      final boolean needLocation)
      throws IOException {
    DirectoryListing dl = null;
    try {
      dl = FSDirStatAndListingOp.getListingInt(dir, src, startAfter,
          needLocation);
    } catch (AccessControlException e) {
      logAuditEvent(false, "listStatus", src);
      throw e;
    } 
    logAuditEvent(true, "listStatus", src);
    return dl;
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////

  /**
   * Register Datanode.
   * <p/>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p/>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes.
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode.
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   *
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode
   */
  void registerDatanode(DatanodeRegistration nodeReg) throws IOException {
    getBlockManager().getDatanodeManager().registerDatanode(nodeReg);
      checkSafeMode();
  }

  /**
   * Get registrationID for datanodes based on the namespaceID.
   *
   * @return registration ID
   * @see #registerDatanode(DatanodeRegistration)
   */
  String getRegistrationID() throws IOException {
    return Storage.getRegistrationID(StorageInfo.getStorageInfoFromDB());
  }

  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * <p/>
   * If a substantial amount of time passed since the last datanode
   * heartbeat then request an immediate block report.
   *
   * @return an array of datanode commands
   * @throws IOException
   */
  HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, long cacheCapacity, long cacheUsed, int xceiverCount,
      int xmitsInProgress, int failedVolumes) throws IOException {
    //get datanode commands
    final int maxTransfer =
        blockManager.getMaxReplicationStreams() - xmitsInProgress;

    DatanodeCommand[] cmds = blockManager.getDatanodeManager()
        .handleHeartbeat(nodeReg, reports, blockPoolId, cacheCapacity, cacheUsed, xceiverCount,
            maxTransfer, failedVolumes);

    return new HeartbeatResponse(cmds, getRollingUpgradeInfoTX());
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   *
   * @return true if there were sufficient resources available, false otherwise.
   */
  private boolean nameNodeHasResourcesAvailable() {
    return hasResourcesAvailable;
  }

  /**
   * Perform resource checks and cache the results.
   */
  void checkAvailableResources() {
    Preconditions.checkState(nnResourceChecker != null, "nnResourceChecker not initialized");
    int tries = 0; 
    Throwable lastThrowable = null;
    while (tries < maxDBTries) {
      try {
        SafeModeInfo safeMode = safeMode();
        if(safeMode!=null){
          //another namenode set safeMode to true since last time we checked
          //we need to start reading safeMode from the database for every opperation
          //until the all cluster get out of safemode.
          forceReadTheSafeModeFromDB.set(true);
        }
        if (!nnResourceChecker.hasAvailablePrimarySpace()) {
          //Database resources are in between preThreshold and Threshold
          //that means that soon enough we will hit the resources threshold
          //therefore, we enable reading the safemode variable from the
          //database, since at any point from now on, the leader will go to
          //safe mode.
          forceReadTheSafeModeFromDB.set(true);
        }
        
        hasResourcesAvailable = nnResourceChecker.hasAvailableSpace();
        break;
      } catch (StorageException e) {
        LOG.warn("StorageException in checkAvailableResources.", e);
        if (e instanceof TransientStorageException) {
          continue; //do not count TransientStorageException as a failled try
        }
        lastThrowable = e;
        tries++;
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {
          // Deliberately ignore
        }
      } catch (Throwable t) {
        LOG.error("Runtime exception in checkAvailableResources. ", t);
        lastThrowable = t;
        tries++;
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {
          // Deliberately ignore
        }
      }
    }
    if (tries > maxDBTries) {
      terminate(1, lastThrowable);
    }
  }

  /**
   * Persist the block list for the inode.
   * @param path
   * @param file
   * @param logRetryCache
   */
  private void persistBlocks(String path, INodeFile file) throws IOException {
    Preconditions.checkArgument(file.isUnderConstruction());
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistBlocks: " + path
              + " with " + file.getBlocks().length + " blocks is persisted to" +
              " the file system");
    }
  }
  void incrDeletedFileCount(long count) {
    NameNode.getNameNodeMetrics().incrFilesDeleted(count);
  }
  /**
   * Close file.
   * @param path
   * @param file
   */
  private void closeFile(String path, INodeFile file) throws IOException {
    // file is closed
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("closeFile: "
              +path+" with "+ file.getBlocks().length
              +" blocks is persisted to the file system");
    }
    file.logMetadataEvent(MetadataLogEntry.Operation.ADD);
  }

  /**
   * Add the given symbolic link to the fs. Record it in the edits log.
   * @param path
   * @param target
   * @param dirPerms
   * @param createParent
   * @param dir
   */
  private INodeSymlink addSymlink(String path, String target,
                                  PermissionStatus dirPerms,
                                  boolean createParent)
      throws UnresolvedLinkException, FileAlreadyExistsException,
      QuotaExceededException, AclException, IOException {
    final long modTime = now();
    if (createParent) {
      final String parent = new Path(path).getParent().toString();
      if (!FSDirMkdirOp.mkdirsRecursively(dir, parent, dirPerms, true, modTime)) {
        return null;
      }
    }
    final String userName = dirPerms.getUserName();
    long id = IDsGeneratorFactory.getInstance().getUniqueINodeID();
    INodeSymlink newNode = dir.addSymlink(id, path, target, modTime, modTime,
            new PermissionStatus(userName, null, FsPermission.getDefault()));
    if (newNode == null) {
      NameNode.stateChangeLog.info("addSymlink: failed to add " + path);
      return null;
    }
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("addSymlink: " + path + " is added");
    }
    return newNode;
  }
  
  /**
   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and
   * if
   * there are found to be insufficient resources available, causes the NN to
   * enter safe mode. If resources are later found to have returned to
   * acceptable levels, this daemon will cause the NN to exit safe mode.
   */
  class NameNodeResourceMonitor implements Runnable {
    boolean shouldNNRmRun = true;

    @Override
    public void run() {
      try {
        while (fsRunning && shouldNNRmRun) {
          checkAvailableResources();
          if (!nameNodeHasResourcesAvailable()) {
            String lowResourcesMsg = "NameNode's database low on available " +
                "resources.";
            if (!isInSafeMode()) {
              LOG.warn(lowResourcesMsg + "Entering safe mode.");
            } else {
              LOG.warn(lowResourcesMsg + "Already in safe mode.");
            }
            enterSafeMode(true);
          }
          try {
            Thread.sleep(resourceRecheckInterval);
          } catch (InterruptedException ie) {
            // Deliberately ignore
          }
        }
      } catch (Exception e) {
        FSNamesystem.LOG.error("Exception in NameNodeResourceMonitor: ", e);
      }
    }

    public void stopMonitor() {
      shouldNNRmRun = false;
    }
  }


  private void checkBlock(ExtendedBlock block) throws IOException {
    if (block != null && !this.blockPoolId.equals(block.getBlockPoolId())) {
      throw new IOException(
          "Unexpected BlockPoolId " + block.getBlockPoolId() + " - expected " +
              blockPoolId);
    }
  }

  @Metric({"MissingBlocks", "Number of missing blocks"})
  public long getMissingBlocksCount() throws IOException {
    // not locking
    return blockManager.getMissingBlocksCount();
  }

  @Metric({"MissingReplOneBlocks", "Number of missing blocks " +
      "with replication factor 1"})
  public long getMissingReplOneBlocksCount() throws IOException {
    // not locking
    return blockManager.getMissingReplOneBlocksCount();
  }
  
  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();
  }

  /**
   * @see ClientProtocol#getStats()
   */
  long[] getStats() throws IOException {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] = getUnderReplicatedBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] = getCorruptReplicaBlocks();
    stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] = getMissingBlocksCount();
    stats[ClientProtocol.GET_STATS_MISSING_REPL_ONE_BLOCKS_IDX] =
        getMissingReplOneBlocksCount();
    return stats;
  }

  @Override // FSNamesystemMBean
  @Metric({"CapacityTotal", "Total raw capacity of data nodes in bytes"})
  public long getCapacityTotal() {
    return datanodeStatistics.getCapacityTotal();
  }

  @Metric({"CapacityTotalGB", "Total raw capacity of data nodes in GB"})
  public float getCapacityTotalGB() {
    return DFSUtil.roundBytesToGB(getCapacityTotal());
  }

  @Override // FSNamesystemMBean
  @Metric(
      {"CapacityUsed", "Total used capacity across all data nodes in bytes"})
  public long getCapacityUsed() {
    return datanodeStatistics.getCapacityUsed();
  }

  @Metric({"CapacityUsedGB", "Total used capacity across all data nodes in GB"})
  public float getCapacityUsedGB() {
    return DFSUtil.roundBytesToGB(getCapacityUsed());
  }

  @Override // FSNamesystemMBean
  @Metric({"CapacityRemaining", "Remaining capacity in bytes"})
  public long getCapacityRemaining() {
    return datanodeStatistics.getCapacityRemaining();
  }

  @Metric({"CapacityRemainingGB", "Remaining capacity in GB"})
  public float getCapacityRemainingGB() {
    return DFSUtil.roundBytesToGB(getCapacityRemaining());
  }

  @Metric({"CapacityUsedNonDFS",
      "Total space used by data nodes for non DFS purposes in bytes"})
  public long getCapacityUsedNonDFS() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  /**
   * Total number of connections.
   */
  @Override // FSNamesystemMBean
  @Metric
  public int getTotalLoad() {
    return datanodeStatistics.getXceiverCount();
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    return getBlockManager().getDatanodeManager().getDatanodeListForReport(type)
        .size();
  }

  DatanodeInfo[] datanodeReport(final DatanodeReportType type)
      throws AccessControlException {
    checkSuperuserPrivilege();
    final DatanodeManager dm = getBlockManager().getDatanodeManager();
    final List<DatanodeDescriptor> results = dm.getDatanodeListForReport(type);

    DatanodeInfo[] arr = new DatanodeInfo[results.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new DatanodeInfo(results.get(i));
    }
    return arr;
  }

  DatanodeStorageReport[] getDatanodeStorageReport(final DatanodeReportType type
  ) throws AccessControlException, StandbyException {
    checkSuperuserPrivilege();
    final DatanodeManager dm = getBlockManager().getDatanodeManager();
    final List<DatanodeDescriptor> datanodes = dm.getDatanodeListForReport(type);

    DatanodeStorageReport[] reports = new DatanodeStorageReport[datanodes.size()];
    for (int i = 0; i < reports.length; i++) {
      final DatanodeDescriptor d = datanodes.get(i);
      reports[i] = new DatanodeStorageReport(new DatanodeInfo(d), d.getStorageReports());
    }
    return reports;
  }

  Date getStartTime() {
    return new Date(startTime);
  }

  void refreshNodes() throws IOException {
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager()
        .refreshNodes(new HdfsConfiguration());
  }

  void setBalancerBandwidth(long bandwidth) throws IOException {
    checkSuperuserPrivilege();
    getBlockManager().getDatanodeManager().setBalancerBandwidth(bandwidth);
  }
  
  /**
   * Persist the new block (the last block of the given file).
   * @param path
   * @param file
   */
  private void persistNewBlock(String path, INodeFile file) throws IOException {
    Preconditions.checkArgument(file.isUnderConstruction());
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("persistNewBlock: "
              + path + " with new block " + file.getLastBlock().toString()
              + ", current total block count is " + file.getBlocks().length);
    }
  }

  /**
   * SafeModeInfo contains information related to the safe mode.
   * <p/>
   * An instance of {@link SafeModeInfo} is created when the name node
   * enters safe mode.
   * <p/>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <em>safe blocks</em>, those that have at least the minimal number of
   * replicas, and calculates the ratio of safe blocks to the total number
   * of blocks in the system, which is the size of blocks in
   * {@link FSNamesystem#blockManager}. When the ratio reaches the
   * {@link #threshold} it starts the SafeModeMonitor daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p/>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction, boolean)
   */
  public class SafeModeInfo {

    // configuration fields
    /**
     * Safe mode threshold condition %.
     */
    private double threshold;
    /**
     * Safe mode minimum number of datanodes alive
     */
    private int datanodeThreshold;
    /**
     * Safe mode extension after the threshold.
     */
    private int extension;
    /**
     * Min replication required by safe mode.
     */
    private int safeReplication;
    /**
     * threshold for populating needed replication queues
     */
    private double replicationQueueThreshold;

    // internal fields
    /**
     * Time when threshold was reached.
     * <p/>
     * <br> -1 safe mode is off
     * <br> 0 safe mode is on, and threshold is not reached yet
     * <br> >0 safe mode is on, but we are in extension period 
     */
    private long reached() throws IOException{
      return HdfsVariables.getSafeModeReached();
    }
    /**
     * time of the last status printout
     */
    private long lastStatusReport = 0;
    /**
     * Was safe mode entered automatically because available resources were low.
     */
    private boolean resourcesLow = false;
    /** counter for tracking startup progress of reported blocks */
    private Counter awaitingReportedBlocksCounter;

    public ThreadLocal<Boolean> safeModePendingOperation =
        new ThreadLocal<>();

    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *
     * @param conf
     *     configuration
     */
    private SafeModeInfo(Configuration conf) throws IOException {
      this.threshold = conf.getFloat(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
          DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
      if (threshold > 1.0) {
        LOG.warn("The threshold value should't be greater than 1, threshold: " +
            threshold);
      }
      this.datanodeThreshold =
          conf.getInt(DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
              DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 0);
      this.safeReplication = conf.getInt(DFS_NAMENODE_REPLICATION_MIN_KEY,
          DFS_NAMENODE_REPLICATION_MIN_DEFAULT);
      if (this.safeReplication > 1) {
        LOG.warn("Only safe replication 1 is supported");
        this.safeReplication = 1;
      }

      LOG.info(DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY + " = " + threshold);
      LOG.info(
          DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY + " = " + datanodeThreshold);
      LOG.info(DFS_NAMENODE_SAFEMODE_EXTENSION_KEY + "     = " + extension);

      // default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
      this.replicationQueueThreshold =
          conf.getFloat(DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
              (float) threshold);
      HdfsVariables.setBlockTotal(0);
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually, or because
     * available resources are low.
     * <p/>
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     *
     * @see SafeModeInfo
     */
    private SafeModeInfo(boolean resourcesLow) throws IOException {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replicationQueueThreshold = 1.5f; // can never be reached
      HdfsVariables.setBlockTotal(-1);
      this.resourcesLow = resourcesLow;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

    private SafeModeInfo(double threshold, int datanodeThreshold, int extension, int safeReplication,
        double replicationQueueThreshold, boolean resourcesLow) throws IOException {
      this.threshold = threshold;
      this.datanodeThreshold = datanodeThreshold;
      this.extension = extension;
      this.safeReplication = safeReplication;
      this.replicationQueueThreshold = replicationQueueThreshold;
      this.resourcesLow = resourcesLow;
    }

    private SafeModeInfo(){};
    
    public double getThreshold() {
      return threshold;
    }

    public int getDatanodeThreshold() {
      return datanodeThreshold;
    }

    public int getExtension() {
      return extension;
    }

    public int getSafeReplication() {
      return safeReplication;
    }

    public double getReplicationQueueThreshold() {
      return replicationQueueThreshold;
    }

    public long getLastStatusReport() {
      return lastStatusReport;
    }

    public boolean isResourcesLow() {
      return resourcesLow;
    }

    public long getReached() throws IOException{
      return reached();
    }
    
    /**
     * Check if safe mode is on.
     *
     * @return true if in safe mode
     */
    private boolean isOn() throws IOException {
      doConsistencyCheck();
      return this.reached() >= 0;
    }

    /**
     * Enter safe mode.
     */
    private void enter() throws IOException {
      if(isLeader()){
        LOG.info("enter safe mode");
        HdfsVariables.setSafeModeReached(0);
      }
    }

    /**
     * Leave safe mode.
     * <p/>
     * Check for invalid, under- & over-replicated blocks in the end of
     * startup.
     */
    private void leave() throws IOException {
      if(!inSafeMode.getAndSet(false)){
        return;
      }
      forceReadTheSafeModeFromDB.set(false);
      // if not done yet, initialize replication queues.
      // In the standby, do not populate replication queues
      if (!isPopulatingReplQueues() && shouldPopulateReplicationQueues()) {
        initializeReplQueues();
      }
      
      leaveInternal();

      clearSafeBlocks();
    }

    private void leaveInternal() throws IOException {
      long timeInSafeMode = now() - startTime;
      NameNode.stateChangeLog.info(
          "STATE* Leaving safe mode after " + timeInSafeMode / 1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafeMode);

      //Log the following only once (when transitioning from ON -> OFF)
      if (reached() >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF");
      }
      if(isLeader()){
        HdfsVariables.exitSafeMode();
      }
      final NetworkTopology nt =
          blockManager.getDatanodeManager().getNetworkTopology();
      NameNode.stateChangeLog.info(
          "STATE* Network topology has " + nt.getNumOfRacks() + " racks and " +
              nt.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has " +
          blockManager.numOfUnderReplicatedBlocks() + " blocks");

      startSecretManagerIfNecessary();
      // If startup has not yet completed, end safemode phase.
      StartupProgress prog = NameNode.getStartupProgress();
      if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
        prog.endStep(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS);
        prog.endPhase(Phase.SAFEMODE);
      }
    }

    /**
     * Check whether we have reached the threshold for
     * initializing replication queues.
     */
    private boolean canInitializeReplicationQueues() throws IOException {
      return shouldPopulateReplicationQueues() &&
          blockSafe() >= blockReplicationQueueThreshold();
    }

    /**
     * Safe mode can be turned off iff
     * another namenode went out of safe mode or
     * the threshold is reached and
     * the extension time have passed.
     *
     * @return true if can leave or false otherwise.
     */
    private boolean canLeave() throws IOException {
      if(!isLeader()){
        return false;
      }
      if (reached() == 0) {
        return false;
      }
      if (now() - reached() < extension) {
        reportStatus("STATE* Safe mode ON, in safe mode extension.", false);
        return false;
      }
      if (needEnter()) {
        reportStatus("STATE* Safe mode ON, thresholds not met.", false);
        return false;
      }
      return true;
    }

    /**
     * This NameNode tries to help the cluster to get out of safe mode by
     * updating the safe block count.
     * This call will trigger the @link{SafeModeMonitor} if it's not already
     * started.
     * @throws IOException
     */
    private void tryToHelpToGetOut() throws IOException{
      //if the cluster was set in safe mode by the admin we should wait for the admin to get out of safe mode
      //if the cluster has low resources we should wait for the resources to increase to get out of safe mode
      //if the namenode is the leader it should get out by other means.
      if (isManual() || areResourcesLow() || isLeader()) {
        return;
      }
      checkMode();
    }

    /**
     * The cluster already left safe mode, now it's time to for this namenode
     * to leave as well.
     * @throws IOException
     */
    private void clusterLeftSafeModeAlready() throws IOException {
      leaveInternal();
    }

    /**
     * There is no need to enter safe mode
     * if DFS is empty or {@link #threshold} == 0 or another namenode already
     * went out of safe mode
     */
    private boolean needEnter() throws IOException {
      return (threshold != 0 && blockSafe() < blockThreshold()) ||
          (datanodeThreshold != 0 && getNumLiveDataNodes() < datanodeThreshold) ||
          (!nameNodeHasResourcesAvailable());
    }

    /**
     * Check and trigger safe mode if needed.
     */

    private void checkMode() throws IOException {
      if(!isLeader() && !(reached()>=0)){
        return;
      }
      // if smmthread is already running, the block threshold must have been 
      // reached before, there is no need to enter the safe mode again
      if (smmthread == null && needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplicationQueues() && !isPopulatingReplQueues()) { 
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached or was reached before
      if (!isOn() ||                           // safe mode is off
          extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(); // leave safe mode
        return;
      }
      if (reached() > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      if(isLeader()){
        HdfsVariables.setSafeModeReached(now());
      }
      startSafeModeMonitor();

      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to initialize replication queues
      if (canInitializeReplicationQueues() && !isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }

    private synchronized void startSafeModeMonitor() throws IOException{
      if (smmthread == null) {
        smmthread = new Daemon(new SafeModeMonitor());
        smmthread.start();
        reportStatus("STATE* Safe mode extension entered.", true);
      }
    }

    /**
     * Set total number of blocks.
     */
    private synchronized void setBlockTotal(int total) throws IOException {
      int blockTotal = total;
      int blockThreshold = (int) (blockTotal * threshold);
      int blockReplicationQueueThreshold = (int) (blockTotal * replicationQueueThreshold);
      HdfsVariables.setBlockTotal(blockTotal, blockThreshold, blockReplicationQueueThreshold);
      checkMode();
    }

    private synchronized void updateBlockTotal(int deltaTotal) throws IOException {
      HdfsVariables.updateBlockTotal(deltaTotal, threshold, replicationQueueThreshold);
      setSafeModePendingOperation(true);
    }
    
    /**
     * Increment number of safe blocks if current block has
     * reached minimal replication.
     *
     * @param blk
     *     current block
     */
    private void incrementSafeBlockCount(short replication, Block blk) throws IOException {
      if (replication == safeReplication) {
        int added = addSafeBlock(blk.getBlockId());

        // Report startup progress only if we haven't completed startup yet.
        //todo this will not work with multiple NN
        StartupProgress prog = NameNode.getStartupProgress();
        if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
          if (this.awaitingReportedBlocksCounter == null) {
            this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE,
                STEP_AWAITING_REPORTED_BLOCKS);
          }
          this.awaitingReportedBlocksCounter.add(added);
        }

        setSafeModePendingOperation(true);
      }
    }

    /**
     * Decrement number of safe blocks if current block has
     * fallen below minimal replication.
     * @param blk
     *     current block
     * @param replication
     *     current replication
     */
    private void decrementSafeBlockCount(Block blk, short replication)
        throws IOException {
      if (replication == safeReplication - 1) {
        removeSafeBlock(blk.getBlockId());
        setSafeModePendingOperation(true);
      }
    }

    /**
     * Check if safe mode was entered manually or automatically (at startup, or
     * when disk space is low).
     */
    private boolean isManual() {
      return extension == Integer.MAX_VALUE;
    }

    /**
     * Set manual safe mode.
     */
    private synchronized void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * Check if safe mode was entered due to resources being low.
     */
    private boolean areResourcesLow() {
      return resourcesLow;
    }

    /**
     * Set that resources are low for this instance of safe mode.
     */
    private void setResourcesLow() {
      resourcesLow = true;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() throws IOException{
      if(!isOn()){
        return "Safe mode is OFF.";
      }

      //Manual OR low-resource safemode. (Admin intervention required)
      String adminMsg = "It was turned on manually. ";
      if (areResourcesLow()) {
        adminMsg = "Resources are low on NN. Please add or free up more "
          + "resources then turn off safe mode manually. NOTE:  If you turn off"
          + " safe mode before adding resources, "
          + "the NN will immediately return to safe mode. ";
      }
      if (isManual() || areResourcesLow()) {
        return adminMsg
          + "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
      }

      boolean thresholdsMet = true;
      int numLive = getNumLiveDataNodes();
      String msg = "";

      int blockSafe;
      int blockThreshold;
      int blockTotal;
      try {
        blockSafe = blockSafe();
        blockThreshold = blockThreshold();
        blockTotal = blockTotal();
      } catch (IOException ex) {
        LOG.error(ex);
        return "got exception " + ex.getMessage();
      }
      if (blockSafe < blockThreshold) {
        msg += String.format(
          "The reported blocks %d needs additional %d"
          + " blocks to reach the threshold %.4f of total blocks %d.%n",
          blockSafe, (blockThreshold - blockSafe) + 1, threshold, blockTotal);
        thresholdsMet = false;
      } else {
        msg += String.format("The reported blocks %d has reached the threshold" +
            " %.4f of total blocks %d. ", blockSafe, threshold, blockTotal);
      }
      if (numLive < datanodeThreshold) {
        msg += String.format(
          "The number of live datanodes %d needs an additional %d live "
          + "datanodes to reach the minimum number %d.%n",
          numLive, (datanodeThreshold - numLive), datanodeThreshold);
        thresholdsMet = false;
      } else {
          msg += String.format("The number of live datanodes %d has reached " +
                      "the minimum number %d. ",
                      numLive, datanodeThreshold);
      }
      long reached = reached();
      msg += (reached > 0) ? "In safe mode extension. " : "";
      msg += "Safe mode will be turned off automatically ";
      
      if (!thresholdsMet) {
        msg += "once the thresholds have been reached.";
      } else if (reached + extension - now() > 0) {
        msg += ("in " + (reached + extension - now()) / 1000 + " seconds.");
      } else {
        msg += "soon.";
      }
      
      return msg;    
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow)  throws IOException{
      long curTime = now();
      if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) {
        return;
      }
      NameNode.stateChangeLog.error(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    @Override
    public String toString() {
      String blockSafe;
      long blockThreshold=-1;
      long reached = -1;
      try {
        blockSafe = "" + blockSafe();
        blockThreshold = blockThreshold();
        reached = reached();
      } catch (IOException ex) {
        blockSafe = ex.getMessage();
      }
      String resText =
          "Current safe blocks = " + blockSafe + ". Target blocks = " +
              blockThreshold + " for threshold = %" + threshold +
              ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) {
        resText += " Threshold was reached " + new Date(reached) + ".";
      }
      return resText;
    }

    /**
     * Checks consistency of the class state.
     * This is costly so only runs if asserts are enabled.
     */
    private void doConsistencyCheck() throws IOException {
      boolean assertsOn = false;
      assert assertsOn = true; // set to true if asserts are on
      if (!assertsOn) {
        return;
      }
      
      long blockTotal = blockTotal();
      
      if (blockTotal == -1 /*&& blockSafe == -1*/) {
        return; // manual safe mode
      }
      long blockSafe = blockSafe();
      int activeBlocks = blockManager.getActiveBlockCount();
      if ((blockTotal != activeBlocks) &&
          !(blockSafe >= 0 && blockSafe <= blockTotal)) {
        throw new AssertionError(" SafeMode: Inconsistent filesystem state: " +
            "SafeMode data: blockTotal=" + blockTotal + " blockSafe=" +
            blockSafe + "; " + "BlockManager data: active=" + activeBlocks);
      }
    }

    private void adjustBlockTotals(List<Block> deltaSafe, int deltaTotal)
        throws IOException {
      int oldBlockSafe = 0;
      if (LOG.isDebugEnabled()) {
        oldBlockSafe = blockSafe();
      }
      
      if (deltaSafe != null) {
        for (Block b : deltaSafe) {
          removeSafeBlock(b.getBlockId());
        }
      }
      
      if (LOG.isDebugEnabled()) {
        int newBlockSafe = blockSafe();
        long blockTotal = blockTotal();
        LOG.debug("Adjusting block totals from " + oldBlockSafe + "/" + blockTotal + " to " + newBlockSafe + "/"
            + (blockTotal + deltaTotal));
      }
            
      updateBlockTotal(deltaTotal);
    }

    private void setSafeModePendingOperation(Boolean val) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SafeModeX Some operation are put on hold");
      }
      safeModePendingOperation.set(val);
    }

    private void adjustSafeBlocks(Set<Long> safeBlocks) throws IOException {
      int added = addSafeBlocks(new ArrayList<Long>(safeBlocks));
      if (LOG.isDebugEnabled()) {
        long blockTotal = blockTotal();
        LOG.debug("Adjusting safe blocks, added " + added +" blocks");
      }
      
      StartupProgress prog = NameNode.getStartupProgress();
        if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
          if (this.awaitingReportedBlocksCounter == null) {
            this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE,
              STEP_AWAITING_REPORTED_BLOCKS);
          }
          this.awaitingReportedBlocksCounter.add(added);
        }

      setSafeModePendingOperation(true);

      checkMode();
    }

    private void performSafeModePendingOperation() throws IOException {
      if (safeModePendingOperation.get() != null) {
        if (safeModePendingOperation.get()) {
          LOG.debug("SafeMode about to perform pending safe mode operation");
          safeModePendingOperation.set(false);
          checkMode();
        }
      }
    }

    /**
     * Get number of safe blocks from the database
     * @return
     * @throws IOException
     */
    int blockSafe() throws IOException {
      return getBlockSafe();
    }
    
    /**
     * Get number of total blocks from the database
     * @return
     * @throws IOException
     */
    int blockTotal() throws IOException {
      return HdfsVariables.getBlockTotal();
    }
    
    /**
     * Get blockReplicationQueueThreshold from the database
     * @return
     * @throws IOException
     */
    int blockReplicationQueueThreshold() throws IOException {
      return HdfsVariables.getBlockReplicationQueueThreshold();
    }
    
    /**
     * Get blockThreshold from the database
     * @return
     * @throws IOException
     */
    int blockThreshold() throws IOException {
      return HdfsVariables.getBlockThreshold();
    }
  }

  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   */
  class SafeModeMonitor implements Runnable {
    /**
     * interval in ms for checking safe mode: {@value}
     */
    private static final long recheckInterval = 1000;

    /**
     */
    @Override
    public void run() {
      try {
        while (fsRunning) {
          SafeModeInfo safeMode = safeMode();
          if (safeMode == null) { // Not in safe mode.
            break;
          }
          if (safeMode.canLeave()) {
            // Leave safe mode.
            safeMode.leave();
            smmthread = null;
            break;
          }
          try {
            Thread.sleep(recheckInterval);
          } catch (InterruptedException ie) {
          }
        }
        if (!fsRunning) {
          LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
        }
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }
  }

  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch (action) {
        case SAFEMODE_LEAVE: // leave safe mode
          leaveSafeMode();
          break;
        case SAFEMODE_ENTER: // enter safe mode
          enterSafeMode(false);
          break;
        default:
          LOG.error("Unexpected safe mode action");
      }
    }
    return isInSafeMode();
  }

  @Override
  public void checkSafeMode() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = safeMode();
    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  @Override
  public boolean isInSafeMode() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = safeMode();
    if (safeMode == null) {
      if(inSafeMode.get()){
        new SafeModeInfo().leave();
      }
      forceReadTheSafeModeFromDB.set(false);
      return false;
    }
    if(safeMode.isOn() && !isLeader()){
      safeMode.tryToHelpToGetOut();
    }
    inSafeMode.set(safeMode.isOn());
    return safeMode.isOn();
  }

  private SafeModeInfo safeMode() throws IOException{
    if(!forceReadTheSafeModeFromDB.get()){
      return null;
    }
    
    List<Object> vals = HdfsVariables.getSafeModeFromDB();
    if(vals==null || vals.isEmpty()){
      return null;
    }
    return new FSNamesystem.SafeModeInfo((double) vals.get(0),
        (int) vals.get(1), (int) vals.get(2),
        (int) vals.get(3), (double) vals.get(4),
        (int) vals.get(5) == 0 ? true : false);
  }
  
  @Override
  public boolean isInStartupSafeMode() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = safeMode();
    if (safeMode == null) {
        return false;
    }
    // If the NN is in safemode, and not due to manual / low resources, we
    // assume it must be because of startup. If the NN had low resources during
    // startup, we assume it came out of startup safemode and it is now in low
    // resources safemode
    return !safeMode.isManual() && !safeMode.areResourcesLow()
      && safeMode.isOn();
  }

  /**
   * Check if replication queues are to be populated
   * @return true when node is HAState.Active and not in the very first safemode
   */
  @Override
  public boolean isPopulatingReplQueues() throws IOException {
    if (!shouldPopulateReplicationQueues()) {
      return false;
    }
    return initializedReplQueues;
  }

  private boolean shouldPopulateReplicationQueues() {
    return shouldPopulateReplicationQueue;
  }

  @Override
  public void incrementSafeBlockCount(int replication, BlockInfo blk) throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = safeMode();
    if (safeMode == null) {
      return;
    }
    safeMode.incrementSafeBlockCount((short)replication, blk);
  }

  @Override
  public void decrementSafeBlockCount(BlockInfo b)
      throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode();
    if (safeMode == null) // mostly true
    {
      return;
    }
    if (b.isComplete()) {
      safeMode.decrementSafeBlockCount(b,
          (short) blockManager.countNodes(b).liveReplicas());
    }
  }

  /**
   * Adjust the total number of blocks safe and expected during safe mode.
   * If safe mode is not currently on, this is a no-op.
   *
   * @param deltaSafe
   *     the change in number of safe blocks
   * @param deltaTotal
   *     the change i number of total blocks expected
   */
  @Override
  public void adjustSafeModeBlockTotals(List<Block> deltaSafe, int deltaTotal)
      throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode();
    if (safeMode == null) {
      return;
    }
    safeMode.adjustBlockTotals(deltaSafe, deltaTotal);
  }

  /**
   * Set the total number of blocks in the system.
   */
  private void setBlockTotal() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode();
    if (safeMode == null) {
      return;
    }
    safeMode.setBlockTotal(blockManager.getTotalCompleteBlocks());
  }
  
  @Override // NameNodeMXBean
  public long getCacheCapacity() {
    return datanodeStatistics.getCacheCapacity();
  }

  @Override // NameNodeMXBean
  public long getCacheUsed() {
    return datanodeStatistics.getCacheUsed();
  }

  /**
   * Get the total number of blocks in the system.
   */
  @Override // FSNamesystemMBean
  @Metric
  public long getBlocksTotal() throws IOException {
    return blockManager.getTotalBlocks();
  }

  /**
   * Get the total number of COMPLETE blocks in the system.
   * For safe mode only complete blocks are counted.
   */
  private long getCompleteBlocksTotal() throws IOException {

    // Calculate number of blocks under construction
    long numUCBlocks = leaseManager.getNumUnderConstructionBlocks();
    return getBlocksTotal() - numUCBlocks;
  }

  /**
   * Enter safe mode. If resourcesLow is false, then we assume it is manual
   *
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    stopSecretManager();
    shouldPopulateReplicationQueue = true;
    inSafeMode.set(true);    
    forceReadTheSafeModeFromDB.set(true);
    
    //if the resource are low put the cluster in SafeMode even if not the leader.
    if(!resourcesLow && !isLeader()){
      return;
    }
    
    SafeModeInfo safeMode = safeMode();
    if (safeMode != null) {
      if (resourcesLow) {
        safeMode.setResourcesLow();
      } else {
        safeMode.setManual();
      }
    }
    if (safeMode == null || !isInSafeMode()) {
      safeMode = new SafeModeInfo(resourcesLow);
    }
    if (resourcesLow) {
      safeMode.setResourcesLow();
    } else {
      safeMode.setManual();
    }
    HdfsVariables.setSafeModeInfo(safeMode, 0);
    NameNode.stateChangeLog
        .info("STATE* Safe mode is ON" + safeMode.getTurnOffTip());
  }

  /**
   * Leave safe mode.
   *
   * @throws IOException
   */
  void leaveSafeMode() throws IOException {
    if (!isInSafeMode()) {
      NameNode.stateChangeLog.info("STATE* Safe mode is already OFF");
      return;
    }
    SafeModeInfo safeMode = safeMode();
    if(safeMode!=null){
      safeMode.leave();
    }
  }

  String getSafeModeTip() throws IOException {
    if (!isInSafeMode()) {
      return "";
    }
    // There is no need to take readLock.
    // Don't use isInSafeMode as this.safeMode might be set to null.
    // after isInSafeMode returns.
    boolean inSafeMode;
    SafeModeInfo safeMode = safeMode();
    if (safeMode == null) {
      inSafeMode = false;
    } else {
      if(safeMode.isOn() && !isLeader()){
      safeMode.tryToHelpToGetOut();
      }
      inSafeMode = safeMode.isOn();
    }
    if (!inSafeMode) {
      return "";
    } else {
      return safeMode.getTurnOffTip();
    }
  }

  public void processIncrementalBlockReport(DatanodeRegistration nodeReg, StorageReceivedDeletedBlocks r)
      throws IOException {
      blockManager.processIncrementalBlockReport(nodeReg, r);
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), superGroup,
        permission);
  }

  @Override
  public void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 &&
        maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
          maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system.
   */
  @Override // FSNamesystemMBean
  public long getMaxObjects() {
    return maxFsObjects;
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getFilesTotal() {
    try {
      return this.dir.totalInodes();
    } catch (Exception ex) {
      LOG.error(ex);
      return -1;
    }
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getPendingReplicationBlocks() {
    return blockManager.getPendingReplicationBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getUnderReplicatedBlocks() {
    return blockManager.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns number of blocks with corrupt replicas
   */
  @Metric({"CorruptBlocks", "Number of blocks with corrupt replicas"})
  public long getCorruptReplicaBlocks() {
    return blockManager.getCorruptReplicaBlocksCount();
  }

  @Override // FSNamesystemMBean
  @Metric
  public long getScheduledReplicationBlocks() {
    return blockManager.getScheduledReplicationBlocksCount();
  }

  @Override
  @Metric
  public long getPendingDeletionBlocks() throws IOException {
    return blockManager.getPendingDeletionBlocksCount();
  }

  @Override
  public long getBlockDeletionStartTime() {
    return startTime + blockManager.getStartupDelayBlockDeletionInMs();
  }

  @Metric
  public long getExcessBlocks() throws IOException {
    return blockManager.getExcessBlocksCount();
  }

  // HA-only metric
  @Metric
  public long getPostponedMisreplicatedBlocks() {
    return blockManager.getPostponedMisreplicatedBlocksCount();
  }

  @Metric
  public int getBlockCapacity() {
    return blockManager.getCapacity();
  }

  @Override // FSNamesystemMBean
  public String getFSState() throws IOException {
    return isInSafeMode() ? "safeMode" : "Operational";
  }

  private ObjectName mbeanName;
  private ObjectName mxbeanName;

  /**
   * Register the FSNamesystem MBean using the name
   * "hadoop:service=NameNode,name=FSNamesystemState"
   */
  private void registerMBean() {
    // We can only implement one MXBean interface, so we keep the old one.
    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeans.register("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad MBean setup", e);
    }

    LOG.info("Registered FSNamesystemState MBean");
  }

  /**
   * shutdown FSNamesystem
   */
  void shutdown() {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
      mbeanName = null;
    }
    if (mxbeanName != null) {
      MBeans.unregister(mxbeanName);
      mxbeanName = null;
    }
    if (blockManager != null) {
      blockManager.shutdown();
    }
  }

  @Override // FSNamesystemMBean
  @Metric({"LiveDataNodes",
      "Number of datanodes marked as live"})
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  @Metric({"DeadDataNodes",
      "Number of datanodes marked dead due to delayed heartbeat"})
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }

  @Override // FSNamesystemMBean
  public int getNumDecomLiveDataNodes() {
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(live, null, true);
    int liveDecommissioned = 0;
    for (DatanodeDescriptor node : live) {
      liveDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return liveDecommissioned;
  }

  @Override // FSNamesystemMBean
  public int getNumDecomDeadDataNodes() {
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    getBlockManager().getDatanodeManager().fetchDatanodes(null, dead, true);
    int deadDecommissioned = 0;
    for (DatanodeDescriptor node : dead) {
      deadDecommissioned += node.isDecommissioned() ? 1 : 0;
    }
    return deadDecommissioned;
  }

  @Override // FSNamesystemMBean
  public int getNumDecommissioningDataNodes() {
    return getBlockManager().getDatanodeManager().getDecommissioningNodes()
        .size();
  }

  @Override // FSNamesystemMBean
  @Metric({"StaleDataNodes",
      "Number of datanodes marked stale due to delayed heartbeat"})
  public int getNumStaleDataNodes() {
    return getBlockManager().getDatanodeManager().getNumStaleNodes();
  }

  /**
   * Storages are marked as "content stale" after NN restart or fails over and
   * before NN receives the first Heartbeat followed by the first Blockreport.
   */
  @Override // FSNamesystemMBean
  public int getNumStaleStorages() {
      return getBlockManager().getDatanodeManager().getNumStaleStorages();
  }

  private long nextBlockId() throws IOException{
    checkNameNodeSafeMode("Cannot get next block ID");
    return IDsGeneratorFactory.getInstance().getUniqueBlockID();
  }
    
  private INodeFile checkUCBlock(ExtendedBlock block,
      String clientName) throws IOException {
    checkNameNodeSafeMode("Cannot get a new generation stamp and an "
        + "access token for block " + block);

    // check stored block state
    BlockInfo storedBlock =
        getStoredBlock(ExtendedBlock.getLocalBlock(block));
    if (storedBlock == null ||
        storedBlock.getBlockUCState() != BlockUCState.UNDER_CONSTRUCTION) {
      throw new IOException(block +
          " does not exist or is not under Construction" + storedBlock);
    }

    // check file inode
    INodeFile file = (INodeFile) storedBlock.getBlockCollection();
    if (file == null || !file.isUnderConstruction()) {
      throw new IOException("The file " + storedBlock +
          " belonged to does not exist or it is not under construction.");
    }

    // check lease
    if (clientName == null || !clientName.equals(file.getFileUnderConstructionFeature().getClientName())) {
      throw new LeaseExpiredException("Lease mismatch: " + block +
          " is accessed by a non lease holder " + clientName);
    }

    return file;
  }

  /**
   * Client is reporting some bad block locations.
   */
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    NameNode.stateChangeLog.info("*DIR* reportBadBlocks");
    for (LocatedBlock block : blocks) {
      ExtendedBlock blk = block.getBlock();
      DatanodeInfo[] nodes = block.getLocations();
      String[] storageIDs = block.getStorageIDs();
      for (int j = 0; j < nodes.length; j++) {
        blockManager.findAndMarkBlockAsCorrupt(blk, nodes[j],
            storageIDs == null ? null: storageIDs[j],
            "client machine reported it");
      }
    }
  }

  /**
   * Get a new generation stamp together with an access token for
   * a block under construction
   * <p/>
   * This method is called for recovering a failed pipeline or setting up
   * a pipeline to append to a block.
   *
   * @param block
   *     a block
   * @param clientName
   *     the name of a client
   * @return a located block with a new generation stamp and an access token
   * @throws IOException
   *     if any error occurs
   */
  LocatedBlock updateBlockForPipeline(final ExtendedBlock block,
      final String clientName) throws IOException {
    HopsTransactionalRequestHandler updateBlockForPipelineHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.UPDATE_BLOCK_FOR_PIPELINE) {
          INodeIdentifier inodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            Block b = block.getLocalBlock();
            inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
                .add(lf.getBlockLock(block.getBlockId(), inodeIdentifier));
          }

          @Override
          public Object performTask() throws IOException {
            LocatedBlock locatedBlock;
            // check validity of parameters
            checkUCBlock(block, clientName);

            INodeFile pendingFile = (INodeFile) EntityManager
                .find(INode.Finder.ByINodeIdFTIS, inodeIdentifier.getInodeId());

            // get a new generation stamp and an access token
            block.setGenerationStamp(pendingFile.nextGenerationStamp());
            locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
            blockManager.setBlockToken(locatedBlock, AccessMode.WRITE);

            return locatedBlock;
          }
        };
    return (LocatedBlock) updateBlockForPipelineHandler.handle(this);
  }

  /**
   * Update a pipeline for a block under construction
   *
   * @param clientName
   *     the name of the client
   * @param oldBlock
   *     and old block
   * @param newBlock
   *     a new block with a new generation stamp and length
   * @param newNodes
   *     datanodes in the pipeline
   * @throws IOException
   *     if any error occurs
   */
  void updatePipeline(final String clientName, final ExtendedBlock oldBlock,
      final ExtendedBlock newBlock, final DatanodeID[] newNodes, final String[] newStorageIDs)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.UPDATE_PIPELINE) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        Block b = oldBlock.getLocalBlock();
        inodeIdentifier = INodeUtil.resolveINodeFromBlock(b);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add( lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier, true))
                .add(lf.getLeaseLock(LockType.READ))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                .add(lf.getBlockLock(oldBlock.getBlockId(), inodeIdentifier))
                .add(lf.getBlockRelated(BLK.UC))
                .add(lf.getLastBlockHashBucketsLock());
        if(isRetryCacheEnabled) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {
          checkNameNodeSafeMode("Pipeline not updated");
          assert newBlock.getBlockId() == oldBlock.getBlockId() :
              newBlock + " and " + oldBlock + " has different block identifier";

          LOG.info("updatePipeline(" + oldBlock.getLocalBlock()
             + ", newGS=" + newBlock.getGenerationStamp()
              + ", newLength=" + newBlock.getNumBytes()
              + ", newNodes=" + Arrays.asList(newNodes)
              + ", client=" + clientName
              + ")");

          updatePipelineInternal(clientName, oldBlock, newBlock, newNodes,
              newStorageIDs);
          LOG.info("updatePipeline(" + oldBlock.getLocalBlock() + " => "
              + newBlock.getLocalBlock() + ") success");
          success = true;
          return null;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }

      }
    }.handle(this);
  }

  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException, StorageException {
    // check the vadility of the block and lease holder name
    final INodeFile pendingFile =
        checkUCBlock(oldBlock, clientName);

    pendingFile.getFileUnderConstructionFeature().updateLastTwoBlocks(leaseManager.getLease(clientName));

    final BlockInfoUnderConstruction blockInfo =
        (BlockInfoUnderConstruction) pendingFile.getLastBlock();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= blockInfo.getGenerationStamp() ||
        newBlock.getNumBytes() < blockInfo.getNumBytes()) {
      String msg = "Update " + oldBlock + " (len = " +
          blockInfo.getNumBytes() + ") to an older state: " + newBlock +
          " (len = " + newBlock.getNumBytes() + ")";
      LOG.warn(msg);
      throw new IOException(msg);
    }


    //Make sure the hashes are corrected to avoid leaving stale replicas behind
    for (DatanodeStorageInfo oldLocation :
        blockInfo.getStorages(blockManager.getDatanodeManager())){
      HashBuckets.getInstance().undoHash(oldLocation.getSid(),
          HdfsServerConstants.ReplicaState.FINALIZED, oldBlock.getLocalBlock());
    }

    // Update old block with the new generation stamp and new length
    blockInfo.setNumBytes(newBlock.getNumBytes());
    blockInfo.setGenerationStampAndVerifyReplicas(newBlock.getGenerationStamp(), blockManager.getDatanodeManager());
    pendingFile.recomputeFileSize();

    // find the DatanodeStorageInfo objects
    final DatanodeStorageInfo[] storages = blockManager.getDatanodeManager()
        .getDatanodeStorageInfos(newNodes, newStorageIDs);
    blockInfo.setExpectedLocations(storages);
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  void unprotectedChangeLease(String src, String dst)
      throws StorageException, TransactionContextException {
    leaseManager.changeLease(src, dst);
  }

  static class CorruptFileBlockInfo {
    String path;
    Block block;

    CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }

    @Override
    public String toString() {
      return block.getBlockName() + "\t" + path;
    }
  }

  /**
   * @param path
   *     Restrict corrupt files to this portion of namespace.
   * @param cookieTab
   *     Support for continuation; the set of files we return
   *     back is ordered by blockId; startBlockAfter tells where to start from
   * @return a list in which each entry describes a corrupt file/block
   * @throws AccessControlException
   * @throws IOException
   */
  Collection<CorruptFileBlockInfo> listCorruptFileBlocks(final String path,
      String[] cookieTab) throws IOException {
    checkSuperuserPrivilege();
    
    final int[] count = {0};
    final ArrayList<CorruptFileBlockInfo> corruptFiles = new ArrayList<>();
    if (cookieTab == null) {
      cookieTab = new String[]{null};
    }
    // Do a quick check if there are any corrupt files without taking the lock
    if (blockManager.getMissingBlocksCount() == 0) {
      if (cookieTab[0] == null) {
        cookieTab[0] = String.valueOf(getIntCookie(cookieTab[0]));
      }
      LOG.info("there are no corrupt file blocks.");
      return corruptFiles;
    }

    if (!isPopulatingReplQueues()) {
      throw new IOException("Cannot run listCorruptFileBlocks because " +
          "replication queues have not been initialized.");
    }
    // print a limited # of corrupt files per call

    final Iterator<Block> blkIterator =
        blockManager.getCorruptReplicaBlockIterator();

    final int[] skip = {getIntCookie(cookieTab[0])};
    for (int i = 0; i < skip[0] && blkIterator.hasNext(); i++) {
      blkIterator.next();
    }

    HopsTransactionalRequestHandler listCorruptFileBlocksHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.LIST_CORRUPT_FILE_BLOCKS) {
          INodeIdentifier iNodeIdentifier;

          @Override
          public void setUp() throws StorageException {
            Block block = (Block) getParams()[0];
            iNodeIdentifier = INodeUtil.resolveINodeFromBlock(block);
          }

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            Block block = (Block) getParams()[0];
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndividualINodeLock(INodeLockType.READ_COMMITTED,
                iNodeIdentifier, true))
                .add(lf.getBlockLock(block.getBlockId(), iNodeIdentifier))
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER));
          }

          @Override
          public Object performTask() throws IOException {
            Block blk = (Block) getParams()[0];
            INode inode = (INodeFile) blockManager.getBlockCollection(blk);
            skip[0]++;
            if (inode != null &&
                blockManager.countNodes(blk).liveReplicas() == 0) {
              String src = FSDirectory.getFullPathName(inode);
              if (src.startsWith(path)) {
                corruptFiles.add(new CorruptFileBlockInfo(src, blk));
                count[0]++;
              }
            }
            return null;
          }
        };

    while (blkIterator.hasNext()) {
      Block blk = blkIterator.next();
      listCorruptFileBlocksHandler.setParams(blk);
      listCorruptFileBlocksHandler.handle(this);
      if (count[0] >= DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED) {
        break;
      }
    }

    cookieTab[0] = String.valueOf(skip[0]);
    LOG.info("list corrupt file blocks returned: " + count[0]);
    return corruptFiles;
  }

  /**
   * Convert string cookie to integer.
   */
  private static int getIntCookie(String cookie) {
    int c;
    if (cookie == null) {
      c = 0;
    } else {
      try {
        c = Integer.parseInt(cookie);
      } catch (NumberFormatException e) {
        c = 0;
      }
    }
    c = Math.max(0, c);
    return c;
  }

  /**
   * Create delegation token secret manager
   */
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
      Configuration conf) {
    return new DelegationTokenSecretManager(
        conf.getLong(DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT),
        conf.getLong(DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT),
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL,
        conf.getBoolean(DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
            DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT), this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   *
   * @return delegation token secret manager object
   */
  DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  Token<DelegationTokenIdentifier> getDelegationToken(final Text renewer)
      throws IOException {
    //FIXME This does not seem to be persisted
    HopsTransactionalRequestHandler getDelegationTokenHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_DELEGATION_TOKEN) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws IOException {
            Token<DelegationTokenIdentifier> token;
            checkNameNodeSafeMode("Cannot issue delegation token");
            if (!isAllowedDelegationTokenOp()) {
              throw new IOException(
                  "Delegation Token can be issued only with kerberos or web authentication");
            }
            if (dtSecretManager == null || !dtSecretManager.isRunning()) {
              LOG.warn("trying to get DT with no secret manager running");
              return null;
            }

            UserGroupInformation ugi = getRemoteUser();
            String user = ugi.getUserName();
            Text owner = new Text(user);
            Text realUser = null;
            if (ugi.getRealUser() != null) {
              realUser = new Text(ugi.getRealUser().getUserName());
            }
            DelegationTokenIdentifier dtId =
                new DelegationTokenIdentifier(owner, renewer, realUser);
            token = new Token<>(dtId, dtSecretManager);
            long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
            return token;
          }
        };
    return (Token<DelegationTokenIdentifier>) getDelegationTokenHandler
        .handle(this);
  }

  /**
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  long renewDelegationToken(final Token<DelegationTokenIdentifier> token)
      throws IOException {
    //FIXME This does not seem to be persisted
    HopsTransactionalRequestHandler renewDelegationTokenHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.RENEW_DELEGATION_TOKEN) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws IOException {
            long expiryTime;
            checkNameNodeSafeMode("Cannot renew delegation token");
            if (!isAllowedDelegationTokenOp()) {
              throw new IOException(
                  "Delegation Token can be renewed only with kerberos or web authentication");
            }
            String renewer = getRemoteUser().getShortUserName();
            expiryTime = dtSecretManager.renewToken(token, renewer);
            DelegationTokenIdentifier id = new DelegationTokenIdentifier();
            ByteArrayInputStream buf =
                new ByteArrayInputStream(token.getIdentifier());
            DataInputStream in = new DataInputStream(buf);
            id.readFields(in);
            return expiryTime;
          }
        };
    return (Long) renewDelegationTokenHandler.handle(this);
  }

  /**
   * @param token
   * @throws IOException
   */
  void cancelDelegationToken(final Token<DelegationTokenIdentifier> token)
      throws IOException {
    //FIXME This does not seem to be persisted
    HopsTransactionalRequestHandler cancelDelegationTokenHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.CANCEL_DELEGATION_TOKEN) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws IOException {
            checkNameNodeSafeMode("Cannot cancel delegation token");
            String canceller = getRemoteUser().getUserName();
            DelegationTokenIdentifier id =
                dtSecretManager.cancelToken(token, canceller);
            return null;
          }
        };
    cancelDelegationTokenHandler.handle(this);
  }


  /**
   * Log the updateMasterKey operation to edit logs
   *
   * @param key
   *     new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {

    assert !isInSafeMode() :
        "this should never be called while in safe mode, since we stop " +
            "the DT manager before entering safe mode!";
    // No need to hold FSN lock since we don't access any internal
    // structures, and this is stopped before the FSN shuts itself
    // down, etc.
  }

  /**
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled() &&
        (authMethod != AuthenticationMethod.KERBEROS) &&
        (authMethod != AuthenticationMethod.KERBEROS_SSL) &&
        (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }

  /**
   * Returns authentication method used to establish the connection
   *
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = getRemoteUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  /**
   * Client invoked methods are invoked over RPC and will be in
   * RPC call context even if the client exits.
   */
  boolean isExternalInvocation() {
    return Server.isRpcInvocation() ||
        NamenodeWebHdfsMethods.isWebHdfsInvocation();
  }

  private static InetAddress getRemoteIp() {
    InetAddress ip = Server.getRemoteIp();
    if (ip != null) {
      return ip;
    }
    return NamenodeWebHdfsMethods.getRemoteIp();
  }

  // optimize ugi lookup for RPC operations to avoid a trip through
  // UGI.getCurrentUser which is synced
  private static UserGroupInformation getRemoteUser() throws IOException {
    return NameNode.getRemoteUser();
  }

  /**
   * Log fsck event in the audit log
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (isAuditEnabled()) {
      logAuditEvent(true, getRemoteUser(), remoteAddress, "fsck", src, null,
          null);
    }
  }

  /**
   * Register NameNodeMXBean
   */
  private void registerMXBean() {
    mxbeanName = MBeans.register("NameNode", "NameNodeInfo", this);
  }

  /**
   * Class representing Namenode information for JMX interfaces
   */
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() throws IOException {
    if (!this.isInSafeMode()) {
      return "";
    }
    return "Safe mode is ON. " + this.getSafeModeTip();
  }

//  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    throw new UnsupportedOperationException("HOP: Upgrade is not supported");
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return datanodeStatistics.getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return datanodeStatistics.getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public long getBlockPoolUsedSpace() {
    return datanodeStatistics.getBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentBlockPoolUsed() {
    return datanodeStatistics.getPercentBlockPoolUsed();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return datanodeStatistics.getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() throws IOException {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  @Metric
  public long getTotalFiles() {
    return getFilesTotal();
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocks() throws IOException {
    return getMissingBlocksCount();
  }

  @Override // NameNodeMXBean
  public long getNumberOfMissingBlocksWithReplicationFactorOne() throws IOException {
    return getMissingReplOneBlocksCount();
  }

  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() throws IOException {
    final Map<String, Map<String, Object>> info =
        new HashMap<>();
    final List<DatanodeDescriptor> live = new ArrayList<>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (DatanodeDescriptor node : live) {
      Map<String, Object> innerInfo = ImmutableMap.<String, Object>builder()
          .put("infoAddr", node.getInfoAddr())
          .put("infoSecureAddr", node.getInfoSecureAddr())
          .put("xferaddr", node.getXferAddr())
          .put("lastContact", getLastContact(node))
          .put("usedSpace", getDfsUsed(node))
          .put("adminState", node.getAdminState().toString())
          .put("nonDfsUsedSpace", node.getNonDfsUsed())
          .put("capacity", node.getCapacity())
          .put("numBlocks", node.numBlocks())
          .put("version", node.getSoftwareVersion())
          .put("used", node.getDfsUsed())
          .put("remaining", node.getRemaining())
          .put("blockScheduled", node.getBlocksScheduled())
          .put("blockPoolUsed", node.getBlockPoolUsed())
          .put("blockPoolUsedPercent", node.getBlockPoolUsedPercent())
          .put("volfails", node.getVolumeFailures())
          .build();
      info.put(node.getHostName() + ":" + node.getXferPort(), innerInfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Map<String, Object>> info =
        new HashMap<>();
    final List<DatanodeDescriptor> dead = new ArrayList<>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
    for (DatanodeDescriptor node : dead) {
      Map<String, Object> innerInfo = ImmutableMap.<String, Object>builder()
          .put("lastContact", getLastContact(node))
          .put("decommissioned", node.isDecommissioned())
          .put("xferaddr", node.getXferAddr())
          .build();
      info.put(node.getHostName() + ":" + node.getXferPort(), innerInfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decommissioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info =
        new HashMap<>();
    final List<DatanodeDescriptor> decomNodeList =
        blockManager.getDatanodeManager().getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      Map<String, Object> innerInfo = ImmutableMap
          .<String, Object> builder()
          .put("xferaddr", node.getXferAddr())
          .put("underReplicatedBlocks",
              node.decommissioningStatus.getUnderReplicatedBlocks())
          .put("decommissionOnlyReplicas",
              node.decommissioningStatus.getDecommissionOnlyReplicas())
          .put("underReplicateInOpenFiles",
              node.decommissioningStatus.getUnderReplicatedInOpenFiles())
          .build();
      info.put(node.getHostName() + ":" + node.getXferPort(), innerInfo);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor aliveNode) {
    return (Time.now() - aliveNode.getLastUpdate()) / 1000;
  }

  private long getDfsUsed(DatanodeDescriptor aliveNode) {
    return aliveNode.getDfsUsed();
  }

  @Override  // NameNodeMXBean
  public String getClusterId() {
    String cid = "";
    try {
      cid = StorageInfo.getStorageInfoFromDB().getClusterID();
    } catch (IOException e) {
    }
    return cid;

  }

  @Override  // NameNodeMXBean
  public String getBlockPoolId() {
    return blockPoolId;
  }

//  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    throw new UnsupportedOperationException(
        "HOP: there are no name dirs any more");
  }

  @Override // NameNodeMXBean
  public String getNodeUsage() {
    float median = 0;
    float max = 0;
    float min = 0;
    float dev = 0;

    final Map<String, Map<String,Object>> info =
        new HashMap<String, Map<String,Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);

    if (live.size() > 0) {
      float totalDfsUsed = 0;
      float[] usages = new float[live.size()];
      int i = 0;
      for (DatanodeDescriptor dn : live) {
        usages[i++] = dn.getDfsUsedPercent();
        totalDfsUsed += dn.getDfsUsedPercent();
      }
      totalDfsUsed /= live.size();
      Arrays.sort(usages);
      median = usages[usages.length / 2];
      max = usages[usages.length - 1];
      min = usages[0];

      for (i = 0; i < usages.length; i++) {
        dev += (usages[i] - totalDfsUsed) * (usages[i] - totalDfsUsed);
      }
      dev = (float) Math.sqrt(dev / usages.length);
    }

    final Map<String, Object> innerInfo = new HashMap<String, Object>();
    innerInfo.put("min", StringUtils.format("%.2f%%", min));
    innerInfo.put("median", StringUtils.format("%.2f%%", median));
    innerInfo.put("max", StringUtils.format("%.2f%%", max));
    innerInfo.put("stdDev", StringUtils.format("%.2f%%", dev));
    info.put("nodeUsage", innerInfo);

    return JSON.toString(info);
  }

  @Override  // NameNodeMXBean
  public String getNNStarted() {
    return getStartTime().toString();
  }

  @Override  // NameNodeMXBean
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() +
        " from " + VersionInfo.getBranch();
  }

  /**
   * @return the block manager.
   */
  public BlockManager getBlockManager() {
    return blockManager;
  }

  /** @return the FSDirectory. */
  public FSDirectory getFSDirectory() {
    return dir;
  }
  /** Set the FSDirectory. */
  @VisibleForTesting
  public void setFSDirectory(FSDirectory dir) {
    this.dir = dir;
  }
  /** @return the cache manager. */
  public CacheManager getCacheManager() {
    return cacheManager;
  }

  @Override  // NameNodeMXBean
  public String getCorruptFiles() {
    List<String> list = new ArrayList<String>();
    Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks;
    try {
      corruptFileBlocks = listCorruptFileBlocks("/", null);
      int corruptFileCount = corruptFileBlocks.size();
      if (corruptFileCount != 0) {
        for (FSNamesystem.CorruptFileBlockInfo c : corruptFileBlocks) {
          list.add(c.toString());
        }
      }
    } catch (IOException e) {
      LOG.warn("Get corrupt file blocks returned error: " + e.getMessage());
    }
    return JSON.toString(list);
  }

  @Override  //NameNodeMXBean
  public int getDistinctVersionCount() {
    return blockManager.getDatanodeManager().getDatanodesSoftwareVersions()
      .size();
  }

  @Override  //NameNodeMXBean
  public Map<String, Integer> getDistinctVersions() {
    return blockManager.getDatanodeManager().getDatanodesSoftwareVersions();
  }

  @Override  //NameNodeMXBean
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }

  @Override  //NameNodeMXBean
  public int getNumNameNodes() {
    return nameNode.getActiveNameNodes().size();
  }

  @Override //NameNodeMXBean
  public String getLeaderNameNode(){
    return nameNode.getActiveNameNodes().getSortedActiveNodes().get(0).getHostname();
  }

  /**
   * Verifies that the given identifier and password are valid and match.
   *
   * @param identifier
   *     Token identifier.
   * @param password
   *     Password in the token.
   */
  public synchronized void verifyToken(DelegationTokenIdentifier identifier,
      byte[] password) throws InvalidToken, RetriableException {
    try {
      getDelegationTokenSecretManager().verifyToken(identifier, password);
    } catch (InvalidToken it) {
      if (inTransitionToActive()) {
        throw new RetriableException(it);
      }
      throw it;
    }
  }

  @Override
  public boolean isGenStampInFuture(Block block) throws StorageException {
    throw new UnsupportedOperationException("Not supported anymore.");
  }

  @VisibleForTesting
  public SafeModeInfo getSafeModeInfoForTests() throws IOException {
    return safeMode();
  }

  @VisibleForTesting
  public void setNNResourceChecker(NameNodeResourceChecker nnResourceChecker) {
    this.nnResourceChecker = nnResourceChecker;
  }

  RollingUpgradeInfo queryRollingUpgrade() throws IOException {
    checkSuperuserPrivilege();
    return getRollingUpgradeInfoTX();
  }
  
  RollingUpgradeInfo startRollingUpgrade() throws IOException {
    checkSuperuserPrivilege();
    long startTime = now();
    checkNameNodeSafeMode("Failed to start rolling upgrade");
    RollingUpgradeInfo rollingUpgradeInfo = startRollingUpgradeInternal(startTime);

    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(true, "startRollingUpgrade", null, null, null);
    }
    return rollingUpgradeInfo;
  }

  /**
   * Update internal state to indicate that a rolling upgrade is in progress.
   *
   * @param startTime
   */
  RollingUpgradeInfo startRollingUpgradeInternal(final long startTime) throws IOException {
    return (RollingUpgradeInfo) new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ROLLING_UPGRADE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        checkRollingUpgrade("start rolling upgrade");
        return setRollingUpgradeInfo(startTime);
      }
    }.handle();
  }
  
  RollingUpgradeInfo setRollingUpgradeInfo(long startTime) throws TransactionContextException, StorageException {
    RollingUpgradeInfo rollingUpgradeInfo = new RollingUpgradeInfo(blockPoolId, startTime, 0L);
    HdfsVariables.setRollingUpgradeInfo(rollingUpgradeInfo);
    return rollingUpgradeInfo;
  }
  
  public RollingUpgradeInfo getRollingUpgradeInfoTX() throws IOException {
    return (RollingUpgradeInfo) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROLLING_UPGRADE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.READ));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return HdfsVariables.getRollingUpgradeInfo();
      }
    }.handle();
  }
    
  @Override  // NameNodeMXBean
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus() {
    RollingUpgradeInfo upgradeInfo = null;
    try{
      upgradeInfo = getRollingUpgradeInfoTX();
    }catch(IOException ex){
      LOG.warn(ex);
    }
    if (upgradeInfo != null) {
      return new RollingUpgradeInfo.Bean(upgradeInfo);
    }
    return null;
  }
  
  /** Is rolling upgrade in progress? */
  public boolean isRollingUpgrade() throws TransactionContextException, StorageException, InvalidProtocolBufferException {
    return HdfsVariables.getRollingUpgradeInfo()!= null;
  }

  public boolean isRollingUpgradeTX() throws IOException {
    return (boolean) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROLLING_UPGRADE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.READ));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return isRollingUpgrade();
      }
    }.handle();
  }
    
  void checkRollingUpgrade(String action) throws RollingUpgradeException, TransactionContextException, StorageException, InvalidProtocolBufferException {
    if (isRollingUpgrade()) {
      throw new RollingUpgradeException("Failed to " + action
          + " since a rolling upgrade is already in progress."
          + " Existing rolling upgrade info:\n" + HdfsVariables.getRollingUpgradeInfo());
    }
  }
  
  RollingUpgradeInfo finalizeRollingUpgrade() throws IOException {
    checkSuperuserPrivilege();
    final RollingUpgradeInfo returnInfo;
    checkNameNodeSafeMode("Failed to finalize rolling upgrade");
    returnInfo = finalizeRollingUpgradeInternal(now());
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(true, "finalizeRollingUpgrade", null, null, null);
    }
    return returnInfo;
  }
  
  RollingUpgradeInfo finalizeRollingUpgradeInternal(final long finalizeTime)
      throws RollingUpgradeException, IOException {
    return (RollingUpgradeInfo) new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ROLLING_UPGRADE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        if (!isRollingUpgrade()) {
          throw new RollingUpgradeException(
              "Failed to finalize rolling upgrade since there is no rolling upgrade in progress.");
        }
        final long startTime = HdfsVariables.getRollingUpgradeInfo().getStartTime();
        HdfsVariables.setRollingUpgradeInfo(null);
        return new RollingUpgradeInfo(blockPoolId, startTime, finalizeTime);
      }
    }.handle();
  }
  
  static void rollBackRollingUpgradeTX()
      throws RollingUpgradeException, IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ROLLING_UPGRADE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        HdfsVariables.setRollingUpgradeInfo(null);
        return null;
      }
    }.handle();
  }
    
  long addCacheDirective(CacheDirectiveInfo directive,
                         EnumSet<CacheFlag> flags)
      throws IOException {
    
    CacheDirectiveInfo effectiveDirective = null;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    
    try {
      
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache directive", safeMode());
      }
      effectiveDirective = FSNDNCacheOp.addCacheDirective(this, dir, cacheManager,
          directive, flags);
    } finally {
      boolean success = effectiveDirective != null;

      String effectiveDirectiveStr = effectiveDirective != null ?
          effectiveDirective.toString() : null;
      logAuditEvent(success, "addCacheDirective", effectiveDirectiveStr,
          null, null);
    }
    return effectiveDirective != null ? effectiveDirective.getId() : 0;
  }

  void modifyCacheDirective(CacheDirectiveInfo directive,
      EnumSet<CacheFlag> flags) throws IOException {
    boolean success = false;
    if (!flags.contains(CacheFlag.FORCE)) {
      cacheManager.waitForRescanIfNeeded();
    }
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache directive", safeMode());
      }
      FSNDNCacheOp.modifyCacheDirective(this, dir, cacheManager, directive, flags);
      success = true;
    } finally {
      String idStr = "{id: " + directive.getId().toString() + "}";
      logAuditEvent(success, "modifyCacheDirective", idStr,
          directive.toString(), null);
    }
  }

  void removeCacheDirective(long id) throws IOException {
    boolean success = false;
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot remove cache directives", safeMode());
      }
      FSNDNCacheOp.removeCacheDirective(this, cacheManager, id);
      success = true;
    } finally {
      String idStr = "{id: " + Long.toString(id) + "}";
      logAuditEvent(success, "removeCacheDirective", idStr, null,
          null);
    }
  }

  BatchedListEntries<CacheDirectiveEntry> listCacheDirectives(
      long startId, CacheDirectiveInfo filter) throws IOException {
    BatchedListEntries<CacheDirectiveEntry> results;
    cacheManager.waitForRescanIfNeeded();
    boolean success = false;
    try {
      results = FSNDNCacheOp.listCacheDirectives(this, cacheManager, startId,
          filter);
      success = true;
    } finally {
      logAuditEvent(success, "listCacheDirectives", filter.toString(), null,
          null);
    }
    return results;
  }
  
  void addCachePool(CachePoolInfo req)
      throws IOException {
    boolean success = false;
    String poolInfoStr = null;
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot add cache pool " + req.getPoolName(), safeMode());
      }
      CachePoolInfo info = FSNDNCacheOp.addCachePool(this, cacheManager, req);
      poolInfoStr = info.toString();
      success = true;
    } finally {
      logAuditEvent(success, "addCachePool", poolInfoStr, null, null);
    }
    
  }

  void modifyCachePool(CachePoolInfo req)
      throws IOException {
    boolean success = false;
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot modify cache pool " + req.getPoolName(), safeMode());
      }
      FSNDNCacheOp.modifyCachePool(this, cacheManager, req);
      success = true;
    } finally {
      String poolNameStr = "{poolName: " +
          (req == null ? null : req.getPoolName()) + "}";
      logAuditEvent(success, "modifyCachePool", poolNameStr,
                    req == null ? null : req.toString(), null);
    }

  }

  void removeCachePool(String cachePoolName)
      throws IOException {
    boolean success = false;
    try {
      if (isInSafeMode()) {
        throw new SafeModeException(
            "Cannot remove cache pool " + cachePoolName, safeMode());
      }
      FSNDNCacheOp.removeCachePool(this, cacheManager, cachePoolName);
      success = true;
    } finally {
      String poolNameStr = "{poolName: " + cachePoolName + "}";
      logAuditEvent(success, "removeCachePool", poolNameStr, null, null);
    }
    
  }

  BatchedListEntries<CachePoolEntry> listCachePools(String prevKey)
      throws IOException {
    BatchedListEntries<CachePoolEntry> results;
    boolean success = false;
    cacheManager.waitForRescanIfNeeded();
    try {
      results = FSNDNCacheOp.listCachePools(this, cacheManager, prevKey);
      success = true;
    } finally {
      logAuditEvent(success, "listCachePools", null, null, null);
    }
    return results;
}

  /**
   * Default AuditLogger implementation; used when no access logger is
   * defined in the config file. It can also be explicitly listed in the
   * config file.
   */
  private static class DefaultAuditLogger extends HdfsAuditLogger {

    private boolean logTokenTrackingId;

    @Override
    public void initialize(Configuration conf) {
      logTokenTrackingId = conf.getBoolean(
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY,
          DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT);
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status, UserGroupInformation ugi,
        DelegationTokenSecretManager dtSecretManager) {
      if (auditLog.isInfoEnabled()) {
        final StringBuilder sb = auditBuffer.get();
        sb.setLength(0);
        sb.append("allowed=").append(succeeded).append("\t");
        sb.append("ugi=").append(userName).append("\t");
        sb.append("ip=").append(addr).append("\t");
        sb.append("cmd=").append(cmd).append("\t");
        sb.append("src=").append(src).append("\t");
        sb.append("dst=").append(dst).append("\t");
        if (null == status) {
          sb.append("perm=null");
        } else {
          sb.append("perm=");
          sb.append(status.getOwner()).append(":");
          sb.append(status.getGroup()).append(":");
          sb.append(status.getPermission());
        }
        if (logTokenTrackingId) {
          sb.append("\t").append("trackingId=");
          String trackingId = null;
          if (ugi != null && dtSecretManager != null
              && ugi.getAuthenticationMethod() == AuthenticationMethod.TOKEN) {
            for (TokenIdentifier tid: ugi.getTokenIdentifiers()) {
              if (tid instanceof DelegationTokenIdentifier) {
                DelegationTokenIdentifier dtid =
                    (DelegationTokenIdentifier)tid;
                trackingId = dtSecretManager.getTokenTrackingId(dtid);
                break;
              }
            }
          }
          sb.append(trackingId);
        }
        sb.append("\t").append("proto=");
        sb.append(NamenodeWebHdfsMethods.isWebHdfsInvocation() ? "webhdfs" : "rpc");
        logAuditMessage(sb.toString());
      }
    }

    public void logAuditMessage(String message) {
      auditLog.info(message);
    }
  }

  private static void enableAsyncAuditLog() {
    if (!(auditLog instanceof Log4JLogger)) {
      LOG.warn("Log4j is required to enable async auditlog");
      return;
    }
    Logger logger = ((Log4JLogger)auditLog).getLogger();
    @SuppressWarnings("unchecked")
    List<Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof AsyncAppender)) {
      AsyncAppender asyncAppender = new AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);
    }
  }

  private void hopSpecificInitialization(Configuration conf) throws IOException {
    HdfsStorageFactory.setConfiguration(conf);
  }

  @Override
  public boolean isLeader() {
    return nameNode.isLeader();
  }

  @Override
  public long getNamenodeId() {
    return nameNode.getLeCurrentId();
  }

  public String getSuperGroup() {
    return this.superGroup;
  }

  public void performPendingSafeModeOperation() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode();
    if (safeMode != null) {
      safeMode.performSafeModePendingOperation();
    }
  }


  void changeConf(List<String> props, List<String> newVals)
      throws IOException {
    for (int i = 0; i < props.size(); i++) {
      String prop = props.get(i);
      String value = newVals.get(i);
      if (prop.equals(DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED) ||
          prop.equals(DFSConfigKeys.DFS_SET_PARTITION_KEY_ENABLED)) {
        LOG.info("change configuration for  " + prop + " to " + value);
        conf.set(prop, value);
        if (prop.equals(DFSConfigKeys.DFS_RESOLVING_CACHE_ENABLED)) {
          Cache.getInstance()
              .enableOrDisable(Boolean.parseBoolean(value));
        }
      } else {
        LOG.info("change configuration for  " + prop + " to " + value +
            " is not supported yet");
      }
    }
  }
  
  public class FNode implements Comparable<FNode> {
    private String parentPath;
    private INode inode;

    public FNode(String parentPath, INode inode) {
      this.parentPath = parentPath;
      this.inode = inode;
    }

    public String getPath() {
      if (parentPath.endsWith("/")) {
        return parentPath + inode.getLocalName();
      } else {
        return parentPath + "/" + inode.getLocalName();
      }
    }

    public INode getINode() {
      return inode;
    }

    public String getParentPath() {
      return parentPath;
    }

    @Override
    public int compareTo(FNode o) {
      int obj1Length = INode.getPathComponents(getPath()).length;
      int obj2Length = INode.getPathComponents(o.getPath()).length;
      return Integer.compare(obj2Length, obj1Length);
    }
  }

  @Override
  public void adjustSafeModeBlocks(Set<Long> safeBlocks) throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode();
    if (safeMode == null) {
      return;
    }
    safeMode.adjustSafeBlocks(safeBlocks);
  }

  QuotaUpdateManager getQuotaUpdateManager() {
    return quotaUpdateManager;
  }

  /**
   * Update safe blocks in the database
   * @param safeBlock
   *      block to be added to safe blocks
   * @throws IOException
   */
  private int addSafeBlock(Long safeBlock) throws IOException {
    final AtomicInteger added = new AtomicInteger(0);
    List<Long> safeBlocks = new ArrayList<>();
    safeBlocks.add(safeBlock);
    addSafeBlocksTX(safeBlocks, added);
    
    return added.get();
  }
  
  /**
   * Remove a block that is not considered safe anymore
   * @param safeBlock
   *      block to be removed from safe blocks
   * @throws IOException
   */
  private void removeSafeBlock(final Long safeBlock) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.remove(safeBlock);
        return null;
      }
    }.handle();
  }

  private void addSafeBlocksTX(final List<Long> safeBlocks, final AtomicInteger added) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ADD_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        boolean inTransaction = connector.isTransactionActive();
        if (!inTransaction) {
          connector.beginTransaction();
          connector.writeLock();
        }
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        int before = da.countAll();
        da.insert(safeBlocks);
        connector.flush();
        int after = da.countAll();
        added.addAndGet(after - before);
        if (!inTransaction) {
          connector.commit();
        }
        return null;
      }
    }.handle();
  }
  
  /**
   * Update safe blocks in the database
   * @param safeBlocks
   *      list of blocks to be added to safe blocks
   * @throws IOException
   */
  private int addSafeBlocks(final List<Long> safeBlocks) throws IOException {
    final AtomicInteger added = new AtomicInteger(0);
    try {
      Slicer.slice(safeBlocks.size(), slicerBatchSize, slicerNbThreads, fsOperationsExecutor,
          new Slicer.OperationHandler() {
        @Override
        public void handle(int startIndex, int endIndex) throws Exception {
          final List<Long> ids = safeBlocks.subList(startIndex, endIndex);
          addSafeBlocksTX(safeBlocks, added);
        }
      });
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
    return added.get();
  }

  /**
   * Get number of blocks to be considered safe in the current cluster
   * @return number of safe blocks
   * @throws IOException
   */
  private int getBlockSafe() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.GET_SAFE_BLOCKS_COUNT) {
      @Override
      public Object performTask() throws IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * Delete all safe blocks
   * @throws IOException
   */
  private void clearSafeBlocks() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.CLEAR_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  public ExecutorService getFSOperationsExecutor() {
    return fsOperationsExecutor;
  }

  /**
   * Setting the quota of a directory in multiple transactions. Calculating the
   * namespace counts of a large directory tree might take to much time for a
   * single transaction. Hence, this functions first reads up the whole tree in
   * multiple transactions while calculating its quota counts before setting
   * the quota in a single transaction using these counts.
   * The subtree is locked during these operations in order to prevent any
   * concurrent modification.
   *
   * Note: This does not support ".inodes" relative path.
   * @param path1
   *    the path of the directory where the quota should be set
   * @param nsQuota
   *    the namespace quota to be set
   * @param dsQuota
   *    the diskspace quota to be set
   * @throws IOException, UnresolvedLinkException
   */
  void multiTransactionalSetQuota(final String path1, final long nsQuota,
      final long dsQuota) throws IOException {
    checkSuperuserPrivilege();
    checkNameNodeSafeMode("Cannot set quota on " + path1);

    if(!nameNode.isLeader() && dir.isQuotaEnabled()){
      throw new NotALeaderException("Quota enabled. Delete operation can only be performed on a " +
              "leader namenode");
    }

    INodeIdentifier subtreeRoot = null;
    boolean removeSTOLock = false;
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(path1);
    final String path = dir.resolvePath(path1, pathComponents, dir);
    try {
      PathInformation pathInfo = getPathExistingINodesFromDB(path,
          false, null, null, null, null);
      INode lastComp = pathInfo.getPathInodes()[pathInfo.getPathComponents().length-1];
      if(lastComp == null){
        throw new FileNotFoundException("Directory does not exist: " + path);
      }else if(!lastComp.isDirectory()){
        throw new FileNotFoundException(path + ": Is not a directory");
      } else if(lastComp.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET){
        throw new IllegalArgumentException(
            "Cannot clear namespace quota on root.");
      }

      //check if the path is root
      if(INode.getPathNames(path).length == 0){ // this method return empty array in case of
        // path = "/"
        subtreeRoot = INodeDirectory.getRootIdentifier();
      }else{
        //this can only be called by super user we just want to lock the tree, not check needed
        subtreeRoot = lockSubtree(path, SubTreeOperation.Type.QUOTA_STO);
        if(subtreeRoot == null){
          // in the mean while the dir has been deleted by someone
          throw new FileNotFoundException("Directory does not exist: " + path);
        }
        removeSTOLock = true;
      }

      final AbstractFileTree.IdCollectingCountingFileTree fileTree =
          new AbstractFileTree.IdCollectingCountingFileTree(this,
              subtreeRoot);
      fileTree.buildUp();
      Iterator<Long> idIterator =
          fileTree.getOrderedIds().descendingIterator();
      synchronized (idIterator) {
        quotaUpdateManager.addPrioritizedUpdates(idIterator);
        try {
          idIterator.wait();
        } catch (InterruptedException e) {
          // Not sure if this can happen if we are not shutting down but we
          // need to abort in case it happens.
          throw new IOException("Operation failed due to an Interrupt");
        }
      }

      HopsTransactionalRequestHandler setQuotaHandler =
          new HopsTransactionalRequestHandler(HDFSOperationType.SET_QUOTA,
              path) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, path)
                      .setNameNodeID(nameNode.getId())
                      .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                      .setIgnoredSTOInodes(fileTree.getSubtreeRootId().getInodeId())
                      .setIgnoredSTOInodes(fileTree.getSubtreeRootId().getInodeId());
              locks.add(il).add(lf.getBlockLock());
            }

            @Override
            public Object performTask() throws IOException {
              dir.setQuota(path, nsQuota, dsQuota, fileTree.getNamespaceCount(),
                  fileTree.getDiskspaceCount());
              return null;
            }
          };
      setQuotaHandler.handle(this);
    } finally {
      if(removeSTOLock){
        unlockSubtree(path, subtreeRoot.getInodeId());
      }
    }
  }

  /**
   * Delete a directory tree in multiple transactions. Deleting a large directory
   * tree might take to much time for a single transaction. Hence, this function
   * first builds up an in-memory representation of the directory tree to be
   * deleted and then deletes it level by level. The directory tree is locked
   * during the delete to prevent any concurrent modification.
   *
   * @param path
   *    the path to be deleted
   * @param recursive
   *    whether or not and non-empty directory should be deleted
   * @return
   *    true if the delete succeeded
   * @throws IOException
   */
  boolean multiTransactionalDelete(final String path, final boolean recursive)
      throws IOException {
    //only for testing
    saveTimes();

    if(!nameNode.isLeader() && dir.isQuotaEnabled()){
      throw new NotALeaderException("Quota enabled. Delete operation can only be performed on a " +
              "leader namenode");
    }

    boolean ret = false;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug("DIR* NameSystem.multiTransactionalDelete: " + path);
    }

    try {
      ret = multiTransactionalDeleteInternal(path, recursive);
      logAuditEvent(ret, "delete", path);
    } catch (IOException e) {
      logAuditEvent(false, "delete", path);
      throw e;
    }
    return ret;
  }

  private boolean multiTransactionalDeleteInternal(final String path1,
      final boolean recursive) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(path1);
    final String path = dir.resolvePath(path1, pathComponents, dir);
    checkNameNodeSafeMode("Cannot delete " + path);

    if (!recursive) {
      // It is safe to do this as it will only delete a single file or an empty directory
      return deleteWithTransaction(path, recursive);
    }

    PathInformation pathInfo = this.getPathExistingINodesFromDB(path,
        false, null, FsAction.WRITE, null, null);
    INode[] pathInodes = pathInfo.getPathInodes();
    INode pathInode = pathInodes[pathInodes.length - 1];

    if (pathInode == null) {
      NameNode.stateChangeLog
          .debug("Failed to remove " + path + " because it does not exist");
      return false;
    } else if (pathInode.isRoot()) {
      NameNode.stateChangeLog.warn("Failed to remove " + path
          + " because the root is not allowed to be deleted");
      return false;
    }

    INodeIdentifier subtreeRoot = null;
    if (pathInode.isFile()) {
      return deleteWithTransaction(path, false);
    } else {
      CacheEntry cacheEntry = retryCacheWaitForCompletionTransactional();
      if (cacheEntry != null && cacheEntry.isSuccess()) {
        return true; // Return previous response
      }
      boolean ret = false;
      try {
        //if quota is enabled then only the leader namenode can delete the directory.
        //this is because before the deletion is done the quota manager has to apply all the outstanding
        //quota updates for the directory. The current design of the quota manager is not distributed.
        //HopsFS clients send the delete operations to the leader namenode if quota is enabled
        if (!isLeader()) {
          throw new QuotaUpdateException("Unable to delete the file " + path
              + " because Quota is enabled and I am not the leader");
        }

        //sub tree operation
        try {
          //once subtree is locked we still need to check all subAccess in AbstractFileTree.FileTree
          //permission check in Apache Hadoop: doCheckOwner:false, ancestorAccess:null, parentAccess:FsAction.WRITE, 
          //access:null, subAccess:FsAction.ALL, ignoreEmptyDir:true
          subtreeRoot = lockSubtreeAndCheckOwnerAndParentPermission(path, false, 
              FsAction.WRITE, SubTreeOperation.Type.DELETE_STO);

          List<AclEntry> nearestDefaultsForSubtree = calculateNearestDefaultAclForSubtree(pathInfo);
          AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(this, subtreeRoot, FsAction.ALL, true,
              nearestDefaultsForSubtree);
          fileTree.buildUp();
          delayAfterBbuildingTree("Built tree for "+path1+" for delete op");

          if (dir.isQuotaEnabled()) {
            Iterator<Long> idIterator = fileTree.getAllINodesIds().iterator();
            synchronized (idIterator) {
              quotaUpdateManager.addPrioritizedUpdates(idIterator);
              try {
                idIterator.wait();
              } catch (InterruptedException e) {
                // Not sure if this can happen if we are not shutting down but we need to abort in case it happens.
                throw new IOException("Operation failed due to an Interrupt");
              }
            }
          }

          for (int i = fileTree.getHeight(); i > 0; i--) {
            if (!deleteTreeLevel(path, fileTree.getSubtreeRoot().getId(), fileTree, i)) {
              ret = false;
              return ret;
            }
          }
        } finally {
          if (subtreeRoot != null) {
            unlockSubtree(path, subtreeRoot.getInodeId());
          }
        }
        ret = true;
        return ret;
      } finally {
        retryCacheSetStateTransactional(cacheEntry, ret);
      }
    }
  }

  private boolean deleteTreeLevel(final String subtreeRootPath, final long subTreeRootID,
      final AbstractFileTree.FileTree fileTree, int level) throws TransactionContextException, IOException {
    ArrayList<Future> barrier = new ArrayList<>();

     for (final ProjectedINode dir : fileTree.getDirsByLevel(level)) {
       if (fileTree.countChildren(dir.getId()) <= BIGGEST_DELETABLE_DIR) {
         final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
         Future f = multiTransactionDeleteInternal(path, subTreeRootID);
         barrier.add(f);
       } else {
         //delete the content of the directory one by one.
         for (final ProjectedINode inode : fileTree.getChildren(dir.getId())) {
           if(!inode.isDirectory()) {
             final String path = fileTree.createAbsolutePath(subtreeRootPath, inode);
             Future f = multiTransactionDeleteInternal(path, subTreeRootID);
             barrier.add(f);
           }
         }
         // the dir is empty now. delete it.
         final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
         Future f = multiTransactionDeleteInternal(path, subTreeRootID);
         barrier.add(f);
       }
     }

    boolean result = true;
    for (Future f : barrier) {
      try {
        if (!((Boolean) f.get())) {
          result = false;
        }
      } catch (ExecutionException e) {
        result = false;
        LOG.error("Exception was thrown during partial delete", e);
        Throwable throwable= e.getCause();
        if ( throwable instanceof  IOException ){
          throw (IOException)throwable; //pass the exception as is to the client
        } else {
          throw new IOException(e); //only io exception as passed to clients.
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return result;
  }

  private Future multiTransactionDeleteInternal(final String path1, final long subTreeRootId)
          throws StorageException, TransactionContextException, IOException {
   byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(path1);
   final String path = dir.resolvePath(path1, pathComponents, dir);
   return  fsOperationsExecutor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          HopsTransactionalRequestHandler deleteHandler =
                  new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_DELETE) {
                    @Override
                    public void acquireLock(TransactionLocks locks)
                            throws IOException {
                      LockFactory lf = LockFactory.getInstance();
                      INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                              INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY,path)
                               .setNameNodeID(nameNode.getId())
                              .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                              .skipReadingQuotaAttr(!dir.isQuotaEnabled())
                              .setIgnoredSTOInodes(subTreeRootId);
                      locks.add(il).add(lf.getLeaseLock(LockType.WRITE))
                              .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                              .add(lf.getBlockLock()).add(
                              lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE, BLK.IV));

                      locks.add(lf.getAllUsedHashBucketsLock());

                      if (dir.isQuotaEnabled()) {
                        locks.add(lf.getQuotaUpdateLock(true, path));
                      }

                      if (erasureCodingEnabled) {
                        locks.add(lf.getEncodingStatusLock(true, LockType.WRITE, path));
                      }
                    }

                    @Override
                    public Object performTask() throws IOException {
                      if(!deleteInternal(path,true,false)){
                        //at this point the delete op is expected to succeed. Apart from DB errors
                        // this can only fail if the quiesce phase in subtree operation failed to
                        // quiesce the subtree. See TestSubtreeConflicts.testConcurrentSTOandInodeOps
                        throw new RetriableException("Unable to Delete path: "+path1+"."+
                                " Possible subtree quiesce failure");

                      }
                      return true;
                    }
                  };
          return (Boolean) deleteHandler.handle(this);
        }
      });
  }

  /**
   * Lock a subtree of the filesystem tree.
   * Locking a subtree prevents it from any concurrent write operations.
   *
   * @param path
   *    the root of the subtree to be locked
   * @return
   *  the inode representing the root of the subtree
   * @throws IOException
   */
  @VisibleForTesting
  INodeIdentifier lockSubtree(final String path, SubTreeOperation.Type stoType) throws
      IOException {
    return lockSubtreeAndCheckOwnerAndParentPermission(path, false, null, stoType);
  }

  /**
   * Lock a subtree of the filesystem tree and ensure that the client has
   * sufficient permissions. Locking a subtree prevents it from any concurrent
   * write operations.
   *
   * @param path
   *    the root of the subtree to be locked
   * @param doCheckOwner
   *    whether or not to check the owner
   * @param ancestorAccess
   *    the requested ancestor access
   * @param parentAccess
   *    the requested parent access
   * @param access
   *    the requested access
   * @param subAccess
   *    the requested sub access
   * @return
   *  the inode representing the root of the subtree
   * @throws IOException
   */
  @VisibleForTesting
  INodeIdentifier lockSubtreeAndCheckOwnerAndParentPermission(final String path,
      final boolean doCheckOwner, 
      final FsAction parentAccess,
      final SubTreeOperation.Type stoType) throws IOException {

    if(path.compareTo("/")==0){
      return null;
    }

    return (INodeIdentifier) new HopsTransactionalRequestHandler(
        HDFSOperationType.SET_SUBTREE_LOCK) {

      @Override
      public void setUp() throws IOException {
        super.setUp();
        if(LOG.isDebugEnabled()) {
          LOG.debug("About to lock \"" + path + "\"");
        }
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock( INodeLockType.WRITE, INodeResolveType.PATH, path);
        il.setNameNodeID(nameNode.getId()).setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!dir.isQuotaEnabled())
                .enableHierarchicalLocking(conf.getBoolean(DFSConfigKeys.DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY,
                DFSConfigKeys.DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY_DEFAULT));
        locks.add(il).
                //READ_COMMITTED because it is index scan and hierarchical locking for inodes is sufficient
                add(lf.getSubTreeOpsLock(LockType.READ_COMMITTED,
                getSubTreeLockPathPrefix(path))); // it is
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        INodesInPath iip = dir.getINodesInPath(path, false);
        if (isPermissionEnabled && !pc.isSuperUser()) {
          dir.checkPermission(pc, iip, doCheckOwner, null, parentAccess, null, null,
              true);
        }

        INode[] nodes = iip.getINodes();
        INode inode = nodes[nodes.length - 1];

        if (inode != null && inode.isDirectory() &&
            !inode.isRoot()) { // never lock the fs root
          checkSubTreeLocks(getSubTreeLockPathPrefix(path));
          inode.setSubtreeLocked(true);
          inode.setSubtreeLockOwner(getNamenodeId());
          EntityManager.update(inode);
          if(LOG.isDebugEnabled()) {
            LOG.debug("Lock the INode with sub tree lock flag. Path: \"" + path + "\" "
                    + " id: " + inode.getId()
                    + " pid: " + inode.getParentId() + " name: " + inode.getLocalName());
          }

          EntityManager.update(new SubTreeOperation(getSubTreeLockPathPrefix(path)
                ,nameNode.getId(),stoType));
          INodeIdentifier iNodeIdentifier =  new INodeIdentifier(inode.getId(), inode.getParentId(),
              inode.getLocalName(), inode.getPartitionId());
          iNodeIdentifier.setDepth(inode.myDepth());

          //Wait before commit. Only for testing
          delayBeforeSTOFlag(stoType.toString());
          return  iNodeIdentifier;
        }else{
          if(LOG.isInfoEnabled()) {
            LOG.info("No component was locked in the path using sub tree flag. "
                    + "Path: \"" + path + "\"");
          }
          return null;
        }
      }
    }.handle(this);
  }

  /**
   * adds / at the end of the path
   * suppose /aa/bb is locked and we want to lock an other folder /a.
   * when we search for all prefixes "/a" it will return subtree ops in other
   * folders i.e /aa*. By adding / in the end of the path solves the problem
   * @param path
   * @return /path + "/"
   */
  String getSubTreeLockPathPrefix(String path){
    String subTreeLockPrefix = path;
    if(!subTreeLockPrefix.endsWith("/")){
      subTreeLockPrefix+="/";
    }
    return subTreeLockPrefix;
  }

  /**
   * check for sub tree locks in the descendant tree
   * @return number of active operations in the descendant tree
   */
  private void checkSubTreeLocks(String path) throws TransactionContextException,
          StorageException, RetriableException{
    List<SubTreeOperation> ops = (List<SubTreeOperation>)
        EntityManager.findList(SubTreeOperation.Finder.ByPathPrefix,
            path);  // THIS RETURNS ONLY ONE SUBTREE OP IN THE CHILD TREE. INCREASE THE LIMIT IN IMPL LAYER IF NEEDED
    Set<Long> activeNameNodeIds = new HashSet<>();
    for(ActiveNode node:nameNode.getActiveNameNodes().getActiveNodes()){
      activeNameNodeIds.add(node.getId());
    }

    for(SubTreeOperation op : ops){
      if(activeNameNodeIds.contains(op.getNameNodeId())){
        throw new RetriableException("At least one ongoing " +
            "subtree operation on the descendants of this subtree, e.g., Path: "+op.getPath()
            +" Operation: "+op.getOpType()+" NameNodeId: "+ op.getNameNodeId());
      }else{ // operation started by a dead namenode.
        //TODO: what if the activeNameNodeIds does not contain all new namenode ids
        //An operation belonging to new namenode might be considered dead
        //handle this my maintaining a list of dead namenodes.
        EntityManager.remove(op);
      }
    }
  }

  /**
   * Unlock a subtree in the filesystem tree.
   *
   * @param path
   *    the root of the subtree
   * @throws IOException
   */
  @VisibleForTesting
  void unlockSubtree(final String path, final long ignoreStoInodeId) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RESET_SUBTREE_LOCK) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = (INodeLock)lf.getINodeLock( INodeLockType.WRITE, INodeResolveType.PATH, path)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!dir.isQuotaEnabled())
                .setIgnoredSTOInodes(ignoreStoInodeId)
                .enableHierarchicalLocking(conf.getBoolean(DFSConfigKeys.DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY,
                        DFSConfigKeys.DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY_DEFAULT));
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        INodesInPath inodesInPath = dir.getINodesInPath(path, false);
        INode[] nodes = inodesInPath.getINodes();
        INode inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSTOLocked()) {
          inode.setSubtreeLocked(false);
          EntityManager.update(inode);
        }
        EntityManager.remove(new SubTreeOperation(getSubTreeLockPathPrefix(path)));
        return null;
      }
    }.handle(this);
  }

  private int pid(String param) {
    StringTokenizer tok = new StringTokenizer(param);
    tok.nextElement();
    return Integer.parseInt((String) tok.nextElement());
  }

  private String pname(String param) {
    StringTokenizer tok = new StringTokenizer(param);
    return (String) tok.nextElement();
  }

  public NameNode getNameNode() {
    return nameNode;
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getEncodingStatus
   */
  public EncodingStatus getEncodingStatus(final String filePathArg)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(filePathArg);
    final String filePath = dir.resolvePath(filePathArg, pathComponents, dir);
    HopsTransactionalRequestHandler findReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.FIND_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ_COMMITTED, INodeResolveType.PATH, filePath)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il).add(lf.getEncodingStatusLock(LockType.READ_COMMITTED, filePath));
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            INodesInPath iip = dir.getINodesInPath(filePath, true);
            try {
              if (isPermissionEnabled) {
                dir.checkPathAccess(pc, iip, FsAction.READ);
              }
            } catch (AccessControlException e){
              logAuditEvent(false, "getEncodingStatus", filePath);
              throw e;
            }
            INode targetNode = iip.getLastINode();
            if (targetNode == null) {
              throw new FileNotFoundException();
            }
            return EntityManager
                .find(EncodingStatus.Finder.ByInodeId, targetNode.getId());
          }
        };
    Object result = findReq.handle();
    if (result == null) {
      return new EncodingStatus(EncodingStatus.Status.NOT_ENCODED);
    }
    return (EncodingStatus) result;
  }

  /**
   * Get the inode with the given id.
   *
   * @param id
   *    the inode id
   * @return
   *    the inode
   * @throws IOException
   */
  public INode findInode(final long id) throws IOException {
    LightWeightRequestHandler findHandler =
        new LightWeightRequestHandler(HDFSOperationType.GET_INODE) {
          @Override
          public Object performTask() throws IOException {
            INodeDataAccess<INode> dataAccess =
                (INodeDataAccess) HdfsStorageFactory
                    .getDataAccess(INodeDataAccess.class);
            return dataAccess.findInodeByIdFTIS(id);
          }
        };
    return (INode) findHandler.handle();
  }

  /**
   * Get the path of a file with the given inode id.
   *
   * @param id
   *    the inode id of the file
   * @return
   *    the path
   * @throws IOException
   */
  public String getPath(long id, boolean inTree) throws IOException {
    LinkedList<INode> resolvedInodes = new LinkedList<>();
    boolean resolved[] = new boolean[1];
    INodeUtil.findPathINodesById(id, inTree, resolvedInodes, resolved);

    if (!resolved[0]) {
      throw new IOException(
          "Path could not be resolved for inode with id " + id);
    }

    return INodeUtil.constructPath(resolvedInodes);
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getMissingBlockLocations
   */
  public LocatedBlocks getMissingBlockLocations(final String clientMachine,
      final String filePath)
      throws
      IOException {

    LocatedBlocks blocks =
        getBlockLocations(clientMachine, filePath, 0, Long.MAX_VALUE);
    Iterator<LocatedBlock> iterator = blocks.getLocatedBlocks().iterator();
    while (iterator.hasNext()) {
      LocatedBlock b = iterator.next();
      if ((b.isCorrupt() ||
          (b.getLocations().length == 0 && b.getBlockSize() > 0)) == false) {
        iterator.remove();
      }
    }
    return blocks;
  }

  /**
   * Add and encoding status for a file without checking the retry cache.
   *
   * @param sourcePath
   *    the file path
   * @param policy
   *    the policy to be used
   * @throws IOException
   */
  public void addEncodingStatus(final String sourcePathArg,
      final EncodingPolicy policy, final EncodingStatus.Status status, final boolean checkRetryCache)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(sourcePathArg);
    final String sourcePath = dir.resolvePath(sourcePathArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, sourcePath)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getEncodingStatusLock(LockType.WRITE, sourcePath));
        if(isRetryCacheEnabled) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
      }

      @Override
      public Object performTask() throws IOException {
        CacheEntry cacheEntry = null;
        if (checkRetryCache) {
          cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
          if (cacheEntry != null && cacheEntry.isSuccess()) {
            return null; // Return previous response
          }
        }
        boolean success = false;
        try {
          FSPermissionChecker pc = getPermissionChecker();
          INodesInPath iip = dir.getINodesInPath(sourcePath, true);
          try {
            if (isPermissionEnabled) {
              dir.checkPathAccess(pc, iip, FsAction.WRITE);
            }
          } catch (AccessControlException e) {
            logAuditEvent(false, "encodeFile", sourcePath);
            throw e;
          }
          INode target = iip.getLastINode();
          EncodingStatus existing = EntityManager.find(
              EncodingStatus.Finder.ByInodeId, target.getId());
          if (existing != null) {
            throw new IOException("Attempting to request encoding for an" + "encoded file");
          }
          INode inode = dir.getINode(sourcePath);
          EncodingStatus encodingStatus = new EncodingStatus(inode.getId(), inode.isInTree(), status,
              policy, System.currentTimeMillis());
          EntityManager.add(encodingStatus);
          success = true;
          return null;

        } finally {
          if (checkRetryCache) {
            RetryCacheDistributed.setState(cacheEntry, success);
          }
        }
      }
    }.handle();
  }

  /**
   * Remove the status of an erasure-coded file.
   *
   * @param encodingStatus
   *    the status of the file
   * @throws IOException
   */
  public void removeEncodingStatus(final EncodingStatus encodingStatus)
      throws IOException {
    // All referring inodes are already deleted. No more lock necessary.
    LightWeightRequestHandler removeHandler =
        new LightWeightRequestHandler(EncodingStatusOperationType.DELETE) {
          @Override
          public Object performTask() throws IOException {
            BlockChecksumDataAccess blockChecksumDataAccess =
                (BlockChecksumDataAccess) HdfsStorageFactory
                    .getDataAccess(BlockChecksumDataAccess.class);
            EncodingStatusDataAccess encodingStatusDataAccess =
                (EncodingStatusDataAccess) HdfsStorageFactory
                    .getDataAccess(EncodingStatusDataAccess.class);
            blockChecksumDataAccess.deleteAll(encodingStatus.getInodeId());
            blockChecksumDataAccess
                .deleteAll(encodingStatus.getParityInodeId());
            encodingStatusDataAccess.delete(encodingStatus);
            return null;
          }
        };
    removeHandler.handle();
  }

  /**
   * Remove the status of an erasure-coded file.
   *
   * @param path
   *    the path of the file
   * @param encodingStatus
   *    the status of the file
   * @throws IOException
   */
  public void removeEncodingStatus(final String path,
      final EncodingStatus encodingStatus) throws IOException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.DELETE_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, path)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il)
            .add(lf.getEncodingStatusLock(LockType.WRITE, path));
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.remove(encodingStatus);
        return null;
      }
    }.handle();
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#revokeEncoding
   */
  public void revokeEncoding(final String filePathArg, short replication)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(filePathArg);
    final String filePath = dir.resolvePath(filePathArg, pathComponents, dir);
    setReplication(filePath, replication);
    new HopsTransactionalRequestHandler(
        HDFSOperationType.REVOKE_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, filePath)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il)
            .add(lf.getEncodingStatusLock(LockType.WRITE, filePath));
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        INodesInPath iip = dir.getINodesInPath(filePath, true);
        try {
          if (isPermissionEnabled) {
            dir.checkPathAccess(pc, iip, FsAction.WRITE);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "revokeEncoding", filePath);
          throw e;
        }
        INode targetNode = iip.getLastINode();
        EncodingStatus encodingStatus = EntityManager
            .find(EncodingStatus.Finder.ByInodeId, targetNode.getId());
        encodingStatus.setRevoked(true);
        EntityManager.update(encodingStatus);
        return null;
      }
    }.handle();
  }

  /**
   * Set the status of an erasure-coded file.
   *
   * @param sourceFile
   *    the file path
   * @param status
   *    the file status
   * @throws IOException
   */
  public void updateEncodingStatus(String sourceFile,
      EncodingStatus.Status status) throws IOException {
    updateEncodingStatus(sourceFile, status, null, null);
  }

  /**
   * Set the parity status of an erasure-coded file.
   *
   * @param sourceFile
   *    the file path
   * @param parityStatus
   *    the parity file status
   * @throws IOException
   */
  public void updateEncodingStatus(String sourceFile,
      EncodingStatus.ParityStatus parityStatus) throws IOException {
    updateEncodingStatus(sourceFile, null, parityStatus, null);
  }

  /**
   * Set the status of an erasure-coded file.
   *
   * @param sourceFile
   *    the file path
   * @param status
   *    the file status
   * @param parityFile
   *    the parity file name
   * @throws IOException
   */
  public void updateEncodingStatus(String sourceFile,
      EncodingStatus.Status status, String parityFile) throws IOException {
    updateEncodingStatus(sourceFile, status, null, parityFile);
  }

  /**
   * Set the status of an erasure-coded file and its parity file.
   *
   * @param sourceFile
   *    the file path
   * @param status
   *    the file status
   * @param parityStatus
   *    the parity status
   * @param parityFile
   *    the parity file name
   * @throws IOException
   */
  public void updateEncodingStatus(final String sourceFileArg,
      final EncodingStatus.Status status,
      final EncodingStatus.ParityStatus parityStatus, final String parityFile)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(sourceFileArg);
    final String sourceFile = dir.resolvePath(sourceFileArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(
        HDFSOperationType.UPDATE_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, sourceFile)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getEncodingStatusLock(LockType.WRITE, sourceFile));
      }

      @Override
      public Object performTask() throws IOException {
        INode targetNode = getINode(sourceFile);
        EncodingStatus encodingStatus = EntityManager
            .find(EncodingStatus.Finder.ByInodeId, targetNode.getId());
        if (status != null) {
          encodingStatus.setStatus(status);
          encodingStatus.setStatusModificationTime(System.currentTimeMillis());
        }
        if (parityFile != null) {
          encodingStatus.setParityFileName(parityFile);
          // Should be updated together with the status so the modification time is already set
        }
        if (parityStatus != null) {
          encodingStatus.setParityStatus(parityStatus);
          encodingStatus.setStatusModificationTime(System.currentTimeMillis());
        }
        EntityManager.update(encodingStatus);
        return null;
      }
    }.handle();
  }

  public INode getINode(String path)
      throws UnresolvedLinkException, StorageException,
      TransactionContextException {
    INodesInPath inodesInPath = dir.getINodesInPath4Write(path);
    INode[] inodes = inodesInPath.getINodes();
    return inodes[inodes.length - 1];
  }

  public boolean isErasureCodingEnabled() {
    return erasureCodingEnabled;
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#addBlockChecksum
   */
  public void addBlockChecksum(final String srcArg, final int blockIndex,
      final long checksum) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_BLOCK_CHECKSUM) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        INodesInPath iip = dir.getINodesInPath(src, true);
        try {
          if (isPermissionEnabled) {
            dir.checkPathAccess(pc, iip, FsAction.WRITE);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "addBlockChecksum", src);
          throw e;
        }
        long inodeId = iip.getLastINode().getId();
        BlockChecksum blockChecksum =
            new BlockChecksum(inodeId, blockIndex, checksum);
        EntityManager.add(blockChecksum);
        return null;
      }
    }.handle();
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getBlockChecksum
   */
  public long getBlockChecksum(final String srcArg, final int blockIndex)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    return (Long) new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_BLOCK_CHECKSUM) {
          
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockChecksumLock(src, blockIndex));
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        INodesInPath iip = dir.getINodesInPath(src, true);
        try {
          if (isPermissionEnabled) {
            dir.checkPathAccess(pc, iip, FsAction.READ);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "getBlockChecksum", src);
          throw e;
        }
        INode node = iip.getLastINode();
        BlockChecksumDataAccess.KeyTuple key =
            new BlockChecksumDataAccess.KeyTuple(node.getId(), blockIndex);
        BlockChecksum checksum =
            EntityManager.find(BlockChecksum.Finder.ByKeyTuple, key);

        if (checksum == null) {
          throw new IOException("No checksum was found for " + key);
        }

        return checksum.getChecksum();
      }
    }.handle();
  }


  void modifyAclEntries(final String srcArg, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot modify acl entries " + srcArg, safeMode());
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.MODIFY_ACL_ENTRIES) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(false);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        try {
          FSPermissionChecker pc = getPermissionChecker();
          if (isPermissionEnabled) {
            final INodesInPath iip = dir.getINodesInPath4Write(src);
            dir.checkOwner(pc, iip);
          }
          dir.modifyAclEntries(src, aclSpec);
        } catch (AccessControlException e) {
          logAuditEvent(false, "modifyAclEntries", srcArg);
          throw e;
        }
        return null;
      }
    }.handle();
  }

  void removeAclEntries(final String srcArg, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove acl entries " + srcArg, safeMode());
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_ACL_ENTRIES) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }
      @Override
      public Object performTask() throws IOException {
        try {
          FSPermissionChecker pc = getPermissionChecker();
          final INodesInPath iip = dir.getINodesInPath4Write(src);
          dir.checkOwner(pc, iip);
          dir.removeAclEntries(src, aclSpec);
        } catch (AccessControlException e) {
          logAuditEvent(false, "removeAclEntries", srcArg);
          throw e;
        }
        return null;
      }
    }.handle();
  }

  void removeDefaultAcl(final String srcArg) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove default acl " + srcArg, safeMode());
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_DEFAULT_ACL) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }
      @Override
      public Object performTask() throws IOException {
        try {
          FSPermissionChecker pc = getPermissionChecker();
          final INodesInPath iip = dir.getINodesInPath4Write(src);
          dir.checkOwner(pc, iip);
          dir.removeDefaultAcl(src);
        } catch (AccessControlException e) {
          logAuditEvent(false, "removeDefaultAcl", srcArg);
          throw e;
        }
        return null;
      }
    }.handle();
  }

  void removeAcl(final String srcArg) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove acl " + srcArg, safeMode());
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.REMOVE_ACL) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }
      @Override
      public Object performTask() throws IOException {
        try {
          FSPermissionChecker pc = getPermissionChecker();
          final INodesInPath iip = dir.getINodesInPath4Write(src);
          dir.checkOwner(pc, iip);
          dir.removeAcl(src);
        } catch (AccessControlException e) {
          logAuditEvent(false, "removeAcl", srcArg);
          throw e;
        }
        return null;
      }
    }.handle();
  }

  void setAcl(final String srcArg, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot set acl " + srcArg, safeMode());
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_ACL) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        try {
          FSPermissionChecker pc = getPermissionChecker();
          final INodesInPath iip = dir.getINodesInPath4Write(src);
          dir.checkOwner(pc, iip);
          dir.setAcl(src, aclSpec);
        } catch (AccessControlException e) {
          logAuditEvent(false, "setAcl", srcArg);
          throw e;
        }
        return null;
      }
    }.handle();

  }

  AclStatus getAclStatus(final String srcArg) throws IOException {
    aclConfigFlag.checkForApiCall();
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    return (AclStatus) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ACL_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(true);
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        boolean success = false;
        try {
          FSPermissionChecker pc = getPermissionChecker();
          INodesInPath iip = dir.getINodesInPath(src, true);
          if (isPermissionEnabled) {
            dir.checkPermission(pc, iip, false, null, null, null, null);
          }
          final AclStatus ret = dir.getAclStatus(src);
          success = true;
          return ret;
        } finally {
          logAuditEvent(success, "getAclStatus", src);
        }
      }
    }.handle();
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getRepairedBlockLocations
   */
  public LocatedBlock getRepairedBlockLocations(String clientMachine,
      final String sourcePath, String parityPath, LocatedBlock block,
      boolean isParity) throws IOException {
    EncodingStatus status = getEncodingStatus(sourcePath);
    Codec codec = Codec.getCodec(status.getEncodingPolicy().getCodec());

    ArrayList<LocatedBlock> sourceLocations =
        new ArrayList(getBlockLocations(clientMachine, sourcePath, 0,
            Long.MAX_VALUE).getLocatedBlocks());
    Collections.sort(sourceLocations, LocatedBlock.blockIdComparator);
    ArrayList<LocatedBlock> parityLocations =
        new ArrayList(getBlockLocations(clientMachine, parityPath, 0,
            Long.MAX_VALUE).getLocatedBlocks());
    Collections.sort(parityLocations, LocatedBlock.blockIdComparator);

    HashSet<Node> excluded = new HashSet<>();
    int stripe =
        isParity ? getStripe(block, parityLocations, codec.getParityLength()) :
            getStripe(block, sourceLocations, codec.getStripeLength());

    // Exclude all nodes from the related source stripe
    int index = stripe * codec.getStripeLength();
    for (int i = index;
         i < sourceLocations.size() && i < index + codec.getStripeLength();
         i++) {
      DatanodeInfo[] nodes = sourceLocations.get(i).getLocations();
      for (DatanodeInfo node : nodes) {
        excluded.add(node);
      }
    }

    // Exclude all nodes from the related parity blocks
    index = stripe * codec.getParityLength();
    for (int i = index; i < parityLocations.size()
        && i < index + codec.getParityLength(); i++) {
      DatanodeInfo[] nodes = parityLocations.get(i).getLocations();
      for (DatanodeInfo node : nodes) {
        excluded.add(node);
      }
    }

    // Find which storage policy is used for this file.
    byte storagePolicyID = (byte) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED,
                TransactionLockTypes.INodeResolveType.PATH, sourcePath)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        INode targetNode = nameNode.getNamesystem().getINode(sourcePath);
        return targetNode.getLocalStoragePolicyID();
      }
    }.handle();

    List<DatanodeStorageInfo> chosenStorages = new LinkedList<DatanodeStorageInfo>();

    final DatanodeStorageInfo[] descriptors = blockManager.chooseTarget4ParityRepair(
                isParity ? parityPath : sourcePath,
            isParity ? 1 : status.getEncodingPolicy().getTargetReplication(),
            null, chosenStorages, excluded, block.getBlockSize(), storagePolicyID);

    return new LocatedBlock(block.getBlock(), descriptors);
  }

  private int getStripe(LocatedBlock block,
      ArrayList<LocatedBlock> locatedBlocks, int length) {
    int i = 0;
    for (LocatedBlock b : locatedBlocks) {
      if (block.getBlock().getBlockId() == b.getBlock().getBlockId()) {
        break;
      }
      i++;
    }
    return i / length;
  }

  public PathInformation getPathExistingINodesFromDB(final String path,
      final boolean doCheckOwner, final FsAction ancestorAccess,
      final FsAction parentAccess, final FsAction access,
      final FsAction subAccess) throws IOException{
    HopsTransactionalRequestHandler handler =
        new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_PATH_INFO) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ_COMMITTED,
                    INodeResolveType.PATH, path)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il).add(lf.getBlockLock()); // blk lock only if file
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            byte[][] pathComponents = INode.getPathComponents(path);
            INodesInPath iip = dir.getExistingPathINodes(pathComponents);
            if (isPermissionEnabled && !pc.isSuperUser()) {
              dir.checkPermission(pc, iip, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, false);
            }

            boolean isDir = false;
            INode.DirCounts srcCounts = new INode.DirCounts();
            final INode[] pathInodes = iip.getINodes();

            INodeAttributes quotaDirAttributes = null;
            INode leafInode = pathInodes[pathInodes.length - 1];
            if(leafInode != null){  // complete path resolved
              if(leafInode instanceof INodeFile ){
                isDir = false;
                //do ns and ds counts for file only
                leafInode.spaceConsumedInTree(srcCounts);
              } else{
                isDir =true;
                if(leafInode instanceof INodeDirectory && dir
                    .isQuotaEnabled()){
                  final DirectoryWithQuotaFeature q = ((INodeDirectory) leafInode).getDirectoryWithQuotaFeature();
                  if (q != null) {
                    quotaDirAttributes = q.getINodeAttributes((INodeDirectory) leafInode);
                  }
                }
              }
            }

            //Get acls
            List[] acls = new List[pathInodes.length];
            for (int i = 0; i < pathInodes.length ; i++){
              if (pathInodes[i] != null){
                AclFeature aclFeature = INodeAclHelper.getAclFeature(pathInodes[i]);
                acls[i] = aclFeature != null ? aclFeature.getEntries() : null;
              }
            }

            return new PathInformation(path, pathComponents,
                iip,isDir,
                srcCounts.getNsCount(), srcCounts.getDsCount(), quotaDirAttributes, acls);
          }
        };
    return (PathInformation)handler.handle(this);
  }

  public boolean storeSmallFilesInDB() {
    return this.storeSmallFilesInDB;
  }

  public static int dbOnDiskFileMaximumSize() {
    return DB_ON_DISK_FILE_MAX_SIZE;
  }


  public static int dbOnDiskSmallFileMaxSize() {
    return DB_ON_DISK_SMALL_FILE_MAX_SIZE;
  }

  public static int dbOnDiskMediumFileMaxSize() {
    return DB_ON_DISK_MEDIUM_FILE_MAX_SIZE;
  }

  public static int dbOnDiskLargeFileMaxSize() {
    return DB_ON_DISK_LARGE_FILE_MAX_SIZE;
  }

  public static int dbInMemorySmallFileMaxSize() {
    return DB_IN_MEMORY_FILE_MAX_SIZE;
  }

  public byte[] getSmallFileData(final long id) throws IOException {
    final long inodeId = -id;
    return (byte[]) ( new HopsTransactionalRequestHandler(HDFSOperationType.GET_SMALL_FILE_DATA) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier = new INodeIdentifier(inodeId);
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualINodeLock(INodeLockType.READ, inodeIdentifier));
      }

      @Override
      public Object performTask() throws IOException {
        INode iNode = EntityManager.find(INode.Finder.ByINodeIdFTIS, inodeId);

        if (iNode == null ){
          throw new  FileNotFoundException("The file id: "+id+" does not exist");
        }

        if (iNode instanceof  INodeFile){
          INodeFile file = ((INodeFile)iNode);
          if (!file.isFileStoredInDB() ){
            throw new  IOException("The requested file is not stored in the database.");
          }

          return ((INodeFile)iNode).getFileDataInDB();
        } else{
          throw new  FileNotFoundException("Inode id: "+id+" is not a file.");
        }
      }
    }).handle();
  }


  void checkAccess(final String srcArg, final FsAction mode) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(srcArg);
    final String src = dir.resolvePath(srcArg, pathComponents, dir);
    HopsTransactionalRequestHandler checkAccessHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_ACCESS,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(true/*skip quota*/);
            locks.add(il);
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            INodesInPath iip = dir.getINodesInPath(src, true);
            try {
              INode[] inodes = iip.getINodes();
              if (inodes[inodes.length - 1] == null) {
                throw new FileNotFoundException("Path not found");
              }
              if (isPermissionEnabled) {
                dir.checkPathAccess(pc, iip, mode);
              }
            } catch (AccessControlException e) {
              logAuditEvent(false, "checkAccess", src);
              throw e;
            }
            return null;
          }
        };

    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }

    checkAccessHandler.handle(this);
  }

  public LastUpdatedContentSummary getLastUpdatedContentSummary(final String path) throws
      IOException{

    LastUpdatedContentSummary luSummary = (LastUpdatedContentSummary) new
        HopsTransactionalRequestHandler (HDFSOperationType
            .GET_LAST_UPDATED_CONTENT_SUMMARY){

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ_COMMITTED, INodeResolveType.PATH, path)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                .skipReadingQuotaAttr(!dir.isQuotaEnabled());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        INode subtreeRoot = getINode(path);
        if(subtreeRoot instanceof INodeDirectory){
          INodeDirectory quotaDir = (INodeDirectory) subtreeRoot;
          final DirectoryWithQuotaFeature q = quotaDir.getDirectoryWithQuotaFeature();
          if (q != null) {
            return new LastUpdatedContentSummary(q.numItemsInTree(quotaDir),
                q.diskspaceConsumed(quotaDir), quotaDir.getQuotaCounts().get(Quota.NAMESPACE), quotaDir
                .getQuotaCounts().get(Quota.DISKSPACE));
          }
        }
        return null;
      }

    }.handle(this);

    if(luSummary == null) {
      ContentSummary summary = getContentSummary(path);
      luSummary = new LastUpdatedContentSummary(summary.getFileCount() +
          summary.getDirectoryCount(), summary.getSpaceConsumed(),
          summary.getQuota(), summary.getSpaceQuota());
    }

    return luSummary;

  }

  CacheEntry retryCacheWaitForCompletionTransactional() throws IOException {
    if(!isRetryCacheEnabled){
      return null;
    }
    HopsTransactionalRequestHandler rh = new HopsTransactionalRequestHandler(HDFSOperationType
            .RETRY_CACHE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
      }

      @Override
      public Object performTask() throws IOException {
        return RetryCacheDistributed.waitForCompletion(retryCache);
      }
    };
    return (CacheEntry) rh.handle(this);
  }

  CacheEntry retryCacheSetStateTransactional(final CacheEntry cacheEntry, final boolean ret) throws IOException {
    if(!isRetryCacheEnabled){
      return null;
    }
    HopsTransactionalRequestHandler rh = new HopsTransactionalRequestHandler(HDFSOperationType
            .RETRY_CACHE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
      }

      @Override
      public Object performTask() throws IOException {
        RetryCacheDistributed.setState(cacheEntry, ret);
        return null;
      }
    };
    return (CacheEntry) rh.handle(this);
  }

  class RetryCacheCleaner implements Runnable {

    boolean shouldCacheCleanerRun = true;
    long entryExpiryMillis;
    Timer timer = new Timer();

    public RetryCacheCleaner() {
      entryExpiryMillis = conf.getLong(
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);
    }

    @Override
    public void run() {
      int numRun = 0;
      while (fsRunning && shouldCacheCleanerRun) {
        try {
          final List<CacheEntry> toRemove = new ArrayList<>();
          int num = retryCache.getToRemove().drainTo(toRemove);
          if (num > 0) {
            HopsTransactionalRequestHandler rh
                = new HopsTransactionalRequestHandler(HDFSOperationType.CLEAN_RETRY_CACHE) {
              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getRetryCacheEntryLock(toRemove));
              }

              @Override
              public Object performTask() throws IOException {
                for (CacheEntry entry : toRemove) {
                  if (EntityManager.find(RetryCacheEntry.Finder.ByClientIdAndCallId, entry.getClientId(), entry.
                      getCallId()) != null) {
                    EntityManager.remove(new RetryCacheEntry(entry.getClientId(), entry.getCallId()));
                  }
                }
                return null;
              }
            };
            rh.handle();
          }

          if (isLeader() && numRun % 60 == 0) {
            new LightWeightRequestHandler(
                HDFSOperationType.CLEAN_RETRY_CACHE) {
              @Override
              public Object performTask() throws IOException {

                RetryCacheEntryDataAccess da = (RetryCacheEntryDataAccess) HdfsStorageFactory
                    .getDataAccess(RetryCacheEntryDataAccess.class);
                da.removeOlds(timer.now() - entryExpiryMillis);
                return null;
              }
            }.handle();
          }

          Thread.sleep(1000);
          numRun++;
        } catch (Exception e) {
          FSNamesystem.LOG.error("Exception in RetryCacheCleaner: ", e);
        }
      }
    }

    public void stopMonitor() {
      shouldCacheCleanerRun = false;
    }
  }

  public List<AclEntry> calculateNearestDefaultAclForSubtree(final PathInformation pathInfo) throws IOException {
    for (int i = pathInfo.getPathInodeAcls().length-1; i > -1 ; i--){
      List<AclEntry> aclEntries = pathInfo.getPathInodeAcls()[i];
      if (aclEntries == null){
        continue;
      }

      List<AclEntry> onlyDefaults = new ArrayList<>();
      for (AclEntry aclEntry : aclEntries) {
        if (aclEntry.getScope().equals(AclEntryScope.DEFAULT)){
          onlyDefaults.add(aclEntry);
        }
      }

      if (!onlyDefaults.isEmpty()){
        return onlyDefaults;
      }
    }
   return new ArrayList<>();
  }

  public void setDelayBeforeSTOFlag(long delay){
    if(isTestingSTO)
      this.delayBeforeSTOFlag = delay;
  }

  public void setDelayAfterBuildingTree(long delay){
    if (isTestingSTO)
      this.delayAfterBuildingTree = delay;
  }


  public void delayBeforeSTOFlag(String message) {
    if (isTestingSTO) {
      //Only for testing
      Times time = delays.get();
      if (time == null){
        time = saveTimes();
      }
      try {
        LOG.debug("Testing STO. "+message+" Sleeping for " + time.delayBeforeSTOFlag);
        Thread.sleep(time.delayBeforeSTOFlag);
        LOG.debug("Testing STO. "+message+" Waking up from sleep of " + time.delayBeforeSTOFlag);
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
  }

  public void delayAfterBbuildingTree(String message) {
    //Only for testing
    if (isTestingSTO) {
      Times time = delays.get();
      if (time == null){
        time = saveTimes();
      }
      try {
        LOG.debug("Testing STO. "+message+" Sleeping for " + time.delayAfterBuildingTree);
        Thread.sleep(time.delayAfterBuildingTree);
        LOG.debug("Testing STO. "+message+" Waking up from sleep of " + time.delayAfterBuildingTree);
      } catch (InterruptedException e) {
        LOG.warn(e);
      }
    }
  }

  private class Times {
    long delayBeforeSTOFlag = 0; //This parameter can not be more than TxInactiveTimeout: 1.2 sec
    long delayAfterBuildingTree = 0;

    public Times(long delayBeforeSTOFlag, long delayAfterBuildingTree) {
      this.delayBeforeSTOFlag = delayBeforeSTOFlag;
      this.delayAfterBuildingTree = delayAfterBuildingTree;
    }
  }

  public void setTestingSTO(boolean val) {
    this.isTestingSTO = val ;
  }

  public boolean isTestingSTO() {
    return isTestingSTO;
  }

  private Times saveTimes(){
    if(isTestingSTO) {
      delays.remove();
      Times times = new Times(delayBeforeSTOFlag, delayAfterBuildingTree);
      delays.set(times);
      return times;
    }else{
      return null;
    }
  }
  
  
  public void addUserGroup(String userName, String groupName, boolean cacheOnly)
      throws IOException {
    checkSuperuserPrivilege();
    UsersGroups.addUserGroupTx(userName, groupName, cacheOnly);
  }
  
  
  public void removeUserGroup(String userName, String groupName,
      boolean cacheOnly) throws IOException {
    checkSuperuserPrivilege();
    UsersGroups.removeUserGroupTx(userName, groupName, cacheOnly);
  }
  
  public boolean isRetryCacheEnabled(){
    return isRetryCacheEnabled;
  }
}

