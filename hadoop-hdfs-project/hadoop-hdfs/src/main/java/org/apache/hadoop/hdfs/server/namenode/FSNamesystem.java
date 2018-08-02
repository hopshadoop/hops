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
import io.hops.common.IDsGeneratorFactory;
import io.hops.common.IDsMonitor;
import io.hops.common.INodeUtil;
import io.hops.erasure_coding.Codec;
import io.hops.erasure_coding.ErasureCodingManager;
import io.hops.exception.*;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.LeasePath;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.resolvingcache.Cache;
import io.hops.security.Users;
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
import org.apache.hadoop.fs.Options.Rename;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.HashBuckets;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.MutableBlockCollection;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;
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
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.DirectoryListingStartAfterNotFoundException;
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
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
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
@Metrics(context = "dfs")
public class FSNamesystem
    implements Namesystem, FSClusterStats, FSNamesystemMBean, NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);

  private static final ThreadLocal<StringBuilder> auditBuffer =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  @VisibleForTesting
  private boolean isAuditEnabled() {
    return !isDefaultAuditLogger || auditLog.isInfoEnabled();
  }

  private HdfsFileStatus getAuditFileInfo(String path, boolean resolveSymlink)
      throws IOException {
    return (isAuditEnabled() && isExternalInvocation()) ?
        dir.getFileInfo(path, resolveSymlink, false) : null;
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
  private final String fsOwnerShortUserName;
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

  private volatile boolean hasResourcesAvailable = true; //HOP. yes we have huge namespace
  private volatile boolean fsRunning = true;

  /**
   * The start time of the namesystem.
   */
  private final long startTime = now();

  /**
   * The interval of namenode checking for the disk space availability
   */
  private final long resourceRecheckInterval;

  private final FsServerDefaults serverDefaults;
  private final boolean supportAppends;
  private final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

  private volatile SafeModeInfo safeMode;  // safe mode information

  private final long maxFsObjects;          // maximum number of fs objects

  private final long minBlockSize;         // minimum block size
  private final long maxBlocksPerFile;     // maximum # of blocks per file

  // precision of access times.
  private final long accessTimePrecision;

  private NameNode nameNode;
  private final Configuration conf;
  private final QuotaUpdateManager quotaUpdateManager;

  private final ExecutorService subtreeOperationsExecutor;
  private final boolean erasureCodingEnabled;
  private final ErasureCodingManager erasureCodingManager;

  private final boolean storeSmallFilesInDB;
  private static int DB_ON_DISK_FILE_MAX_SIZE;
  private static int DB_ON_DISK_SMALL_FILE_MAX_SIZE;
  private static int DB_ON_DISK_MEDIUM_FILE_MAX_SIZE;
  private static int DB_ON_DISK_LARGE_FILE_MAX_SIZE;
  private static int DB_IN_MEMORY_FILE_MAX_SIZE;
  private final long BIGGEST_DELETABLE_DIR;

  /**
   * Whether the namenode is in the middle of starting the active service
   */
  private volatile boolean startingActiveService = false;

  private final AclConfigFlag aclConfigFlag;

  private final RetryCacheDistributed retryCache;

  //Add delay for file system operations. Used only for testing
  private boolean isTestingSTO = false;
  private ThreadLocal<Times> delays = new ThreadLocal<Times>();
  long delayBeforeSTOFlag = 0; //This parameter can not be more than TxInactiveTimeout: 1.2 sec
  long delayAfterBuildingTree=0;

  /**
   * Clear all loaded data
   */
  void clear() throws IOException {
    dir.reset();
    dtSecretManager.reset();
    leaseManager.removeAllLeases();
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
  public static FSNamesystem loadFromDisk(Configuration conf, NameNode namenode)
      throws IOException {

    FSNamesystem namesystem = new FSNamesystem(conf, namenode, false);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.RECOVER) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    long loadStart = now();

    namesystem.dir
        .imageLoadComplete();     //HOP: this function was called inside the  namesystem.loadFSImage(...) which is commented out

    long timeTakenToLoadFSImage = now() - loadStart;
    LOG.info(
        "Finished loading FSImage in " + timeTakenToLoadFSImage + " ms");
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

      this.blockManager = new BlockManager(this, this, conf);
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
      this.fsOwnerShortUserName = fsOwner.getShortUserName();
      this.superGroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
          DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
          DFS_PERMISSIONS_ENABLED_DEFAULT);

      blockPoolId = StorageInfo.getStorageInfoFromDB().getBlockPoolId();
      blockManager.setBlockPoolId(blockPoolId);
      hopSpecificInitialization(conf);
      this.quotaUpdateManager = new QuotaUpdateManager(this, conf);
      subtreeOperationsExecutor = Executors.newFixedThreadPool(
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
      this.safeMode = new SafeModeInfo(conf);
      this.auditLoggers = initAuditLoggers(conf);
      this.isDefaultAuditLogger = auditLoggers.size() == 1 &&
          auditLoggers.get(0) instanceof DefaultAuditLogger;
      this.aclConfigFlag = new AclConfigFlag(conf);
      this.retryCache = ignoreRetryCache ? null : initRetryCache(conf);
    } catch (IOException | RuntimeException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    }
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
      long entryExpiryNanos = entryExpiryMillis * 1000 * 1000;
      return new RetryCacheDistributed("Namenode Retry Cache", heapPercent,
          entryExpiryNanos);
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
    if (isClusterInSafeMode()) {
      assert safeMode != null && !safeMode.isPopulatingReplicationQueues();
      StartupProgress prog = NameNode.getStartupProgress();
      prog.beginPhase(Phase.SAFEMODE);
      prog.setTotal(Phase.SAFEMODE, STEP_AWAITING_REPORTED_BLOCKS,
        getCompleteBlocksTotal());
      setBlockTotal();
      performPendingSafeModeOperation();
    }
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

      if (isClusterInSafeMode()) {
        if (!isInSafeMode() || (isInSafeMode() && safeMode.isPopulatingReplicationQueues())) {
          LOG.info("Reprocessing replication and invalidation queues");
          blockManager.processMisReplicatedBlocks();
        }
      }

      leaseManager.startMonitor();
      startSecretManagerIfNecessary();

      //ResourceMonitor required only at ActiveNN. See HDFS-2914
      this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
      nnrmthread.start();

      this.retryCacheCleanerThread = new Daemon(new RetryCacheCleaner());
      retryCacheCleanerThread.start();

      if (erasureCodingEnabled) {
        erasureCodingManager.activate();
      }
    } finally {
      startingActiveService = false;
    }
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
    if (leaseManager != null) {
      leaseManager.stopMonitor();
    }
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
      SafeModeException se = new SafeModeException(errorMsg, safeMode);
      if (shouldRetrySafeMode(this.safeMode)) {
        throw new RetriableException(se);
      } else {
        throw se;
      }
    }
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
      subtreeOperationsExecutor.shutdownNow();
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

  long getDefaultBlockSize() {
    return serverDefaults.getBlockSize();
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    boolean txFailed = true;
    final INodeIdentifier inode = lockSubtreeAndCheckPathPermission(src, true,
            null, null, null, null, SubTreeOperation.Type.SET_PERMISSION_STO);
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
    checkOwner(pc, src);
    dir.setPermission(src, permission);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setPermission", src, null, resultingStat);

    //remove sto from
    if(isSTO){
      INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(src, false);
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
      throws
      IOException {
    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set permission for " + src);
    checkOwner(pc, src);
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    boolean txFailed = true;
    final INodeIdentifier inode =  lockSubtreeAndCheckPathPermission(src, true,
            null, null, null, null, SubTreeOperation.Type.SET_OWNER_STO);
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
        locks.add(il).add(lf.getBlockLock());
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
    checkOwner(pc, src);
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
      INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(src, false);
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
    src = FSDirectory.resolvePath(src, pathComponents, dir);
    checkOwner(pc, src);
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
                        .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC));
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

              LocatedBlock lastBlock = blocks.getLastLocatedBlock();
              if (lastBlock != null) {
                ArrayList<LocatedBlock> lastBlockList =
                    new ArrayList<>();
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC));
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
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.READ);
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
              "Zero blocklocations for " + src, safeMode);
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
        dir.setTimes(src, inode, -1, now, false);
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
        dir.setTimes(src, inode, -1, now, false);
      }
      final long fileSize = inode.computeFileSizeNotIncludingLastUcBlock();
      boolean isUc = inode.isUnderConstruction();

      return blockManager
          .createLocatedBlocks(inode.getBlocks(), fileSize,
              isUc, offset, length, needBlockToken);
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

    final String[] paths = new String[srcs.length + 1];
    System.arraycopy(srcs, 0, paths, 0, srcs.length);
    paths[srcs.length] = target;

    new HopsTransactionalRequestHandler(HDFSOperationType.CONCAT) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, paths)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock()).add(
            lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.PE, BLK.UC, BLK.IV))
            .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
        if (erasureCodingEnabled) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, srcs));
        }
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        final CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        // Either there is no previous request in progres or it has failed
        boolean success = false;
        try {
          concatInt(target, srcs);
          success = true;
        } catch (AccessControlException e) {
          logAuditEvent(false, "concat", Arrays.toString(srcs), target, null);
          throw e;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
        return null;
      }
    }.handle(this);
  }

  private void concatInt(String target, String[] srcs)
      throws IOException {
    if (FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
          " to " + target);
    }

    // verify args
    if (target.isEmpty()) {
      throw new IllegalArgumentException("Target file name is empty");
    }
    if (srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("No sources given");
    }

    // We require all files be in the same directory
    String trgParent =
        target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for (String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if (!srcParent.equals(trgParent)) {
        throw new IllegalArgumentException(
            "Sources and target are not in the same directory");
      }
    }

    HdfsFileStatus resultingStat;
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot concat " + target);
    concatInternal(pc, target, srcs);
    resultingStat = getAuditFileInfo(target, false);
    logAuditEvent(true, "concat", Arrays.toString(srcs), target, resultingStat);
  }

  /**
   * See {@link #concat(String, String[])}
   */
  private void concatInternal(FSPermissionChecker pc, String target,
      String[] srcs)
      throws IOException {

    // write permission for the target
    if (isPermissionEnabled) {
      checkPathAccess(pc, target, FsAction.WRITE);

      // and srcs
      for (String aSrc : srcs) {
        checkPathAccess(pc, aSrc, FsAction.READ); // read the file
        checkParentAccess(pc, aSrc, FsAction.WRITE); // for delete
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks

    // check the target
    final INodeFile trgInode = INodeFile.valueOf(dir.getINode(target), target);
    if(trgInode.isFileStoredInDB()){
      throw new IOException("The target file is stored in the database. Can not concat to a a file stored in the database");
    }

    if (trgInode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException(
          "concat: target file " + target + " is under construction");
    }
    // per design target shouldn't be empty and all the blocks same size
    if (trgInode.numBlocks() == 0) {
      throw new HadoopIllegalArgumentException(
          "concat: target file " + target + " is empty");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    final BlockInfo last = trgInode.getLastBlock();
    if (blockSize != last.getNumBytes()) {
      throw new HadoopIllegalArgumentException(
          "The last block in " + target + " is not full; last block size = " +
              last.getNumBytes() + " but file block size = " + blockSize);
    }

    si.add(trgInode);
    short replication = trgInode.getBlockReplication();

    // now check the srcs
    boolean endSrc =
        false; // final src file doesn't have to have full end block
    for (int i = 0; i < srcs.length; i++) {
      String src = srcs[i];
      if (i == srcs.length - 1) {
        endSrc = true;
      }

      final INodeFile srcInode = INodeFile.valueOf(dir.getINode(src), src);

      if(srcInode.isFileStoredInDB()){
        throw new IOException(src+" is stored in the database. Can not concat to a a file stored in the database");
      }

      if (src.isEmpty() || srcInode.isUnderConstruction() ||
          srcInode.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src +
            " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if (replication != srcInode.getBlockReplication()) {
        throw new HadoopIllegalArgumentException(
            "concat: the source file " + src + " and the target file " +
                target +
                " should have the same replication: source replication is " +
                srcInode.getBlockReplication() + " but target replication is " +
                replication);
      }

      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      final BlockInfo[] srcBlocks = srcInode.getBlocks();
      int idx = srcBlocks.length - 1;
      if (endSrc) {
        idx = srcBlocks.length - 2; // end block of endSrc is OK not to be full
      }
      if (idx >= 0 && srcBlocks[idx].getNumBytes() != blockSize) {
        throw new HadoopIllegalArgumentException(
            "concat: the source file " + src + " and the target file " +
                target +
                " should have the same blocks sizes: target block size is " +
                blockSize + " but the size of source block " + idx + " is " +
                srcBlocks[idx].getNumBytes());
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if (si.size() < srcs.length + 1) { // trg + srcs
      // it means at least two files are the same
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " +
          Arrays.toString(srcs) + " to " + target);
    }

    dir.concat(target, srcs);
  }

  /**
   * stores the modification and access time for this inode.
   * The access time is precise up to an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(final String src1, final long mtime, final long atime)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
    // Write access is required to set access and modification times
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.WRITE);
    }
    INode inode = dir.getINode(src);
    if (inode != null) {
      dir.setTimes(src, inode, mtime, atime, true);
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
    final String link = FSDirectory.resolvePath(link1, pathComponents, dir);
    new HopsTransactionalRequestHandler(HDFSOperationType.CREATE_SYM_LINK,
        link) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH,  link)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getAcesLock())
                .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
      }

      @Override
      public Object performTask() throws IOException {
        final CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
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
    if (!createParent) {
      verifyParentDir(link);
    }
    createSymlinkInternal(pc, target, link, dirPerms, createParent);
    resultingStat = getAuditFileInfo(link, false);
    logAuditEvent(true, "createSymlink", link, target, resultingStat);
  }

  /**
   * Create a symbolic link.
   */
  private void createSymlinkInternal(FSPermissionChecker pc, String target,
      String link, PermissionStatus dirPerms, boolean createParent)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target=" +
          target + " link=" + link);
    }
    checkNameNodeSafeMode("Cannot create symlink " + link);
    if (FSDirectory.isReservedName(target)) {
      throw new InvalidPathException("Invalid target name: " + target);
    }
    if (!DFSUtil.isValidName(link)) {
      throw new InvalidPathException("Invalid file name: " + link);
    }
    if (!dir.isValidToCreate(link)) {
      throw new IOException("failed to create link " + link +
          " either because the filename is invalid or the file exists");
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(pc, link, FsAction.WRITE);
    }
    // validate that we have enough inodes.
    checkFsObjectLimit();

    // add symbolic link to namespace
    dir.addSymlink(link, target, dirPerms, createParent);
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.WRITE);
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

  void setMetaEnabled(final String src, final boolean metaEnabled)
      throws IOException {
    INodeIdentifier stoRootINode = null;
    try {
      stoRootINode = lockSubtree(src, SubTreeOperation.Type.META_ENABLE_STO);
      final AbstractFileTree.FileTree fileTree = buildTreeForLogging(stoRootINode,
          metaEnabled);
      new HopsTransactionalRequestHandler(HDFSOperationType.SET_META_ENABLED,
          src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          int stoRootINodeId = (Integer) getParams()[0];
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
    FSPermissionChecker pc = getPermissionChecker();
    checkNameNodeSafeMode("Cannot set metaEnabled for " + src);
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.WRITE);
    }

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

  private void setStoragePolicyInt(final String filename, final String policyName)
      throws IOException, UnresolvedLinkException, AccessControlException {

    if (!isStoragePolicyEnabled) {
      throw new IOException("Failed to set storage policy since "
          + DFSConfigKeys.DFS_STORAGE_POLICY_ENABLED_KEY + " is set to false.");
    }

    final BlockStoragePolicy policy =  blockManager.getStoragePolicy(policyName);
    if (policy == null) {
      throw new HadoopIllegalArgumentException("Cannot find a block policy with the name " + policyName);
    }

    HopsTransactionalRequestHandler setStoragePolicyHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.SET_STORAGE_POLICY, filename) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, filename)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il);
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            checkNameNodeSafeMode("Cannot set metaEnabled for " + filename);
            if (isPermissionEnabled) {
              checkPathAccess(pc, filename, FsAction.WRITE);
            }

            dir.setStoragePolicy(filename, policy);

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

  long getPreferredBlockSize(final String filename1) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(filename1);
    final String filename = FSDirectory.resolvePath(filename1, pathComponents, dir);
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
            if (isPermissionEnabled) {
              checkTraverse(pc, filename);
            }
            return dir.getPreferredBlockSize(filename);
          }
        };
    return (Long) getPreferredBlockSizeHandler.handle(this);
  }

  /*
   * Verify that parent directory of src exists.
   */
  private void verifyParentDir(String src)
      throws FileNotFoundException, ParentNotDirectoryException,
      UnresolvedLinkException, StorageException, TransactionContextException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode parentNode = getINode(parent.toString());
      if (parentNode == null) {
        throw new FileNotFoundException(
            "Parent directory doesn't exist: " + parent.toString());
      } else if (!parentNode.isDirectory() && !parentNode.isSymlink()) {
        throw new ParentNotDirectoryException(
            "Parent path is not a directory: " + parent.toString());
      }
    }
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
                  lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR, BLK.PE, BLK.IV))
                  .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));

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

    startFileInternal(pc, src, permissions, holder, clientMachine, create, overwrite,
        createParent, replication, blockSize);
    final HdfsFileStatus stat = dir.getFileInfo(src, false, true);
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
  private void startFileInternal(FSPermissionChecker pc, String src,
      PermissionStatus permissions, String holder, String clientMachine,
      boolean create, boolean overwrite, boolean createParent, short replication,
      long blockSize) throws
      IOException {

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException(
          "Cannot create file " + src + "; already exists as a directory.");
    }
    if (isPermissionEnabled) {
      if (overwrite && pathExists) {
        checkPathAccess(pc, src, FsAction.WRITE);
      } else {
        checkAncestorAccess(pc, src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      final INode inode = dir.getINode(src);
      
      if (inode == null) {
        if (!create) {
          throw new FileNotFoundException("failed to overwrite non-existent file "
             + src + " on client " + clientMachine);
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
          final INodeFile myFile = INodeFile.valueOf(dir.getINode(src), src);
          recoverLeaseInternal(myFile, src, holder, clientMachine, false);
          throw new FileAlreadyExistsException("failed to create file " + src
              + " on client " + clientMachine + " because the file exists");
        }
      }

      checkFsObjectLimit();
      final DatanodeDescriptor clientNode = blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
      
      INodeFile newNode = dir.addFile(src, permissions, replication, blockSize,
          holder, clientMachine, clientNode);
      if (newNode == null) {
        throw new IOException("DIR* NameSystem.startFile: " + "Unable to add file to namespace.");
      }
      leaseManager.addLease(newNode.getFileUnderConstructionFeature()
          .getClientName(), src);

      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
            + "add " + src + " to namespace for " + holder);
      }
    } catch (IOException ie) {
      NameNode.stateChangeLog
          .warn("DIR* NameSystem.startFile: " + ie.getMessage());
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
  private LocatedBlock appendFileInternal(FSPermissionChecker pc, String src,
      String holder, String clientMachine) throws AccessControlException,
      UnresolvedLinkException, FileNotFoundException, IOException {
    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException("Cannot append to directory " + src
          + "; already exists as a directory.");
    }
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.WRITE);
    }

    try {
      final INode inode = dir.getINode(src);
      if (inode == null) {
        throw new FileNotFoundException("failed to append to non-existent file "
          + src + " on client " + clientMachine);
      }
      final INodeFile myFile = INodeFile.valueOf(inode, src);

      final BlockInfo lastBlock = myFile.getLastBlock();
      // Check that the block has at least minimum replication.
      if (lastBlock != null && lastBlock.isComplete() && !getBlockManager().isSufficientlyReplicated(lastBlock)) {
        throw new IOException("append: lastBlock=" + lastBlock + " of src=" + src
            + " is not sufficiently replicated yet.");
      }

      // Opening an existing file for write - may need to recover lease.
      recoverLeaseInternal(myFile, src, holder, clientMachine, false);

      final DatanodeDescriptor clientNode =
          blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);
      return prepareFileForWrite(src, myFile, holder, clientMachine, clientNode);
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
   * @param clientNode
   *     if the client is collocated with a DN, that DN's descriptor
   * @return the last block locations if the block is partial or null otherwise
   * @throws UnresolvedLinkException
   * @throws IOException
   */
  private LocatedBlock prepareFileForWrite(String src, INodeFile file,
      String leaseHolder, String clientMachine, DatanodeDescriptor clientNode)
      throws IOException {
    INodeFile cons =
        file.toUnderConstruction(leaseHolder, clientMachine, clientNode);
    Lease lease = leaseManager.addLease(cons.getFileUnderConstructionFeature()
        .getClientName(), src);
    if(cons.isFileStoredInDB()){
      LOG.debug("Stuffed Inode:  prepareFileForWrite stored in database. " +
          "Returning phantom block");
      return blockManager.createPhantomLocatedBlocks(cons,cons.getFileDataInDB(),true,false).getLocatedBlocks().get(0);
    } else {
      LocatedBlock ret = blockManager.convertLastBlockToUnderConstruction(cons);
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
   *     the path of the file to start lease recovery
   * @param holder
   *     the lease holder's name
   * @param clientMachine
   *     the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(final String src1, final String holder,
      final String clientMachine) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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

            final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
            if (!inode.isUnderConstruction()) {
              return true;
            }
            if (isPermissionEnabled) {
              checkPathAccess(pc, src, FsAction.WRITE);
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
        if ((leaseFile != null && leaseFile.equals(lease)) ||
            lease.getHolder().equals(holder)) {
          throw new AlreadyBeingCreatedException(
              "failed to create file " + src + " for " + holder +
                  " on client " + clientMachine +
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
                " on client " + clientMachine +
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

  LocatedBlock appendFile(final String src, final String holder,
                          final String clientMachine) throws IOException {
    LocatedBlock lb = null;
    try{
      lb = appendFileHopFS(src, holder, clientMachine);
      return lb;
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
  private LocatedBlock moveToDNsAndAppend(final String src, final String holder,
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

  private LocatedBlock appendFileHopFS(final String src1, final String holder,
      final String clientMachine) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
            locks.add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
            // Always needs to be read. Erasure coding might have been
            // enabled earlier and we don't want to end up in an inconsistent
            // state.
            locks.add(lf.getEncodingStatusLock(LockType.READ_COMMITTED, src));
            locks.add(lf.getAcesLock());
          }

      @Override
      public Object performTask() throws IOException {
        LocatedBlock locatedBlock = null;
        CacheEntryWithPayload cacheEntry = RetryCacheDistributed.waitForCompletion(retryCache,
            null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          locatedBlock = PBHelper.convert(HdfsProtos.LocatedBlockProto.parseFrom(cacheEntry.getPayload()));
          if (locatedBlock.isPhantomBlock()) {
            //we did not store the data in the cache as it was too big, we should fetch it again.
            INode inode = dir.getINode(src);
            final INodeFile file = INodeFile.valueOf(inode, src);
            if (file.isFileStoredInDB()) {
              LOG.debug("Stuffed Inode: appendFileHopFS  stored in database. " + "Returning phantom block");
              locatedBlock.setData(file.getFileDataInDB());
            }
          }
          return locatedBlock;
        }

        boolean success = false;
        try {
          INode target = getINode(src);
          if (target != null && target instanceof INodeFile && !((INodeFile) target).isFileStoredInDB()) {
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

          locatedBlock = appendFileInt(src, holder, clientMachine);

          if (locatedBlock != null && !locatedBlock.isPhantomBlock()) {
            for (String storageID : locatedBlock.getStorageIDs()) {

              int sId = blockManager.getDatanodeManager().getSid(storageID);
              BlockInfo blockInfo = EntityManager.find(BlockInfo.Finder.ByBlockIdAndINodeId,
                  locatedBlock.getBlock().getBlockId(), target.getId());
              Block undoBlock = new Block(blockInfo);
              undoBlock.setGenerationStampNoPersistance(undoBlock
                  .getGenerationStamp() - 1);
              HashBuckets.getInstance().undoHash(sId, HdfsServerConstants.ReplicaState.FINALIZED, undoBlock);
            }
          }
          success = true;
          return locatedBlock;
        } catch (AccessControlException e) {
          logAuditEvent(false, "append", src);
          throw e;
        } finally {
          //do not put data in the cache as it may be too big.
          //recover the data value if needed when geting from cache.
          byte[] locatedBlockBytes = null;
          if (locatedBlock != null) {
            LocatedBlock lb = new LocatedBlock(locatedBlock.getBlock(), locatedBlock.getLocations(), locatedBlock.
                getStorageIDs(), locatedBlock.getStorageTypes(), locatedBlock.getStartOffset(), locatedBlock.isCorrupt(),
                locatedBlock.getBlockToken());
            locatedBlockBytes = PBHelper.convert(lb).toByteArray();
          }
          RetryCacheDistributed.setState(cacheEntry, success, locatedBlockBytes);
        }
      }
    };
    return (LocatedBlock) appendFileHandler.handle(this);
  }

  private LocatedBlock appendFileInt(String src, String holder,
      String clientMachine) throws
      IOException {

    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
              DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }
    LocatedBlock lb;
    FSPermissionChecker pc = getPermissionChecker();
    lb = appendFileInternal(pc, src, holder, clientMachine);
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "DIR* NameSystem.appendFile: file " + src + " for " + holder +
                " at " + clientMachine + " block " + lb.getBlock() +
                " block size " + lb.getBlock().getNumBytes());
      }
    }
    logAuditEvent(true, "append", src);
    return lb;
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler additionalBlockHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_BLOCK, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il)
                    .add(lf.getLeaseLock(LockType.READ, clientName))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                    .add(lf.getLastTwoBlocksLock(src))
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC));
          }

          @Override
          public Object performTask() throws IOException {
            long blockSize;
            int replication;
            Node clientNode;

            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.getAdditionalBlock: file " + src +
                      " for " + clientName);
            }

            // Part I. Analyze the state of the file with respect to the input data.
            LocatedBlock[] onRetryBlock = new LocatedBlock[1];
            final INodesInPath inodesInPath;
            try {
              inodesInPath = analyzeFileState(src, fileId, clientName,
                previous, onRetryBlock);
            } catch(IOException e) {
              throw e;
            }
            INode[] inodes = inodesInPath.getINodes();
            final INodeFile pendingFile = inodes[inodes.length - 1].asFile();

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
            
            clientNode = pendingFile.getFileUnderConstructionFeature().getClientNode() == null ? null :
                getBlockManager().getDatanodeManager()
                    .getDatanode(pendingFile.getFileUnderConstructionFeature().getClientNode());

            replication = pendingFile.getBlockReplication();

            // Get the storagePolicyID of this file
            byte storagePolicyID = pendingFile.getStoragePolicyID();

            // choose targets for the new block to be allocated.
            final DatanodeStorageInfo targets[] = getBlockManager().chooseTarget4NewBlock(
                src, replication, clientNode, excludedNodes, blockSize,
                favoredNodes, storagePolicyID);

            // Part II.
            // Allocate a new block, add it to the INode and the BlocksMap.
            Block newBlock;
            long offset;
            onRetryBlock = new LocatedBlock[1];
            INodesInPath iNodesInPath2 =
                analyzeFileState(src, fileId, clientName, previous, onRetryBlock);
            INode[] inodes2 = iNodesInPath2.getINodes();
            final INodeFile pendingFile2 = inodes2[inodes2.length - 1].asFile();

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
            commitOrCompleteLastBlock(pendingFile2,
                ExtendedBlock.getLocalBlock(previous));

            // allocate new block, record block locations in INode.
            newBlock = createNewBlock(pendingFile2);
            saveAllocatedBlock(src, iNodesInPath2, newBlock, targets);


            dir.persistNewBlock(src, pendingFile2);
            offset = pendingFile2.computeFileSize(true);

            Lease lease = leaseManager.getLease(clientName);
            lease.updateLastTwoBlocksInLeasePath(src, newBlock,
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

  private INodesInPath analyzeFileState(String src, long fileId, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
      throws IOException {

    checkBlock(previous);
    onRetryBlock[0] = null;
    checkNameNodeSafeMode("Cannot add block to " + src);

    // have we exceeded the configured limit of fs objects.
    checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(src, true);
    final INode[] inodes = inodesInPath.getINodes();
    
    final INodeFile pendingFile =
        checkLease(src, fileId, clientName, inodes[inodes.length - 1]);
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
        return inodesInPath;
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }

    // Check if the penultimate block is minimally replicated
    if (!checkFileProgress(pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src +
              " block " + pendingFile.getPenultimateBlock());
    }
    return inodesInPath;
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
  LocatedBlock getAdditionalDatanode(final String src1, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final String[] storageIDs,
      final HashSet<Node> excludes, final int numAdditionalNodes,
      final String clientName) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler getAdditionalDatanodeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_DATANODE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il).add(lf.getLeaseLock(LockType.READ, clientName));
          }

          @Override
          public Object performTask() throws IOException {
            //check if the feature is enabled
            dtpReplaceDatanodeOnFailure.checkEnabled();

            final DatanodeDescriptor clientNode;
            final long preferredBlockSize;
            final List<DatanodeStorageInfo> chosen;
            //check safe mode
            checkNameNodeSafeMode("Cannot add datanode; src=" + src + ", blk=" + blk);

            //check lease
            final INodeFile file = checkLease(src,
                clientName, false);
            //clientNode = file.getClientNode(); HOP
            clientNode = file.getFileUnderConstructionFeature().getClientNode() == null ? null :
                getBlockManager().getDatanodeManager()
                    .getDatanode(file.getFileUnderConstructionFeature().getClientNode());
            preferredBlockSize = file.getPreferredBlockSize();

            byte storagePolicyID = file.getStoragePolicyID();

            //find datanode storages
            final DatanodeManager dm = blockManager.getDatanodeManager();
            chosen = Arrays.asList(dm.getDatanodeStorageInfos(existings, storageIDs));

            // choose new datanodes.
            final DatanodeStorageInfo[] targets = blockManager.chooseTarget4AdditionalDatanode(
                src, numAdditionalNodes, clientNode, chosen,
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
  boolean abandonBlock(final ExtendedBlock b, final String src1,
      final String holder) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler abandonBlockHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.ABANDON_BLOCK,
            src) {

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
            locks.add(il).add(lf.getLeaseLock(LockType.READ))
                    .add(lf.getLeasePathLock(LockType.READ_COMMITTED, src))
                    .add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR));
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
            checkNameNodeSafeMode("Cannot abandon block " + b + " for fle" + src);
            INodeFile file = checkLease(src, holder, false);
            boolean removed = dir.removeBlock(src, file, ExtendedBlock.getLocalBlock(b));
            if (!removed) {
              return true;
            }
            leaseManager.getLease(holder).updateLastTwoBlocksInLeasePath(src,
                file.getLastBlock(), file.getPenultimateBlock());

            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.abandonBlock: " + b +
                      " is removed from pendingCreates");
            }
            dir.persistBlocks(src, file);
            file.recomputeFileSize();

            return true;
          }
        };
    return (Boolean) abandonBlockHandler.handle(this);
  }

  // make sure that we still have the lease on this file.
  private INodeFile checkLease(String src, String holder)
      throws LeaseExpiredException, UnresolvedLinkException, StorageException,
      TransactionContextException, FileNotFoundException {
    return checkLease(src, INodeDirectory.ROOT_PARENT_ID, holder, true);
  }

  private INodeFile checkLease(String src, String holder,
      boolean updateLastTwoBlocksInFile) throws LeaseExpiredException,
      UnresolvedLinkException, StorageException,
      TransactionContextException, FileNotFoundException {
    return checkLease(src, INodeDirectory.ROOT_PARENT_ID, holder, updateLastTwoBlocksInFile);
  }

  private INodeFile checkLease(String src, int fileId, String holder,
      boolean updateLastTwoBlocksInFile) throws LeaseExpiredException,
      UnresolvedLinkException, StorageException,
      TransactionContextException, FileNotFoundException {
    return checkLease(src, fileId, holder, dir.getINode(src), updateLastTwoBlocksInFile);
  }

  private INodeFile checkLease(String src, long fileId, String holder,
      INode file) throws LeaseExpiredException, StorageException,
      TransactionContextException, FileNotFoundException {
    return checkLease(src, fileId, holder, file, true);
  }

  private INodeFile checkLease(String src, long fileId, String holder,
      INode inode, boolean updateLastTwoBlocksInFile) throws
      LeaseExpiredException, StorageException,
      TransactionContextException, FileNotFoundException {
    final INodeFile file = inode.asFile();
    if (file == null || !(file instanceof INodeFile)) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + src + ": File does not exist. " +
              (lease != null ? lease.toString() :
                  "Holder " + holder + " does not have any open files."));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException(
          "No lease on " + src + ": File is not open for writing. " +
              (lease != null ? lease.toString() :
                  "Holder " + holder + " does not have any open files."));
    }
    String clientName = file.getFileUnderConstructionFeature().getClientName();
    if (holder != null && !clientName.equals(holder)) {
      throw new LeaseExpiredException(
          "Lease mismatch on " + src + " owned by " +
              clientName + " but is accessed by " + holder);
    }

    if(updateLastTwoBlocksInFile) {
      file.getFileUnderConstructionFeature().updateLastTwoBlocks(leaseManager.getLease(holder), src);
    }
    INode.checkId(fileId, file);
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
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler completeFileHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.COMPLETE_FILE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.WRITE, INodeResolveType.PATH, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il)
                    .add(lf.getLeaseLock(LockType.WRITE, holder))
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
    final INodesInPath iip = dir.getLastINodeInPath(src);
    INodeFile pendingFile;
    try {
      pendingFile = checkLease(src,fileId, holder, iip.getINode(0));
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
              "request from " + holder + " to complete " + src +
              " which is already closed. But, it appears to be an RPC " +
              "retry. Returning success");
          return true;
        }
      }
      throw lee;
    }
    // Check the state of the penultimate block. It should be completed
    // before attempting to complete the last one.
    if (!checkFileProgress(pendingFile, false)) {
      return false;
    }

    // commit the last block and complete it if it has minimum replicas
    commitOrCompleteLastBlock(pendingFile, last);

    if (!checkFileProgress(pendingFile, true)) {
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
    final INodesInPath iip = dir.getRootDir().getLastINodeInPath(src, true);
    pendingFile = checkLease(src,fileId, holder, iip.getINode(0));

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
          .getBlockReplication();
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
    NameNode.stateChangeLog.info(
        "BLOCK* allocateBlock: " + src + ". " + getBlockPoolId() + " " + b);
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
  private boolean checkFileProgress(INodeFile v, boolean checkAll)
      throws IOException {
    if (checkAll) {
      //
      // check all blocks of the file.
      //
      for (BlockInfo block : v.getBlocks()) {
        if (!block.isComplete()) {
          BlockInfo cBlock = blockManager
              .tryToCompleteBlock(v,
                  block.getBlockIndex());
          if (cBlock != null) {
            block = cBlock;
          }
          if (!block.isComplete()) {
            LOG.info("BLOCK* checkFileProgress: " + block +
                " has not reached minimal replication " +
                blockManager.minReplication);
            return false;
          }
        }
      }
    } else {
      //
      // check the penultimate block of this file
      //
      BlockInfo b = v.getPenultimateBlock();
      if (b != null && !b.isComplete()) {
        blockManager
            .tryToCompleteBlock(v, b.getBlockIndex());
        b = v.getPenultimateBlock();
        if (!b.isComplete()) {
          LOG.warn("BLOCK* checkFileProgress: " + b +
              " has not reached minimal replication " +
              blockManager.minReplication);
          return false;
        }

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
   * Remove the indicated file from namespace.
   *
   * @see ClientProtocol#delete(String, boolean) for detailed description and
   * description of exceptions
   */
  public boolean deleteWithTransaction(final String src1,
      final boolean recursive) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
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
                    .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE, BLK.IV))
                    .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
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
    try {
      return new FSPermissionChecker(fsOwnerShortUserName, superGroup, getRemoteUser());
    } catch (IOException ioe) {
      throw new AccessControlException(ioe);
    }
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
    if (!recursive && dir.isNonEmptyDirectory(src)) {
      throw new IOException(src + " is non empty");
    }
    if (enforcePermission && isPermissionEnabled) {
      checkPermission(pc, src, false, null, FsAction.WRITE, null, FsAction.ALL, false);
    }
    // Unlink the target directory from directory tree
    if (!dir.delete(src, collectedBlocks)) {
      return false;
    }

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
      throws StorageException, TransactionContextException {
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
    for (Block b : blocks.getToDeleteList()) {
      blockManager.removeBlock(b);
    }
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
  public HdfsFileStatus getFileInfo(final String src1, final boolean resolveLink)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    HopsTransactionalRequestHandler getFileInfoHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.GET_FILE_INFO,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH, src)
                    .resolveSymLink(resolveLink).setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(true);
            locks.add(il);
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            HdfsFileStatus stat;
            FSPermissionChecker pc = getPermissionChecker();
            try {
              boolean isSuperUser = true;
              if (isPermissionEnabled) {
                checkPermission(pc, src, false, null, null, null, null, resolveLink);
                isSuperUser = pc.isSuperUser();
              }
              stat = dir.getFileInfo(src, resolveLink, isSuperUser);
            } catch (AccessControlException e) {
              logAuditEvent(false, "getfileinfo", src);
              throw e;
            }
            logAuditEvent(true, "getfileinfo", src);
            return stat;
          }
        };
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException("Invalid file name: " + src);
    }
    return (HdfsFileStatus) getFileInfoHandler.handle(this);
  }

  /**
   * Returns true if the file is closed
   */
  boolean isFileClosed(final String src)
      throws AccessControlException, UnresolvedLinkException,
      StandbyException, IOException {
    HopsTransactionalRequestHandler isFileClosedHandler = new HopsTransactionalRequestHandler(
        HDFSOperationType.GET_FILE_INFO,
        src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.READ,INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        try {
          if (isPermissionEnabled) {
            checkTraverse(pc, src);
          }
          return !INodeFile.valueOf(dir.getINode(src), src).isUnderConstruction();
        } catch (AccessControlException e) {
          if (isAuditEnabled() && isExternalInvocation()) {
            logAuditEvent(false, UserGroupInformation.getCurrentUser(),
                getRemoteIp(),
                "isFileClosed", src, null, null);
          }
          throw e;
        }
      }
    };
    return (boolean) isFileClosedHandler.handle();
  }

  /**
   * Create all the necessary directories
   */
  boolean mkdirs(final String src1, final PermissionStatus permissions,
      final boolean createParent) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    final boolean resolvedLink = false;
    HopsTransactionalRequestHandler mkdirsHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.MKDIRS, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src)
                    .resolveSymLink(resolvedLink)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            locks.add(il);
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            try {
              return mkdirsInt(src, permissions, createParent);
            } catch (AccessControlException e) {
              logAuditEvent(false, "mkdirs", src);
              throw e;
            }
          }
        };
    return (Boolean) mkdirsHandler.handle(this);
  }

  private boolean mkdirsInt(String src, PermissionStatus permissions,
      boolean createParent)
      throws IOException {
    HdfsFileStatus resultingStat = null;
    boolean status = false;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug(this.getNamenodeId() + ") DIR* NameSystem.mkdirs: " + src);
    }
    FSPermissionChecker pc = getPermissionChecker();
    status = mkdirsInternal(pc, src, permissions, createParent);
    if (status) {
      resultingStat = dir.getFileInfo(src, false, false);
    }

    if (status) {
      logAuditEvent(true, "mkdirs", src, null, resultingStat);
    }
    return status;
  }

  /**
   * Create all the necessary directories
   */
  private boolean mkdirsInternal(FSPermissionChecker pc, String src,
      PermissionStatus permissions, boolean createParent)
      throws IOException {
    checkNameNodeSafeMode("Cannot create directory " + src);
    if (isPermissionEnabled) {
      checkTraverse(pc, src);
    }
    if (dir.isDir(src)) {
      // all the users of mkdirs() are used to expect 'true' even if
      // a new directory is not created.
      return true;
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    if (isPermissionEnabled) {
      checkAncestorAccess(pc, src, FsAction.WRITE);
    }
    if (!createParent) {
      verifyParentDir(src);
    }

    // validate that we have enough inodes. This is, at best, a
    // heuristic because the mkdirs() operation might need to
    // create multiple inodes.
    checkFsObjectLimit();

    if (!dir.mkdirs(src, permissions, false, now())) {
      throw new IOException("Failed to create directory: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(final String src)
      throws
      IOException {
    return multiTransactionalGetContentSummary(src);
  }

  /**
   * Persist all metadata about this file.
   *
   * @param src
   *     The string representation of the path
   * @param clientName
   *     The string representation of the client
   * @param lastBlockLength
   *     The length of the last block
   *     under construction reported from client.
   * @throws IOException
   *     if path does not exist
   */
  void fsync(final String src, final String clientName,
      final long lastBlockLength) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.FSYNC, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, src)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getLeaseLock(LockType.READ, clientName))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
            .add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        NameNode.stateChangeLog
            .info("BLOCK* fsync: " + src + " for " + clientName);
        checkNameNodeSafeMode("Cannot fsync file " + src);
        INodeFile pendingFile = checkLease(src, clientName);
        if (lastBlockLength > 0) {
          pendingFile.getFileUnderConstructionFeature().updateLengthOfLastBlock(pendingFile, lastBlockLength);
        }
        dir.persistBlocks(src, pendingFile);
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
    boolean penultimateBlockMinReplication;
    BlockUCState penultimateBlockState;
    if (penultimateBlock == null) {
      penultimateBlockState = BlockUCState.COMPLETE;
      // If penultimate block doesn't exist then its minReplication is met
      penultimateBlockMinReplication = true;
    } else {
      penultimateBlockState = BlockUCState.COMMITTED;
      penultimateBlockMinReplication =
          blockManager.checkMinReplication(penultimateBlock);
    }
    assert penultimateBlockState == BlockUCState.COMPLETE ||
        penultimateBlockState == BlockUCState.COMMITTED :
        "Unexpected state of penultimate block in " + src;

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
            -diff * fileINode.getBlockReplication());
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
    dir.closeFile(src, newFile);

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
        INodeFile iFile = ((INode)storedBlock.getBlockCollection()).asFile();
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
          src = persistBlocks(iFile);
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

  /**
   * Persist the block list for the given file.
   *
   * @param pendingFile
   * @return Path to the given file.
   * @throws IOException
   */
  @VisibleForTesting
  String persistBlocks(INodeFile pendingFile)
      throws IOException {
    String src = pendingFile.getFullPathName();
    dir.persistBlocks(src, pendingFile);
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
  DirectoryListing getListing(final String src1, byte[] startAfter1,
      final boolean needLocation)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    String startAfterString = new String(startAfter1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);

    // Get file name when startAfter is an INodePath
    if (FSDirectory.isReservedName(startAfterString)) {
      byte[][] startAfterComponents = FSDirectory
          .getPathComponentsForReservedPath(startAfterString);
      try {
        String tmp = FSDirectory.resolvePath(src, startAfterComponents, dir);
        byte[][] regularPath = INode.getPathComponents(tmp);
        startAfter1 = regularPath[regularPath.length - 1];
      } catch (IOException e) {
        // Possibly the inode is deleted
        throw new DirectoryListingStartAfterNotFoundException(
            "Can't find startAfter " + startAfterString);
      }
    }

    final byte[] startAfter = startAfter1;

    HopsTransactionalRequestHandler getListingHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.GET_LISTING,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getINodeLock( INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(true);
            locks.add(il);
            if (needLocation) {
              locks.add(lf.getBlockLock()).add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC));
            }
            locks.add(lf.getAcesLock());
          }

          @Override
          public Object performTask() throws IOException {
            try {
              return getListingInt(src, startAfter, needLocation);
            } catch (AccessControlException e) {
              logAuditEvent(false, "listStatus", src);
              throw e;
            }
          }
        };
    return (DirectoryListing) getListingHandler.handle(this);
  }

  private DirectoryListing getListingInt(String src, byte[] startAfter,
      boolean needLocation)
      throws IOException {
    DirectoryListing dl;
    FSPermissionChecker pc = getPermissionChecker();
    boolean isSuperUser = true;
    if (isPermissionEnabled) {
      if (dir.isDir(src)) {
        checkPathAccess(pc, src, FsAction.READ_EXECUTE);
      } else {
        checkTraverse(pc, src);
      }
      isSuperUser = pc.isSuperUser();
    }
    logAuditEvent(true, "listStatus", src);
    dl = dir.getListing(src, startAfter, needLocation, isSuperUser);
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
      StorageReport[] reports, int xceiverCount,
      int xmitsInProgress, int failedVolumes) throws IOException {
    final int maxTransfer =
        blockManager.getMaxReplicationStreams() - xmitsInProgress;

    DatanodeCommand[] cmds = blockManager.getDatanodeManager()
        .handleHeartbeat(nodeReg, reports, blockPoolId, xceiverCount,
            maxTransfer, failedVolumes);

    return new HeartbeatResponse(cmds);
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
          if (!nameNodeHasResourcesAvailable()) {
            String lowResourcesMsg = "NameNode low on available disk space. ";
            if (!isInSafeMode()) {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Entering safe mode.");
            } else {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Already in safe mode.");
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

  @Metric({"ExpiredHeartbeats", "Number of expired heartbeats"})
  public int getExpiredHeartbeats() {
    return datanodeStatistics.getExpiredHeartbeats();
  }

  /**
   * @see ClientProtocol#getStats()
   */
  long[] getStats() throws IOException {
    final long[] stats = datanodeStatistics.getStats();
    stats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] =
        getUnderReplicatedBlocks();
    stats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] =
        getCorruptReplicaBlocks();
    stats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] =
        getMissingBlocksCount();
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
    private long reached = -1;
    /**
     * Total number of blocks.
     */
    int blockTotal;
    /**
     * Number of blocks needed to satisfy safe mode threshold condition
     */
    private int blockThreshold;
    /**
     * Number of blocks needed before populating replication queues
     */
    private int blockReplicationQueueThreshold;
    /**
     * time of the last status printout
     */
    private long lastStatusReport = 0;
    /**
     * flag indicating whether replication queues have been initialized
     */
    boolean initializedReplicationQueues = false;
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
    private SafeModeInfo(Configuration conf) {
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
      this.blockTotal = 0;
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
    private SafeModeInfo(boolean resourcesLow, boolean isReplQueuesInited) throws IOException {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replicationQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.resourcesLow = resourcesLow;
      this.initializedReplicationQueues = isReplQueuesInited;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

    /**
     * Check if safe mode is on.
     *
     * @return true if in safe mode
     */
    private boolean isOn() throws IOException {
      doConsistencyCheck();
      return this.reached >= 0 && isClusterInSafeMode();
    }

    /**
     * Check if we are populating replication queues.
     */
    private boolean isPopulatingReplicationQueues() {
      return initializedReplicationQueues;
    }

    /**
     * Enter safe mode.
     */
    private void enter() {
      this.reached = 0;
    }

    /**
     * Leave safe mode.
     * <p/>
     * Check for invalid, under- & over-replicated blocks in the end of
     * startup.
     */
    private void leave() throws IOException {
      // if not done yet, initialize replication queues.
      // In the standby, do not populate replication queues
      if (!isPopulatingReplicationQueues() && shouldPopulateReplicationQueues()) {
        initializeReplicationQueues();
      }

      leaveInternal();

      HdfsVariables.exitClusterSafeMode();
      HdfsVariables.resetMisReplicatedIndex();
      clearSafeBlocks();
    }

    private void leaveInternal() throws IOException {
      long timeInSafeMode = now() - startTime;
      NameNode.stateChangeLog.info(
          "STATE* Leaving safe mode after " + timeInSafeMode / 1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafeMode);

      //Log the following only once (when transitioning from ON -> OFF)
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF");
      }
      reached = -1;
      safeMode = null;
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
     * Initialize replication queues.
     */
    private void initializeReplicationQueues() throws IOException {
      LOG.info("initializing replication queues");
      assert !isPopulatingReplicationQueues() : "Already initialized " +
          "replication queues";
      long startTimeMisReplicatedScan = now();
      blockManager.processMisReplicatedBlocks();
      initializedReplicationQueues = true;
      NameNode.stateChangeLog.info("STATE* Replication Queue initialization " +
          "scan for invalid, over- and under-replicated blocks " +
          "completed in " + (now() - startTimeMisReplicatedScan) + " ms");
    }

    /**
     * Check whether we have reached the threshold for
     * initializing replication queues.
     */
    private boolean canInitializeReplicationQueues() throws IOException {
      return shouldPopulateReplicationQueues() &&
          blockSafe() >= blockReplicationQueueThreshold;
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
      if (reached == 0 && isClusterInSafeMode()) {
        return false;
      }
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }

    /**
     * This NameNode tries to help the cluster to get out of safe mode by
     * updating the safe block count.
     * This call will trigger the @link{SafeModeMonitor} if it's not already
     * started.
     * @throws IOException
     */
    private void tryToHelpToGetOut() throws IOException{
      if (isManual() && !resourcesLow) {
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
      if (!isClusterInSafeMode()) {
        return false;
      }
      return (threshold != 0 && blockSafe() < blockThreshold) ||
          (datanodeThreshold != 0 && getNumLiveDataNodes() < datanodeThreshold) ||
          (!nameNodeHasResourcesAvailable());
    }

    /**
     * Check and trigger safe mode if needed.
     */

    private void checkMode() throws IOException {

      // if smmthread is already running, the block threshold must have been 
      // reached before, there is no need to enter the safe mode again
      if (smmthread == null && needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplicationQueues() && !isPopulatingReplicationQueues()) {
          initializeReplicationQueues();
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
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = now();
      startSafeModeMonitor();

      reportStatus("STATE* Safe mode extension entered.", true);

      // check if we are ready to initialize replication queues
      if (canInitializeReplicationQueues() && !isPopulatingReplicationQueues()) {
        initializeReplicationQueues();
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
    private synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      this.blockReplicationQueueThreshold = (int) (blockTotal *
          replicationQueueThreshold);
      setSafeModePendingOperation(true);
    }

    /**
     * Increment number of safe blocks if current block has
     * reached minimal replication.
     *
     * @param blk
     *     current block
     */
    private void incrementSafeBlockCount(Block blk) throws IOException {
      addSafeBlock(blk.getBlockId());

      // Report startup progress only if we haven't completed startup yet.
        StartupProgress prog = NameNode.getStartupProgress();
        if (prog.getStatus(Phase.SAFEMODE) != Status.COMPLETE) {
          if (this.awaitingReportedBlocksCounter == null) {
            this.awaitingReportedBlocksCounter = prog.getCounter(Phase.SAFEMODE,
              STEP_AWAITING_REPORTED_BLOCKS);
          }
          this.awaitingReportedBlocksCounter.increment();
        }

      setSafeModePendingOperation(true);
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
      String leaveMsg = "It was turned on manually. ";
      if (areResourcesLow()) {
        leaveMsg = "Resources are low on NN. Please add or free up more "
          + "resources then turn off safe mode manually. NOTE:  If you turn off"
          + " safe mode before adding resources, "
          + "the NN will immediately return to safe mode. ";
      }
      if (isManual() || areResourcesLow()) {
        return leaveMsg
          + "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
      }


      //Automatic safemode. System will come out of safemode automatically.
      leaveMsg = "Safe mode will be turned off automatically";
      int numLive = getNumLiveDataNodes();
      String msg = "";

      long blockSafe;
      try {
        blockSafe = blockSafe();
      } catch (IOException ex) {
        LOG.error(ex);
        return "got exception " + ex.getMessage();
      }
      if (reached == 0) {
        if (blockSafe < blockThreshold) {
          msg += String.format("The reported blocks %d needs additional %d" +
                  " blocks to reach the threshold %.4f of total blocks %d.\n",
              blockSafe, (blockThreshold - blockSafe) + 1, threshold,
              blockTotal);
        }
        if (numLive < datanodeThreshold) {
          msg += String.format(
              "The number of live datanodes %d needs an additional %d live " +
                  "datanodes to reach the minimum number %d.\n", numLive,
              (datanodeThreshold - numLive), datanodeThreshold);
        }
      } else {
        msg = String.format("The reported blocks %d has reached the threshold" +
            " %.4f of total blocks %d.", blockSafe, threshold, blockTotal);

        if (datanodeThreshold > 0) {
          msg += String.format("The number of live datanodes %d has reached " +
              "the minimum number %d.", numLive, datanodeThreshold);
        }
      }
      msg += leaveMsg;
      // threshold is not reached or manual or resources low
      if (reached == 0 ||
          (isManual() && !areResourcesLow())) {  // threshold is not reached or manual
        return msg;
      }
      // extension period is in progress
      return msg + (reached + extension - now() > 0 ?
        " in " + (reached + extension - now()) / 1000 + " seconds."
        : " soon.");
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
      try {
        blockSafe = "" + blockSafe();
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

    private void adjustBlockTotals(int deltaSafe, int deltaTotal)
        throws IOException {
      //FIXME ?!
    }

    private void setSafeModePendingOperation(Boolean val) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SafeModeX Some operation are put on hold");
      }
      safeModePendingOperation.set(val);
    }

    private void adjustSafeBlocks(Set<Long> safeBlocks) throws IOException {
      int lastSafeBlockSize = blockSafe();
      addSafeBlocks(safeBlocks);
      int newSafeBlockSize = blockSafe();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adjusting safe blocks from " + lastSafeBlockSize + "/" +
            blockTotal + " to " + newSafeBlockSize + "/" + blockTotal);
      }

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
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  @Override
  public boolean isInSafeMode() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return false;
    }
    if (!isClusterInSafeMode()) {
      safeMode.clusterLeftSafeModeAlready();
      return false;
    } else {
      safeMode.tryToHelpToGetOut();
    }
    return safeMode.isOn();
  }

  @Override
  public boolean isInStartupSafeMode() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
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
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplicationQueues()) {
      return false;
    }
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return true;
    }
    return safeMode.isPopulatingReplicationQueues();
  }

  private boolean shouldPopulateReplicationQueues() {
    return true;
  }

  @Override
  public void incrementSafeBlockCount(BlockInfo blk) throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return;
    }
    safeMode.incrementSafeBlockCount(blk);
  }

  @Override
  public void decrementSafeBlockCount(BlockInfo b)
      throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
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
  public void adjustSafeModeBlockTotals(int deltaSafe, int deltaTotal)
      throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
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
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return;
    }
    safeMode.setBlockTotal(blockManager.getTotalCompleteBlocks());
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
    long numUCBlocks = 0;
    for (final Lease lease : leaseManager.getSortedLeases()) {

      final HopsTransactionalRequestHandler ucBlocksHandler = new HopsTransactionalRequestHandler(
          HDFSOperationType.GET_LISTING) {
        private Set<String> leasePaths = null;

        @Override
        public void setUp() throws StorageException {
          String holder = lease.getHolder();
          leasePaths = INodeUtil.findPathsByLeaseHolder(holder);
          if (leasePaths != null) {
            LOG.debug("Total Paths " + leasePaths.size() + " Paths: " + Arrays.toString(leasePaths.toArray()));
          }
        }

        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          String holder = lease.getHolder();
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(INodeLockType.READ, INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN,
                  leasePaths.toArray(new String[leasePaths.size()]))
                  .setNameNodeID(nameNode.getId())
                  .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
          locks.add(il).add(lf.getLeaseLock(LockType.READ, holder))
                  .add(lf.getLeasePathLock(LockType.READ)).add(lf.getBlockLock())
                  .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
        }

        @Override
        public Object performTask() throws IOException {
          int numUCBlocks = 0;
          for (LeasePath leasePath : lease.getPaths()) {
            final String path = leasePath.getPath();
            final INodeFile cons;
            try {
              cons = dir.getINode(path).asFile();
              Preconditions.checkState(cons.isUnderConstruction());
            } catch (UnresolvedLinkException e) {
              throw new AssertionError("Lease files should reside on this FS");
            }
            BlockInfo[] blocks = cons.getBlocks();
            if (blocks == null) {
              continue;
            }
            for (BlockInfo b : blocks) {
              if (!b.isComplete()) {
                numUCBlocks++;
              }
            }
          }
          return numUCBlocks;
        }
      };

      numUCBlocks += (int) ucBlocksHandler.handle();
    }
    LOG.info("Number of blocks under construction: " + numUCBlocks);
    return getBlocksTotal() - numUCBlocks;
  }

  /**
   * Enter safe mode. If resourcesLow is false, then we assume it is manual
   *
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    // Stop the secret manager, since rolling the master key would
    // try to write to the edit log
    stopSecretManager();

    if (safeMode != null) {
      if (resourcesLow) {
        safeMode.setResourcesLow();
      } else {
        safeMode.setManual();
      }
    }
    if (!isInSafeMode()) {
      safeMode = new SafeModeInfo(resourcesLow, isPopulatingReplQueues());
      HdfsVariables.enterClusterSafeMode();
      return;
    }
    if (resourcesLow) {
      safeMode.setResourcesLow();
    } else {
      safeMode.setManual();
    }

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
    safeMode.leave();
  }

  String getSafeModeTip() throws IOException {
    if (!isInSafeMode()) {
      return "";
    }
    return safeMode.getTurnOffTip();
  }

  public void processIncrementalBlockReport(DatanodeRegistration nodeReg, StorageReceivedDeletedBlocks r)
      throws IOException {
      blockManager.processIncrementalBlockReport(nodeReg, r);
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), superGroup,
        permission);
  }

  private void checkOwner(FSPermissionChecker pc, String path)
      throws IOException {
    checkPermission(pc, path, true, null, null, null, null);
  }

  private void checkPathAccess(FSPermissionChecker pc, String path,
      FsAction access)
      throws IOException {
    checkPermission(pc, path, false, null, null, access, null);
  }

  private void checkParentAccess(FSPermissionChecker pc, String path,
      FsAction access)
      throws IOException {
    checkPermission(pc, path, false, null, access, null, null);
  }

  private void checkAncestorAccess(FSPermissionChecker pc, String path,
      FsAction access)
      throws IOException {
    checkPermission(pc, path, false, access, null, null, null);
  }

  private void checkTraverse(FSPermissionChecker pc, String path)
      throws IOException {
    checkPermission(pc, path, false, null, null, null, null);
  }

  @Override
  public void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission}.
   */
  private void checkPermission(FSPermissionChecker pc, String path,
      boolean doCheckOwner, FsAction ancestorAccess, FsAction parentAccess,
      FsAction access, FsAction subAccess)throws AccessControlException, UnresolvedLinkException, IOException{
    checkPermission(pc, path, doCheckOwner, ancestorAccess,
        parentAccess, access, subAccess, true);
  }

  /**
   * Check whether current user have permissions to access the path. For more
   * details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}}.
   */
  private void checkPermission(FSPermissionChecker pc,
      String path, boolean doCheckOwner, FsAction ancestorAccess,
      FsAction parentAccess, FsAction access, FsAction subAccess,
      boolean resolveLink) throws AccessControlException, UnresolvedLinkException, TransactionContextException,
      IOException {
    if (!pc.isSuperUser()) {
      pc.checkPermission(path, dir.getRootDir(), doCheckOwner, ancestorAccess,
          parentAccess, access, subAccess, resolveLink);
    }
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  private void checkFsObjectLimit() throws IOException {
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

  @Metric
  public long getExcessBlocks() {
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

            if(dir.isQuotaEnabled()){
              long diff = pendingFile.getPreferredBlockSize() - block
                  .getNumBytes();
              dir.updateSpaceConsumed(pendingFile.getFullPathName(), 0, diff
                  * pendingFile.getBlockReplication());
            }
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
                .add(lf.getLastBlockHashBucketsLock())
                .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
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

          LOG.info("updatePipeline(block=" + oldBlock
              + ", newGenerationStamp=" + newBlock.getGenerationStamp()
              + ", newLength=" + newBlock.getNumBytes()
              + ", newNodes=" + Arrays.asList(newNodes)
              + ", clientName=" + clientName
              + ")");

          updatePipelineInternal(clientName, oldBlock, newBlock, newNodes,
              newStorageIDs);
          LOG.info("updatePipeline(" + oldBlock + ") successfully to " + newBlock);
          success = true;
          return null;
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }

      }
    }.handle(this);
  }

  /**
   * @see #updatePipeline(String, ExtendedBlock, ExtendedBlock, DatanodeID[], String[])
   */
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
    if (!isPopulatingReplQueues()) {
      throw new IOException("Cannot run listCorruptFileBlocks because " +
          "replication queues have not been initialized.");
    }
    // print a limited # of corrupt files per call
    final int[] count = {0};
    final ArrayList<CorruptFileBlockInfo> corruptFiles =
        new ArrayList<>();

    final Iterator<Block> blkIterator =
        blockManager.getCorruptReplicaBlockIterator();

    if (cookieTab == null) {
      cookieTab = new String[]{null};
    }
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
  private boolean isExternalInvocation() {
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

      info.put(node.getHostName(), innerInfo);
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
      info.put(node.getHostName(), innerInfo);
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
      info.put(node.getHostName(), innerInfo);
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
  public SafeModeInfo getSafeModeInfoForTests() {
    return safeMode;
  }

  @Override
  public boolean isAvoidingStaleDataNodesForWrite() {
    return this.blockManager.getDatanodeManager()
        .shouldAvoidStaleDataNodesForWrite();
  }

  @Override // FSClusterStats
  public int getNumDatanodesInService() {
    return datanodeStatistics.getNumDatanodesInService();
  }

  @Override
  public double getInServiceXceiverAverage() {
    double avgLoad = 0;
    final int nodes = getNumDatanodesInService();
    if (nodes != 0) {
      final int xceivers = datanodeStatistics.getInServiceXceiverCount();
      avgLoad = (double)xceivers/nodes;
    }
    return avgLoad;
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
    SafeModeInfo safeMode = this.safeMode;
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

  public void flushCache(String userName, String groupName){
    Users.flushCache(userName, groupName);
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
    SafeModeInfo safeMode = this.safeMode;
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
  private void addSafeBlock(final Long safeBlock) throws IOException {
    Set<Long> safeBlocks = new HashSet<>();
    safeBlocks.add(safeBlock);
    addSafeBlocks(safeBlocks);
  }

  /**
   * Remove a block that is not considered safe anymore
   * @param safeBlock
   *      block to be removed from safe blocks
   * @throws IOException
   */
  private void removeSafeBlock(final Long safeBlock) {
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.remove(safeBlock);
        return null;
      }
    };
  }

  /**
   * Update safe blocks in the database
   * @param safeBlocks
   *      list of blocks to be added to safe blocks
   * @throws IOException
   */
  private void addSafeBlocks(final Set<Long> safeBlocks) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ADD_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.insert(safeBlocks);
        return null;
      }
    }.handle();
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

  /**
   * Check if the cluster is in safe mode?
   * @return true if the cluster in safe mode, false otherwise.
   * @throws IOException
   */
  private boolean isClusterInSafeMode() throws IOException {
    return HdfsVariables.isClusterInSafeMode();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }

  public ExecutorService getSubtreeOperationsExecutor() {
    return subtreeOperationsExecutor;
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
    final String path = FSDirectory.resolvePath(path1, pathComponents, dir);
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
      Iterator<Integer> idIterator =
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
   * Creates the content summary of a directory tree in multiple transactions.
   * Creating the content summary of a large directory tree might take to much
   * time for a single transaction. Hence, this function first builds up an
   * in-memory representation of the directory tree before reading its attributes
   * level by level. The directory tree is locked during the operation to prevent
   * any concurrent modification.
   *
   * @param path1
   *    the path
   * @return
   *    the content summary for the given path
   * @throws IOException
   */
  // [S] what if you call content summary on the root
  // I have removed sub tree locking from the content summary for now
  // TODO : fix content summary sub tree locking
  //
  private ContentSummary multiTransactionalGetContentSummary(final String path1)
      throws
      IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(path1);
    final String path = FSDirectory.resolvePath(path1, pathComponents, dir);
      PathInformation pathInfo = getPathExistingINodesFromDB(path,
              false, null, null, null, null);
      if(pathInfo.getPathInodes()[pathInfo.getPathComponents().length-1] == null){
        throw new FileNotFoundException("File does not exist: " + path);
      }
      final INode subtreeRoot = pathInfo.getPathInodes()[pathInfo.getPathComponents().length-1];
      final INodeAttributes subtreeAttr = pathInfo.getSubtreeRootAttributes();
      final INodeIdentifier subtreeRootIdentifier = new INodeIdentifier(subtreeRoot.getId(),subtreeRoot.getParentId(),
          subtreeRoot.getLocalName(),subtreeRoot.getPartitionId());
      subtreeRootIdentifier.setDepth(((short) (INodeDirectory.ROOT_DIR_DEPTH + pathInfo.getPathComponents().length-1 )));


    //Calcualte subtree root default ACLs to be inherited in the tree.
    List<AclEntry> nearestDefaultsForSubtree = calculateNearestDefaultAclForSubtree(pathInfo);

    final AbstractFileTree.CountingFileTree fileTree =
            new AbstractFileTree.CountingFileTree(this, subtreeRootIdentifier, FsAction.READ_EXECUTE, nearestDefaultsForSubtree);
    fileTree.buildUp();

    return new ContentSummary(fileTree.getFileSizeSummary(),
        fileTree.getFileCount(), fileTree.getDirectoryCount(),
        subtreeAttr == null ? subtreeRoot.getQuotaCounts().get(Quota.NAMESPACE) : subtreeAttr.getQuotaCounts().get(
            Quota.NAMESPACE),
        fileTree.getDiskspaceCount(), subtreeAttr == null ? subtreeRoot
            .getQuotaCounts().get(Quota.DISKSPACE) : subtreeAttr.getQuotaCounts().get(Quota.DISKSPACE));

  }

  /**
   * Renaming a directory tree in multiple transactions. Renaming a large
   * directory tree might take to much time for a single transaction when
   * its quota counts need to be calculated. Hence, this functions first
   * reads up the whole tree in multiple transactions while calculating its
   * quota counts before executing the rename in a single transaction.
   * The subtree is locked during these operations in order to prevent any
   * concurrent modification.
   *
   * @param src1
   *    the source
   * @param dst1
   *    the destination
   * @throws IOException
   */
  void multiTransactionalRename(final String src1, final String dst1,
      final Options.Rename... options) throws IOException {
    //only for testing
    saveTimes();

    CacheEntry cacheEntry = retryCacheWaitForCompletionTransactional();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return; // Return previous response
    }
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.multiTransactionalRename: with options - " + src1
              + " to " + dst1);
    }
    if (!DFSUtil.isValidName(dst1)) {
      throw new InvalidPathException("Invalid name: " + dst1);
    }
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    pathComponents = FSDirectory.getPathComponentsForReservedPath(dst1);
    final String dst = FSDirectory.resolvePath(dst1, pathComponents, dir);

    boolean success = false;

    try {
    if (isInSafeMode()) {
      checkNameNodeSafeMode("Cannot rename " + src);
    }

    if (dst.equals(src)) {
      throw new FileAlreadyExistsException(
          "The source " + src + " and destination " + dst + " are the same");
    }
    // dst cannot be a directory or a file under src
    if (dst.startsWith(src)
        && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      String error = "Rename destination " + dst
          + " is a directory or file under source " + src;
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    //--
    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }
    String error;
    PathInformation srcInfo = getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INode[] srcInodes = srcInfo.getPathInodes();
    INode srcInode = srcInodes[srcInodes.length - 1];
    // validate source
    if (srcInode == null) {
      error = "rename source " + src + " is not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (srcInodes.length == 1) {
      error = "rename source cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    if (srcInode.isSymlink()
        && dst.equals(((INodeSymlink) srcInode).getSymlinkString())) {
      throw new FileAlreadyExistsException(
          "Cannot rename symlink " + src + " to its target " + dst);
    }

    //validate dst
    PathInformation dstInfo = getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    INode[] dstInodes = dstInfo.getPathInodes();
    INode dstInode = dstInodes[dstInodes.length - 1];
    if (dstInodes.length == 1) {
      error = "rename destination cannot be the root";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new IOException(error);
    }
    if (dstInode != null) { // Destination exists
      // It's OK to rename a file to a symlink and vice versa
      if (dstInode.isDirectory() != srcInode.isDirectory()) {
        error = "Source " + src + " and destination " + dst
            + " must both be directories";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
      if (!overwrite) { // If destination exists, overwrite flag must be true
        error = "rename destination " + dst + " already exists";
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new FileAlreadyExistsException(error);
      }

      short depth = (short) (INodeDirectory.ROOT_DIR_DEPTH + dstInfo.getPathInodes().length-1);
      boolean areChildrenRandomlyPartitioned = INode.isTreeLevelRandomPartitioned(depth);
      if (dstInode.isDirectory() && dir.hasChildren(dstInode.getId(),areChildrenRandomlyPartitioned)) {
        error =
            "rename cannot overwrite non empty destination directory " + dst;
        NameNode.stateChangeLog
            .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
        throw new IOException(error);
      }
    }
    if (dstInodes[dstInodes.length - 2] == null) {
      error = "rename destination parent " + dst + " not found.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new FileNotFoundException(error);
    }
    if (!dstInodes[dstInodes.length - 2].isDirectory()) {
      error = "rename destination parent " + dst + " is a file.";
      NameNode.stateChangeLog
          .warn("DIR* FSDirectory.unprotectedRenameTo: " + error);
      throw new ParentNotDirectoryException(error);
    }

    INode srcDataSet = getMetaEnabledParent(srcInodes);
    INode dstDataSet = getMetaEnabledParent(dstInodes);
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //--
    //TODO [S]  if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation.
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcCounts.nsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    srcCounts.dsCount = srcInfo.getDsCount();
    INode.DirCounts dstCounts = new INode.DirCounts();
    dstCounts.nsCount = dstInfo.getNsCount();
    dstCounts.dsCount = dstInfo.getDsCount();
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;
    String subTreeLockDst = INode.constructPath(dstInfo.getPathComponents(),
        0, dstInfo.getNumExistingComp());
    if(subTreeLockDst.equals(INodeDirectory.ROOT_NAME)){
      subTreeLockDst = "/"; // absolute path
    }
    try {
      if (isUsingSubTreeLocks) {
        LOG.debug("Rename src: " + src + " dst: " + dst + " requires sub-tree locking mechanism");
        srcSubTreeRoot = lockSubtreeAndCheckPathPermission(src, false, null,
            FsAction.WRITE, null, null, SubTreeOperation.Type.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (shouldLogSubtreeInodes(srcInfo, dstInfo, srcDataSet,
              dstDataSet, srcSubTreeRoot)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(this,
                    srcSubTreeRoot, srcDataSet, dstDataSet);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(this,
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcCounts.nsCount = srcFileTree.getNamespaceCount();
          srcCounts.dsCount = srcFileTree.getDiskspaceCount();

          delayAfterBbuildingTree("Built Tree for "+src1+" for rename. ");
        }
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      renameTo(src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks,
              subTreeLockDst, logEntries, options);

      renameTransactionCommitted = true;
    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          unlockSubtree(src, srcSubTreeRoot.getInodeId());
        }
      }
    }
      success = true;
    } finally {
      retryCacheSetStateTransactional(cacheEntry, success);
    }
  }

  private boolean pathIsMetaEnabled(INode[] pathComponents) {
    return getMetaEnabledParent(pathComponents) == null ? false : true;
  }

  private INode getMetaEnabledParent(INode[] pathComponents) {
    for (INode node : pathComponents) {
      if (node != null && node.isDirectory()) {
        INodeDirectory dir = (INodeDirectory) node;
        if (dir.isMetaEnabled()) {
          return dir;
        }
      }
    }
    return null;
  }

  private void renameTo(final String src1, final int srcINodeID, final String dst1,
                        final INode.DirCounts srcCounts, final INode.DirCounts dstCounts,
                        final boolean isUsingSubTreeLocks, final String subTreeLockDst,
                        final Collection<MetadataLogEntry> logEntries,
                        final Options.Rename... options) throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    pathComponents = FSDirectory.getPathComponentsForReservedPath(dst1);
    final String dst = FSDirectory.resolvePath(dst1, pathComponents, dir);
    new HopsTransactionalRequestHandler(
        isUsingSubTreeLocks?HDFSOperationType.SUBTREE_RENAME:
            HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getRenameINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH, src, dst)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        if (isUsingSubTreeLocks) {
          il.setIgnoredSTOInodes(srcINodeID);
        }
        locks.add(il)
            .add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.IV,
                BLK.PE, BLK.ER));
        if (dir.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src, dst));
        }
        if(!isUsingSubTreeLocks){
          locks.add(lf.getLeaseLock(LockType.WRITE))
              .add(lf.getLeasePathLock(LockType.READ_COMMITTED));
        }else{
          locks.add(lf.getLeaseLock(LockType.WRITE))
              .add(lf.getLeasePathLock(LockType.WRITE, src));
        }
        if (erasureCodingEnabled) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, dst));
        }
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* NameSystem.renameTo: with options - " + src + " to " + dst);
        }

        checkNameNodeSafeMode("Cannot rename " + src);
        if (!DFSUtil.isValidName(dst)) {
          throw new InvalidPathException("Invalid name: " + dst);
        }
        for (MetadataLogEntry logEntry : logEntries) {
          EntityManager.add(logEntry);
        }

        AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime
            (logEntries);

        for (Options.Rename op : options) {
          if (op == Rename.KEEP_ENCODING_STATUS) {
            INodesInPath srcInodesInPath =
                dir.getRootDir().getExistingPathINodes(src, false);
            INode[] srcNodes = srcInodesInPath.getINodes();
            INodesInPath dstInodesInPath =
                dir.getRootDir().getExistingPathINodes(dst, false);
            INode[] dstNodes = dstInodesInPath.getINodes();
            INode srcNode = srcNodes[srcNodes.length - 1];
            INode dstNode = dstNodes[dstNodes.length - 1];
            EncodingStatus status = EntityManager.find(
                EncodingStatus.Finder.ByInodeId, dstNode.getId());
            EncodingStatus newStatus = new EncodingStatus(status);
            newStatus.setInodeId(srcNode.getId(), srcNode.isInTree());
            EntityManager.add(newStatus);
            EntityManager.remove(status);
            break;
          }
        }

        removeSubTreeLocksForRenameInternal(src, isUsingSubTreeLocks,
            subTreeLockDst);

        dir.renameTo(src, dst, srcCounts, dstCounts,
            options);
        return null;
      }
    }.handle(this);
  }

  private void removeSubTreeLocksForRenameInternal(final String src,
      final boolean isUsingSubTreeLocks, final String subTreeLockDst)
      throws StorageException, TransactionContextException,
      UnresolvedLinkException {
    if (isUsingSubTreeLocks) {
      INode[] nodes;
      INode inode;
      if (!src.equals("/")) {
        EntityManager.remove(new SubTreeOperation(getSubTreeLockPathPrefix(src)));
        INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(src, false);
        nodes = inodesInPath.getINodes();
        inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSTOLocked()) {
          inode.setSubtreeLocked(false);
          EntityManager.update(inode);
        }
      }
    }
  }

  @Deprecated
  boolean multiTransactionalRename(final String src, final String dst)
      throws IOException {
    //only for testing
    saveTimes();

    CacheEntry cacheEntry = retryCacheWaitForCompletionTransactional();
    if (cacheEntry != null && cacheEntry.isSuccess()) {
      return true; // Return previous response
    }
    boolean ret = false;
    try{
      ret = renameToInt(src, dst);
    } finally {
      retryCacheSetStateTransactional(cacheEntry, ret);
    }
    return ret;
   }

   private boolean renameToInt(final String src1, final String dst1) throws IOException{
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    pathComponents = FSDirectory.getPathComponentsForReservedPath(dst1);
    final String dst = FSDirectory.resolvePath(dst1, pathComponents, dir);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.multiTransactionalRename: with options - " + src +
              " to " + dst);
    }

    checkNameNodeSafeMode("Cannot rename " + src);
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }

    if (INode.getPathComponents(src).length == 1) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source is the root");
      return false;
    }


    PathInformation srcInfo = getPathExistingINodesFromDB(src,
        false, null, FsAction.WRITE, null, null);
    INode[] srcInodes = srcInfo.getPathInodes();
    INode srcInode = srcInodes[srcInodes.length - 1];
    if(srcInode == null){
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst + " because source does not exist");
      return false;
    }

    PathInformation dstInfo = getPathExistingINodesFromDB(dst,
        false, FsAction.WRITE, null, null, null);
    String actualDst = dst;
    if(dstInfo.isDir()){
      actualDst += Path.SEPARATOR + new Path(src).getName();
    }

    if (actualDst.equals(src)) {
      return true;
    }

    INode[] dstInodes =  dstInfo.getPathInodes();
    if(dstInodes[dstInodes.length-2] == null){
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + dst +
              " because destination's parent does not exist");
      return false;
    }

    if (actualDst.startsWith(src) &&
        actualDst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      NameNode.stateChangeLog.warn(
          "DIR* FSDirectory.unprotectedRenameTo: " + "failed to rename " + src +
              " to " + actualDst + " because destination starts with src");
      return false;
    }

    INode srcDataSet = getMetaEnabledParent(srcInfo.getPathInodes());
    INode dstDataSet = getMetaEnabledParent(dstInfo.getPathInodes());
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;

    //TODO [S]  if src is a file then there is no need for sub tree locking
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation.
    INode.DirCounts srcCounts = new INode.DirCounts();
    srcCounts.nsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    srcCounts.dsCount = srcInfo.getDsCount();
    INode.DirCounts dstCounts = new INode.DirCounts();
    dstCounts.nsCount = dstInfo.getNsCount();
    dstCounts.dsCount = dstInfo.getDsCount();
    boolean isUsingSubTreeLocks = srcInfo.isDir();
    boolean renameTransactionCommitted = false;
    INodeIdentifier srcSubTreeRoot = null;

    String subTreeLockDst = INode.constructPath(dstInfo.getPathComponents(),
        0,  dstInfo.getNumExistingComp());
    if(subTreeLockDst.equals(INodeDirectory.ROOT_NAME)){
      subTreeLockDst = "/"; // absolute path
    }
    try {
      if (isUsingSubTreeLocks) {
        LOG.debug("Rename src: "+src+" dst: "+dst+" requires sub-tree locking mechanism");
        srcSubTreeRoot = lockSubtreeAndCheckPathPermission(src, false, null,
            FsAction.WRITE, null, null, SubTreeOperation.Type.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (shouldLogSubtreeInodes(srcInfo, dstInfo, srcDataSet,
              dstDataSet, srcSubTreeRoot)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(this,
                    srcSubTreeRoot, srcDataSet, dstDataSet);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(this,
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcCounts.nsCount = srcFileTree.getNamespaceCount();
          srcCounts.dsCount = srcFileTree.getDiskspaceCount();
        }
        delayAfterBbuildingTree("Built tree of "+src1+" for rename. ");
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      boolean retValue = renameTo(src, srcSubTreeRoot != null?srcSubTreeRoot.getInodeId():0,
              dst, srcCounts, dstCounts, isUsingSubTreeLocks, subTreeLockDst, logEntries);

      // the rename Tx has committed. it has also remove the subTreeLocks
      renameTransactionCommitted = true;

      return retValue;

    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          unlockSubtree(src, srcSubTreeRoot.getInodeId());
        }
     }
    }
  }

  private boolean shouldLogSubtreeInodes(PathInformation srcInfo,
      PathInformation dstInfo, INode srcDataSet, INode dstDataSet,
      INodeIdentifier srcSubTreeRoot){
    if(pathIsMetaEnabled(srcInfo.pathInodes) || pathIsMetaEnabled(dstInfo
        .pathInodes)){
      boolean isRenameLocalOrRenameDataSet = dstDataSet == null ? srcDataSet
          .equalsIdentifier(srcSubTreeRoot) : srcDataSet.equals(dstDataSet);
      return !isRenameLocalOrRenameDataSet;
    }
    return false;
  }

  /**
   * Change the indicated filename.
   *
   * @deprecated Use {@link #multiTransactionalRename(String, String, Rename...)}}
   * instead.
   */
  @Deprecated
  boolean renameTo(final String src1, final int srcINodeID, final String dst1,
                   final INode.DirCounts srcCounts, final INode.DirCounts dstCounts,
                   final boolean isUsingSubTreeLocks, final String subTreeLockDst,
                   final Collection<MetadataLogEntry> logEntries)
      throws IOException {
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src1);
    final String src = FSDirectory.resolvePath(src1, pathComponents, dir);
    pathComponents = FSDirectory.getPathComponentsForReservedPath(dst1);
    final String dst = FSDirectory.resolvePath(dst1, pathComponents, dir);
    HopsTransactionalRequestHandler renameToHandler =
        new HopsTransactionalRequestHandler(
            isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_DEPRICATED_RENAME :
                HDFSOperationType.DEPRICATED_RENAME
            , src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            INodeLock il = lf.getLegacyRenameINodeLock(
                    INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH, src, dst)
                    .setNameNodeID(nameNode.getId())
                    .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes())
                    .skipReadingQuotaAttr(!dir.isQuotaEnabled());
            if(isUsingSubTreeLocks) {
                    il.setIgnoredSTOInodes(srcINodeID);
            }
            locks.add(il)
                    .add(lf.getBlockLock())
                    .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.IV, BLK.CR, BLK.ER, BLK.PE, BLK.UR));
            if(!isUsingSubTreeLocks){
              locks.add(lf.getLeaseLock(LockType.WRITE))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED));
            }else{
              locks.add(lf.getLeaseLock(LockType.READ_COMMITTED))
                  .add(lf.getLeasePathLock(LockType.READ_COMMITTED,src));
            }
            if (dir.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src, dst));
            }
          }

          @Override
          public Object performTask() throws IOException {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src
                  + " to " + dst);
            }

            checkNameNodeSafeMode("Cannot rename " + src);
            if (!DFSUtil.isValidName(dst)) {
              throw new IOException("Invalid name: " + dst);
            }

            // remove the subtree locks
            removeSubTreeLocksForRenameInternal(src, isUsingSubTreeLocks,
                subTreeLockDst);

            for (MetadataLogEntry logEntry : logEntries) {
              EntityManager.add(logEntry);
            }

            AbstractFileTree.LoggingQuotaCountingFileTree.updateLogicalTime
                (logEntries);

            return dir.renameTo(src, dst, srcCounts, dstCounts);
          }
        };
    return (Boolean) renameToHandler.handle(this);
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
    final String path = FSDirectory.resolvePath(path1, pathComponents, dir);
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
          subtreeRoot = lockSubtreeAndCheckPathPermission(path, false, null,
              FsAction.WRITE, null, null,
              SubTreeOperation.Type.DELETE_STO);

          List<AclEntry> nearestDefaultsForSubtree = calculateNearestDefaultAclForSubtree(pathInfo);
          AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(this, subtreeRoot, FsAction.ALL,
              nearestDefaultsForSubtree);
          fileTree.buildUp();
          delayAfterBbuildingTree("Built tree for "+path1+" for delete op");

          if (dir.isQuotaEnabled()) {
            Iterator<Integer> idIterator = fileTree.getAllINodesIds().iterator();
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

  private boolean deleteTreeLevel(final String subtreeRootPath, final int subTreeRootID,
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

  private Future multiTransactionDeleteInternal(final String path1, final int subTreeRootId)
          throws StorageException, TransactionContextException, IOException {
   byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(path1);
   final String path = FSDirectory.resolvePath(path1, pathComponents, dir);
   return  subtreeOperationsExecutor.submit(new Callable<Boolean>() {
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
                      locks.add(il)
                              .add(lf.getLeaseLock(LockType.WRITE))
                              .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                              .add(lf.getBlockLock()).add(
                              lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE, BLK.IV));
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
  INodeIdentifier lockSubtree(final String path, SubTreeOperation.Type stoType) throws IOException {
    return lockSubtreeAndCheckPathPermission(path, false, null, null, null,
        null, stoType);
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
  private INodeIdentifier lockSubtreeAndCheckPathPermission(final String path,
      final boolean doCheckOwner, final FsAction ancestorAccess,
      final FsAction parentAccess, final FsAction access,
      final FsAction subAccess,
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
        if (isPermissionEnabled && !pc.isSuperUser()) {
          pc.checkPermission(path, dir.getRootDir(), doCheckOwner,
              ancestorAccess, parentAccess, access, subAccess, true);
        }

        INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(path, false);
        INode[] nodes = inodesInPath.getINodes();
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
  private String getSubTreeLockPathPrefix(String path){
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
  void unlockSubtree(final String path, final int ignoreStoInodeId) throws IOException {
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
        INodesInPath inodesInPath = dir.getRootDir().getExistingPathINodes(path, false);
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
  public EncodingStatus getEncodingStatus(final String filePath)
      throws IOException {
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
            try {
              if (isPermissionEnabled) {
                checkPathAccess(pc, filePath, FsAction.READ);
              }
            } catch (AccessControlException e){
              logAuditEvent(false, "getEncodingStatus", filePath);
              throw e;
            }
            INode targetNode = getINode(filePath);
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
  public INode findInode(final int id) throws IOException {
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
  public String getPath(int id, boolean inTree) throws IOException {
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
  public void addEncodingStatus(final String sourcePath,
      final EncodingPolicy policy, final EncodingStatus.Status status, final boolean checkRetryCache)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, sourcePath)
                .setNameNodeID(nameNode.getId())
                .setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getEncodingStatusLock(LockType.WRITE, sourcePath))
                .add(lf.getRetryCacheEntryLock(Server.getClientId(), Server.getCallId()));
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
          try {
            if (isPermissionEnabled) {
              checkPathAccess(pc, sourcePath, FsAction.WRITE);
            }
          } catch (AccessControlException e) {
            logAuditEvent(false, "encodeFile", sourcePath);
            throw e;
          }
          INode target = getINode(sourcePath);
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
  public void revokeEncoding(final String filePath, short replication)
      throws IOException {
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
        try {
          if (isPermissionEnabled) {
            checkPathAccess(pc, filePath, FsAction.WRITE);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "revokeEncoding", filePath);
          throw e;
        }
        INode targetNode = getINode(filePath);
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
  public void updateEncodingStatus(final String sourceFile,
      final EncodingStatus.Status status,
      final EncodingStatus.ParityStatus parityStatus, final String parityFile)
      throws IOException {
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
    INodesInPath inodesInPath = dir.getExistingPathINodes(path);
    INode[] inodes = inodesInPath.getINodes();
    return inodes[inodes.length - 1];
  }

  public boolean isErasureCodingEnabled() {
    return erasureCodingEnabled;
  }

  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#addBlockChecksum
   */
  public void addBlockChecksum(final String src, final int blockIndex,
      final long checksum) throws IOException {
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
        try {
          if (isPermissionEnabled) {
            checkPathAccess(pc, src, FsAction.WRITE);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "addBlockChecksum", src);
          throw e;
        }
        int inodeId = dir.getINode(src).getId();
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
  public long getBlockChecksum(final String src, final int blockIndex)
      throws IOException {
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
        try {
          if (isPermissionEnabled) {
            checkPathAccess(pc, src, FsAction.READ);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "getBlockChecksum", src);
          throw e;
        }
        INode node = dir.getINode(src);
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


  void modifyAclEntries(final String src, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot modify acl entries " + src, safeMode);
    }
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.WRITE);
        }
        dir.modifyAclEntries(src, aclSpec);
        return null;
      }
    }.handle();
  }

  void removeAclEntries(final String src, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove acl entries " + src, safeMode);
    }
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.WRITE);
        }
        dir.removeAclEntries(src, aclSpec);
        return null;
      }
    }.handle();
  }

  void removeDefaultAcl(final String src) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove default acl " + src, safeMode);
    }
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.WRITE);
        }
        dir.removeDefaultAcl(src);
        return null;
      }
    }.handle();
  }

  void removeAcl(final String src) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot remove acl " + src, safeMode);
    }
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.WRITE);
        }
        dir.removeAcl(src);
        return null;
      }
    }.handle();
  }

  void setAcl(final String src, final List<AclEntry> aclSpec) throws IOException {
    aclConfigFlag.checkForApiCall();
    if(isInSafeMode()){
      throw new SafeModeException("Cannot set acl " + src, safeMode);
    }
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.WRITE);
        }
        dir.setAcl(src, aclSpec);
        return null;
      }
    }.handle();

  }

  AclStatus getAclStatus(final String src) throws IOException {
    aclConfigFlag.checkForApiCall();
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
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled) {
          checkPathAccess(pc, src, FsAction.READ);
        }
        return dir.getAclStatus(src);
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

  private PathInformation getPathExistingINodesFromDB(final String path,
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
            if (isPermissionEnabled && !pc.isSuperUser()) {
              pc.checkPermission(path, dir.getRootDir(), doCheckOwner,
                  ancestorAccess, parentAccess, access, subAccess, true);
            }

            byte[][] pathComponents = INode.getPathComponents(path);
            boolean isDir = false;
            INode.DirCounts srcCounts = new INode.DirCounts();
            INodeDirectory.INodesInPath dstInodesInPath = dir.getRootDir().getExistingPathINodes(pathComponents,
                pathComponents.length, false);
            final INode[] pathInodes = dstInodesInPath.getINodes();

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
                pathInodes,dstInodesInPath.getCount(),isDir,
                srcCounts.getNsCount(), srcCounts.getDsCount(), quotaDirAttributes, acls);
          }
        };
    return (PathInformation)handler.handle(this);
  }

  private class PathInformation{
    private String path;
    private byte[][] pathComponents;
    private INode[] pathInodes;
    private boolean dir;
    private long nsCount;
    private long dsCount;
    private int numExistingComp;
    private final INodeAttributes subtreeRootAttributes;
    private final List<AclEntry>[] pathInodeAcls;

    public PathInformation(String path,
        byte[][] pathComponents, INode[] pathInodes, int numExistingComp,
        boolean dir, long nsCount, long dsCount, INodeAttributes subtreeRootAttributes, List<AclEntry>[] pathInodeAcls) {
      this.path = path;
      this.pathComponents = pathComponents;
      this.pathInodes = pathInodes;
      this.dir = dir;
      this.nsCount = nsCount;
      this.dsCount = dsCount;
      this.numExistingComp = numExistingComp;
      this.subtreeRootAttributes = subtreeRootAttributes;
      this.pathInodeAcls = pathInodeAcls;
    }

    public String getPath() {
      return path;
    }

    public byte[][] getPathComponents() {
      return pathComponents;
    }

    public INode[] getPathInodes() {
      return pathInodes;
    }

    public boolean isDir() {
      return dir;
    }

    public long getNsCount() {
      return nsCount;
    }

    public long getDsCount() {
      return dsCount;
    }

    public int getNumExistingComp() {
      return numExistingComp;
    }

    public INodeAttributes getSubtreeRootAttributes() {
      return subtreeRootAttributes;
    }

    public List<AclEntry>[] getPathInodeAcls() { return pathInodeAcls; }
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

  public byte[] getSmallFileData(final int id) throws IOException {
    final int inodeId = -id;
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


  void checkAccess(final String src, final FsAction mode) throws IOException {

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
          }

          @Override
          public Object performTask() throws IOException {
            FSPermissionChecker pc = getPermissionChecker();
            try {
              INode inode = getINode(src);
              if (inode == null) {
                throw new FileNotFoundException("Path not found");
              }
              if (isPermissionEnabled) {
                checkPermission(pc, src, false, null, null, mode, null);
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

  private CacheEntry retryCacheWaitForCompletionTransactional() throws IOException {
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

  private CacheEntry retryCacheSetStateTransactional(final CacheEntry cacheEntry, final boolean ret) throws IOException {
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
    long entryExpiryNanos;
    Timer timer = new Timer();

    public RetryCacheCleaner() {
      long entryExpiryMillis = conf.getLong(
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY,
          DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT);
      entryExpiryNanos = entryExpiryMillis * 1000 * 1000;
    }

    @Override
    public void run() {
      try {
        int numRun = 0;
        while (fsRunning && shouldCacheCleanerRun) {
          final List<CacheEntry> toRemove = new ArrayList<>();
          int num = retryCache.getToRemove().drainTo(toRemove);
          if (num > 0) {
            HopsTransactionalRequestHandler rh = new HopsTransactionalRequestHandler
                    (HDFSOperationType.CLEAN_RETRY_CACHE) {
              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getRetryCacheEntryLock(toRemove));
              }

              @Override
              public Object performTask() throws IOException {
                for (CacheEntry entry : toRemove) {
                  EntityManager.remove(new RetryCacheEntry(entry.getClientId(), entry.getCallId()));
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
                da.removeOlds(timer.monotonicNowNanos() - entryExpiryNanos);
                return null;
              }
            }.handle();
          }

          Thread.sleep(1000);
          numRun++;
        }
      } catch (Exception e) {
        FSNamesystem.LOG.error("Exception in RetryCacheCleaner: ", e);
      }
    }

    public void stopMonitor() {
      shouldCacheCleanerRun = false;
    }
  }

  private List<AclEntry> calculateNearestDefaultAclForSubtree(final PathInformation pathInfo) throws IOException {
    for (int i = pathInfo.pathInodeAcls.length-1; i > -1 ; i--){
      List<AclEntry> aclEntries = pathInfo.pathInodeAcls[i];
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
}

