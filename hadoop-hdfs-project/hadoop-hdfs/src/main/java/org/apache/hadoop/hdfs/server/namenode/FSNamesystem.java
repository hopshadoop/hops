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
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import io.hops.common.IDsGeneratorFactory;
import io.hops.common.IDsMonitor;
import io.hops.common.INodeUtil;
import io.hops.erasure_coding.Codec;
import io.hops.erasure_coding.ErasureCodingManager;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.dal.SizeLogDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.SizeLogEntry;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.resolvingcache.Cache;
import io.hops.security.Users;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.RootINodeCache;
import io.hops.transaction.handler.EncodingStatusOperationType;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
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
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.blockmanagement.MutableBlockCollection;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
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
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.hops.transaction.lock.LockFactory.BLK;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.SubtreeLockedException;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.Time.now;

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

  private boolean isAuditEnabled() {
    return !isDefaultAuditLogger || auditLog.isInfoEnabled();
  }

  private HdfsFileStatus getAuditFileInfo(String path, boolean resolveSymlink)
      throws IOException, StorageException {
    return (isAuditEnabled() && isExternalInvocation()) ?
        dir.getFileInfo(path, resolveSymlink) : null;
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
      HdfsFileStatus stat) {
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
      logger.logAuditEvent(succeeded, ugi.toString(), addr, cmd, src, dst,
          status);
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
  private final boolean persistBlocks;
  private final UserGroupInformation fsOwner;
  private final String fsOwnerShortUserName;
  private final String supergroup;

  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  final DelegationTokenSecretManager dtSecretManager;
  private final boolean alwaysUseDelegationTokensForTests;
  
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

  // Block pool ID used by this namenode
  //HOP mmade it final and now its value is read from the config file. all namenode should have same block pool id
  private final String blockPoolId;


  final LeaseManager leaseManager = new LeaseManager(this);

  Daemon smmthread = null;  // SafeModeMonitor thread
  
  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

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

  // precision of access times.
  private final long accessTimePrecision;

  private NameNode nameNode;
  private final Configuration conf;
  private final QuotaUpdateManager quotaUpdateManager;
  private final boolean legacyDeleteEnabled;
  private final boolean legacyRenameEnabled;
  private final boolean legacyContentSummaryEnabled;
  private final boolean legacySetQuotaEnabled;

  private final ExecutorService subtreeOperationsExecutor;
  private final boolean erasureCodingEnabled;
  private final ErasureCodingManager erasureCodingManager;
  private final long BIGGEST_DELETEABLE_DIR;
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

    FSNamesystem namesystem = new FSNamesystem(conf, namenode);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if (startOpt == StartupOption.RECOVER) {
      namesystem.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    }

    long loadStart = now();

    namesystem.dir
        .imageLoadComplete();     //HOP: this function was called inside the  namesystem.loadFSImage(...) which is commented out

    long timeTakenToLoadFSImage = now() - loadStart;
    LOG.info(
        "Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    NameNodeMetrics nnMetrics = NameNode.getNameNodeMetrics();
    if (nnMetrics != null) {
      nnMetrics.setFsImageLoadTime((int) timeTakenToLoadFSImage);
    }
    return namesystem;
  }

  /**
   * Create an FSNamesystem.
   *
   * @param conf
   *     configuration
   * @param namenode
   *     the namenode
   * @throws IOException
   *      on bad configuration
   */
  FSNamesystem(Configuration conf, NameNode namenode) throws IOException {
    try {
      this.conf = conf;
      this.nameNode = namenode;
      resourceRecheckInterval =
          conf.getLong(DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
              DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

      this.blockManager = new BlockManager(this, this, conf);
      this.erasureCodingEnabled =
          ErasureCodingManager.isErasureCodingEnabled(conf);
      this.erasureCodingManager = new ErasureCodingManager(this, conf);
      this.datanodeStatistics =
          blockManager.getDatanodeManager().getDatanodeStatistics();

      this.fsOwner = UserGroupInformation.getCurrentUser();
      this.fsOwnerShortUserName = fsOwner.getShortUserName();
      this.supergroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
          DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);
      this.isPermissionEnabled = conf.getBoolean(DFS_PERMISSIONS_ENABLED_KEY,
          DFS_PERMISSIONS_ENABLED_DEFAULT);

      blockPoolId = StorageInfo.getStorageInfoFromDB().getBlockPoolId();
      blockManager.setBlockPoolId(blockPoolId);
      hopSpecificInitialization(conf);
      this.quotaUpdateManager = new QuotaUpdateManager(this, conf);
      legacyDeleteEnabled = conf.getBoolean(DFS_LEGACY_DELETE_ENABLE_KEY,
          DFS_LEGACY_DELETE_ENABLE_DEFAULT);
      legacyRenameEnabled = conf.getBoolean(DFS_LEGACY_RENAME_ENABLE_KEY,
          DFS_LEGACY_RENAME_ENABLE_DEFAULT);
      legacyContentSummaryEnabled =
          conf.getBoolean(DFS_LEGACY_CONTENT_SUMMARY_ENABLE_KEY,
              DFS_LEGACY_CONTENT_SUMMARY_ENABLE_DEFAULT);
      legacySetQuotaEnabled = conf.getBoolean(DFS_LEGACY_SET_QUOTA_ENABLE_KEY,
          DFS_LEGACY_SET_QUOTA_ENABLE_DEFAULT);
      subtreeOperationsExecutor = Executors.newFixedThreadPool(
          conf.getInt(DFS_SUBTREE_EXECUTOR_LIMIT_KEY,
              DFS_SUBTREE_EXECUTOR_LIMIT_DEFAULT));
      BIGGEST_DELETEABLE_DIR = conf.getLong(DFS_DIR_DELETE_BATCH_SIZE,
              DFS_DIR_DELETE_BATCH_SIZE_DEFAULT);
      
      LOG.info("fsOwner             = " + fsOwner);
      LOG.info("supergroup          = " + supergroup);
      LOG.info("isPermissionEnabled = " + isPermissionEnabled);

      final boolean persistBlocks =
          conf.getBoolean(DFS_PERSIST_BLOCKS_KEY, DFS_PERSIST_BLOCKS_DEFAULT);
      this.persistBlocks = persistBlocks;

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
          checksumType);
      
      this.maxFsObjects = conf.getLong(DFS_NAMENODE_MAX_OBJECTS_KEY,
          DFS_NAMENODE_MAX_OBJECTS_DEFAULT);

      this.accessTimePrecision =
          conf.getLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 0);
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
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw e;
    } catch (RuntimeException re) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", re);
      close();
      throw re;
    }
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
    return auditLoggers;
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
    if (isClusterInSafeMode()) {
      assert safeMode != null && !safeMode.isPopulatingReplQueues();
      setBlockTotal();
      performPendingSafeModeOperation();
    }
    blockManager.activate(conf);
    RootINodeCache.start();
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
  void stopCommonServices() {
    if (blockManager != null) {
      blockManager.close();
    }
    if (quotaUpdateManager != null) {
      quotaUpdateManager.close();
    }
    RootINodeCache.stop();
  }
  
  /**
   * Start services required in active state
   *
   * @throws IOException
   */
  void startActiveServices() throws IOException {
    LOG.info("Starting services required for active state");
    LOG.info("Catching up to latest edits from old active before " +
        "taking over writer role in edits logs");
    blockManager.getDatanodeManager().markAllDatanodesStale();

    if (isClusterInSafeMode()) {
      if (!isInSafeMode() ||
          (isInSafeMode() && safeMode.isPopulatingReplQueues())) {
        LOG.info("Reprocessing replication and invalidation queues");
        blockManager.processMisReplicatedBlocks();
      }
    }

    leaseManager.startMonitor();
    startSecretManagerIfNecessary();

    //ResourceMonitor required only at ActiveNN. See HDFS-2914
    this.nnrmthread = new Daemon(new NameNodeResourceMonitor());
    nnrmthread.start();

    if (erasureCodingEnabled) {
      erasureCodingManager.activate();
    }
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
    if (erasureCodingManager != null) {
      erasureCodingManager.close();
    }
  }

  NamespaceInfo getNamespaceInfo() throws IOException {
    return unprotectedGetNamespaceInfo();
  }

  /**
   * Version of @see #getNamespaceInfo() that is not protected by a lock.
   */
  NamespaceInfo unprotectedGetNamespaceInfo() throws IOException {

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
  void setPermissionSTO(final String src, final FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {  
    
    boolean txFailed = true;
    INodeIdentifier inode = null;
    try {
      inode = lockSubtreeAndCheckPathPermission(src,
            true, null, null, null, null, SubTreeOperation.StoOperationType.SET_PERMISSION_STO);
      final boolean isSto = inode != null;
      new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_SETPERMISSION, src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode,
              INodeLockType.WRITE, INodeResolveType.PATH,false, true, src)).add(lf.getBlockLock());
        }

        @Override
        public Object performTask() throws StorageException, IOException {
          try {
            setPermissionSTOInt(src, permission, isSto);
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
        unlockSubtree(src);
        }
      }
    }
    
  }
  
  private void setPermissionSTOInt(String src, FsPermission permission,boolean isSTO)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException, StorageException {
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set permission for " + src, safeMode);
    }
    checkOwner(pc, src);
    dir.setPermission(src, permission);
    resultingStat = getAuditFileInfo(src, false);
    logAuditEvent(true, "setPermission", src, null, resultingStat);
    
    //remove sto from 
    if(isSTO){
    INode[] nodes = dir.getRootDir().getExistingPathINodes(src, false);
        INode inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSubtreeLocked()) {
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
  void setPermission(final String src, final FsPermission permission)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_PERMISSION, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, src)).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
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
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException, StorageException {
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set permission for " + src, safeMode);
    }
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
  void setOwnerSTO(final String src, final String username, final String group)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {

    boolean txFailed = true;
    INodeIdentifier inode = null;;
    try{
    inode = lockSubtreeAndCheckPathPermission(src,
            true, null, null, null, null, SubTreeOperation.StoOperationType.SET_OWNER_STO);
    final boolean isSto = inode != null;
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_OWNER_SUBTREE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/,nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, false, true, src)).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        try {
          setOwnerSTOInt(src, username, group,isSto);
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
          unlockSubtree(src);
        }
      }
    }
  }
  
  private void setOwnerSTOInt(String src, String username, String group, boolean isSTO)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException, StorageException {
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set owner for " + src, safeMode);
    }
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
    INode[] nodes = dir.getRootDir().getExistingPathINodes(src, false);
        INode inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSubtreeLocked()) {
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
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_OWNER, src) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, src)).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
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
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException, StorageException {
    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set owner for " + src, safeMode);
    }
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
  LocatedBlocks getBlockLocations(final String clientMachine, final String src,
      final long offset, final long length) throws IOException {
    HopsTransactionalRequestHandler getBlockLocationsHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_BLOCK_LOCATIONS, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false,nameNode, INodeLockType.WRITE,
                INodeResolveType.PATH, src)).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            LocatedBlocks blocks =
                getBlockLocationsInternal(src, offset, length, true, true,
                    true);
            if (blocks != null) {
              blockManager.getDatanodeManager()
                  .sortLocatedBlocks(clientMachine, blocks.getLocatedBlocks());

              LocatedBlock lastBlock = blocks.getLastLocatedBlock();
              if (lastBlock != null) {
                ArrayList<LocatedBlock> lastBlockList =
                    new ArrayList<LocatedBlock>();
                lastBlockList.add(lastBlock);
                blockManager.getDatanodeManager()
                    .sortLocatedBlocks(clientMachine, lastBlockList);
              }
            }
            return blocks;
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
  public LocatedBlocks getBlockLocations(final String src, final long offset,
      final long length, final boolean doAccessTime,
      final boolean needBlockToken, final boolean checkSafeMode)
      throws IOException {
    HopsTransactionalRequestHandler getBlockLocationsHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_BLOCK_LOCATIONS, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(nameNode, INodeLockType.READ,
                INodeResolveType.PATH, src)).add(lf.getBlockLock())
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
  LocatedBlocks getBlockLocationsInternal(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
      throws FileNotFoundException, UnresolvedLinkException, IOException,
      StorageException {
    FSPermissionChecker pc = getPermissionChecker();
    try {
      return getBlockLocationsInt(pc, src, offset, length, doAccessTime,
          needBlockToken, checkSafeMode);
    } catch (AccessControlException e) {
      logAuditEvent(false, "open", src);
      throw e;
    }
  }

  public boolean isFileCorrupt(final String filePath) throws IOException {
    LocatedBlocks blocks =
        getBlockLocationsInternal(filePath, 0, Long.MAX_VALUE, true, true,
            true);
    for (LocatedBlock b : blocks.getLocatedBlocks()) {
      if (b.isCorrupt() ||
          (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
        return true;
      }
    }
    return false;
  }

  private LocatedBlocks getBlockLocationsInt(FSPermissionChecker pc, String src,
      long offset, long length, boolean doAccessTime, boolean needBlockToken,
      boolean checkSafeMode)
      throws FileNotFoundException, UnresolvedLinkException, IOException,
      StorageException {
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
    final LocatedBlocks ret =
        getBlockLocationsUpdateTimes(src, offset, length, doAccessTime,
            needBlockToken);
    logAuditEvent(true, "open", src);
    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.getLocatedBlocks()) {
        // if safemode & no block locations yet then throw safemodeException
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          throw new SafeModeException("Zero blocklocations for " + src,
              safeMode);
        }
      }
    }
    return ret;
  }

  /*
   * Get block locations within the specified range, updating the
   * access times if necessary. 
   */
  private LocatedBlocks getBlockLocationsUpdateTimes(String src, long offset,
      long length, boolean doAccessTime, boolean needBlockToken)
      throws FileNotFoundException, UnresolvedLinkException, IOException,
      StorageException {

    for (int attempt = 0; attempt < 2; attempt++) {
      // if the namenode is in safemode, then do not update access time
      if (isInSafeMode()) {
        doAccessTime = false;
      }

      long now = now();
      final INodeFile inode = INodeFile.valueOf(dir.getINode(src), src);
      if (doAccessTime && isAccessTimeSupported()) {
        if (now <= inode.getAccessTime() + getAccessTimePrecision()) {
          // if we have to set access time but we only have the readlock, then
          // restart this entire operation with the writeLock.
          if (attempt == 0) {
            continue;
          }
        }
        dir.setTimes(src, inode, -1, now, false);
      }
      return blockManager
          .createLocatedBlocks(inode.getBlocks(), inode.computeFileSize(false),
              inode.isUnderConstruction(), offset, length, needBlockToken);
    }
    return null; // can never reach here
  }

  /**
   * Moves all the blocks from srcs and appends them to trg
   * To avoid rollbacks we will verify validitity of ALL of the args
   * before we start actual move.
   *
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
        locks.add(
            lf.getINodeLock(nameNode, INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH, paths)).add(lf.getBlockLock()).add(
            lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.PE, BLK.UC, BLK.IV));
        if (erasureCodingEnabled) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, srcs));
        }
      }

      @Override
      public Object performTask() throws IOException {
        try {
          concatInt(target, srcs);
        } catch (AccessControlException e) {
          logAuditEvent(false, "concat", Arrays.toString(srcs), target, null);
          throw e;
        }
        return null;
      }
    }.handle(this);
  }

  private void concatInt(String target, String[] srcs)
      throws IOException, UnresolvedLinkException, StorageException {
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

    HdfsFileStatus resultingStat = null;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot concat " + target, safeMode);
    }
    concatInternal(pc, target, srcs);
    resultingStat = getAuditFileInfo(target, false);
    logAuditEvent(true, "concat", Arrays.toString(srcs), target, resultingStat);
  }

  /**
   * See {@link #concat(String, String[])}
   */
  private void concatInternal(FSPermissionChecker pc, String target,
      String[] srcs)
      throws IOException, UnresolvedLinkException, StorageException {

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
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks

    // check the target
    final INodeFile trgInode = INodeFile.valueOf(dir.getINode(target), target);
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
    short repl = trgInode.getBlockReplication();

    // now check the srcs
    boolean endSrc =
        false; // final src file doesn't have to have full end block
    for (int i = 0; i < srcs.length; i++) {
      String src = srcs[i];
      if (i == srcs.length - 1) {
        endSrc = true;
      }

      final INodeFile srcInode = INodeFile.valueOf(dir.getINode(src), src);
      if (src.isEmpty() || srcInode.isUnderConstruction() ||
          srcInode.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src +
            " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if (repl != srcInode.getBlockReplication()) {
        throw new HadoopIllegalArgumentException(
            "concat: the soruce file " + src + " and the target file " +
                target +
                " should have the same replication: source replication is " +
                srcInode.getBlockReplication() + " but target replication is " +
                repl);
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
            "concat: the soruce file " + src + " and the target file " +
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
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  void setTimes(final String src, final long mtime, final long atime)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.SET_TIMES, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, src)).add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
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
      throws IOException, UnresolvedLinkException, StorageException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
          " Please set " + DFS_NAMENODE_ACCESSTIME_PRECISION_KEY +
          " configuration parameter.");
    }
    HdfsFileStatus resultingStat = null;
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
  void createSymlink(final String target, final String link,
      final PermissionStatus dirPerms, final boolean createParent)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CREATE_SYM_LINK,
        link) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, false, link));
      }

      @Override
      public Object performTask() throws IOException {
        try {
          createSymlinkInt(target, link, dirPerms, createParent);
        } catch (AccessControlException e) {
          logAuditEvent(false, "createSymlink", link, target, null);
          throw e;
        }
        return null;
      }
    }.handle(this);
  }

  private void createSymlinkInt(String target, String link,
      PermissionStatus dirPerms, boolean createParent)
      throws IOException, UnresolvedLinkException, StorageException {
    HdfsFileStatus resultingStat = null;
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
      throws IOException, UnresolvedLinkException, StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.createSymlink: target=" +
          target + " link=" + link);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create symlink " + link, safeMode);
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
   * @param src
   *     file name
   * @param replication
   *     new replication
   * @return true if successful;
   * false if file does not exist or is a directory
   * @see ClientProtocol#setReplication(String, short)
   */
  boolean setReplication(final String src, final short replication)
      throws IOException {
    HopsTransactionalRequestHandler setReplicationHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.SET_REPLICATION,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/,nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH,
                src)).add(lf.getBlockLock()).add(
                lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC, BLK.UR,
                    BLK.IV));
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
      throws IOException, StorageException {
    blockManager.verifyReplication(src, replication, null);
    final boolean isFile;
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set replication for " + src,
          safeMode);
    }
    if (isPermissionEnabled) {
      checkPathAccess(pc, src, FsAction.WRITE);
    }

    final short[] oldReplication = new short[1];
    final Block[] blocks = dir.setReplication(src, replication, oldReplication);
    isFile = blocks != null;
    if (isFile) {
      blockManager.setReplication(oldReplication[0], replication, src, blocks);
    }

    if (isFile) {
      logAuditEvent(true, "setReplication", src);
    }
    return isFile;
  }
  
    void setMetaEnabled(final String src, final boolean metaEnabled)
      throws IOException {
    try {
      INodeIdentifier inode = lockSubtree(src, SubTreeOperation.StoOperationType.META_ENABLE);
      final AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(
          FSNamesystem.this, inode);
      fileTree.buildUp();
      new HopsTransactionalRequestHandler(HDFSOperationType.SET_META_ENABLED,
          src) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
              INodeResolveType.PATH, true, true, src));
        }

        @Override
        public Object performTask() throws IOException {
          try {
            logMetadataEvents(fileTree, MetadataLogEntry.Operation.ADD);
            setMetaEnabledInt(src, metaEnabled);
          } catch (AccessControlException e) {
            logAuditEvent(false, "setMetaEnabled", src);
            throw e;
          }
          return null;
        }
      }.handle(this);
    } finally {
      unlockSubtree(src);
    }
  }

  private void setMetaEnabledInt(final String src, final boolean metaEnabled)
      throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set metaEnabled for " + src,
          safeMode);
    }
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
      MetadataLogEntry.Operation operation) throws TransactionContextException,
      StorageException {
    ProjectedINode datasetDir = fileTree.getSubtreeRoot();
    for (ProjectedINode node : fileTree.getAllChildren()) {
      MetadataLogEntry logEntry = new MetadataLogEntry(datasetDir.getId(),
          node.getId(), node.getParentId(), node.getName(), operation);
      EntityManager.add(logEntry);
    }
  }

  long getPreferredBlockSize(final String filename) throws IOException {
    HopsTransactionalRequestHandler getPreferredBlockSizeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_PREFERRED_BLOCK_SIZE, filename) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getINodeLock(nameNode, INodeLockType.READ_COMMITTED,
                INodeResolveType.PATH, filename));
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
   * {@link ClientProtocol#create}
   */
  HdfsFileStatus startFile(final String src, final PermissionStatus permissions,
      final String holder, final String clientMachine,
      final EnumSet<CreateFlag> flag, final boolean createParent,
      final short replication, final long blockSize) throws IOException {
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(
        HDFSOperationType.START_FILE, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
                //if quota is disabled then do not read the INode Attributes table
            lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/,nameNode, INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH, false, src)).add(lf.getBlockLock())
            .add(lf.getLeaseLock(LockType.WRITE, holder))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(
            lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR, BLK.PE,
                BLK.IV));

        if (flag.contains(CreateFlag.OVERWRITE) && dir.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(src));
        }
        if (flag.contains(CreateFlag.OVERWRITE) && erasureCodingEnabled) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
        }
      }

      @Override
      public Object performTask() throws IOException {
        try {
          return startFileInt(src, permissions, holder, clientMachine, flag,
              createParent, replication, blockSize);
        } catch (AccessControlException e) {
          logAuditEvent(false, "create", src);
          throw e;
        }
      }
    }.handle(this);
  }

  private HdfsFileStatus startFileInt(String src, PermissionStatus permissions,
      String holder, String clientMachine, EnumSet<CreateFlag> flag,
      boolean createParent, short replication, long blockSize)
      throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, UnresolvedLinkException,
      FileNotFoundException, ParentNotDirectoryException, IOException,
      StorageException {
    FSPermissionChecker pc = getPermissionChecker();
    startFileInternal(pc, src, permissions, holder, clientMachine, flag,
        createParent, replication, blockSize);
    final HdfsFileStatus stat = dir.getFileInfoForCreate(src, false);
    logAuditEvent(true, "create", src, null,
        (isAuditEnabled() && isExternalInvocation()) ? stat : null);
    return stat;
  }

  /**
   * Create new or open an existing file for append.<p>
   * <p/>
   * In case of opening the file for append, the method returns the last
   * block of the file if this is a partial block, which can still be used
   * for writing more data. The client uses the returned block locations
   * to form the data pipeline for this block.<br>
   * The method returns null if the last block is full or if this is a
   * new file. The client then allocates a new block with the next call
   * using {@link NameNodeRpcServer#addBlock}.<p>
   * <p/>
   * For description of parameters and exceptions thrown see
   * {@link ClientProtocol#create}
   *
   * @return the last block locations if the block is partial or null otherwise
   */
  private LocatedBlock startFileInternal(FSPermissionChecker pc, String src,
      PermissionStatus permissions, String holder, String clientMachine,
      EnumSet<CreateFlag> flag, boolean createParent, short replication,
      long blockSize) throws SafeModeException, FileAlreadyExistsException,
      AccessControlException, UnresolvedLinkException, FileNotFoundException,
      ParentNotDirectoryException, IOException, StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.startFile: src=" + src + ", holder=" + holder +
              ", clientMachine=" + clientMachine + ", createParent=" +
              createParent + ", replication=" + replication + ", createFlag=" +
              flag.toString());
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create file" + src, safeMode);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }

    // Verify that the destination does not exist as a directory already.
    boolean pathExists = dir.exists(src);
    if (pathExists && dir.isDir(src)) {
      throw new FileAlreadyExistsException(
          "Cannot create file " + src + "; already exists as a directory.");
    }

    boolean overwrite = flag.contains(CreateFlag.OVERWRITE);
    boolean append = flag.contains(CreateFlag.APPEND);
    if (isPermissionEnabled) {
      if (append || (overwrite && pathExists)) {
        checkPathAccess(pc, src, FsAction.WRITE);
      } else {
        checkAncestorAccess(pc, src, FsAction.WRITE);
      }
    }

    if (!createParent) {
      verifyParentDir(src);
    }

    try {
      blockManager.verifyReplication(src, replication, clientMachine);
      boolean create = flag.contains(CreateFlag.CREATE);
      final INode myFile = dir.getINode(src);
      if (myFile == null) {
        if (!create) {
          throw new FileNotFoundException(
              "failed to overwrite or append to non-existent file " + src +
                  " on client " + clientMachine);
        }
      } else {
        // File exists - must be one of append or overwrite
        if (overwrite) {
          delete(src, true);
        } else {
          // Opening an existing file for write - may need to recover lease.
          recoverLeaseInternal(myFile, src, holder, clientMachine, false);

          if (!append) {
            throw new FileAlreadyExistsException(
                "failed to create file " + src + " on client " + clientMachine +
                    " because the file exists");
          }
        }
      }

      final DatanodeDescriptor clientNode =
          blockManager.getDatanodeManager().getDatanodeByHost(clientMachine);

      if (append && myFile != null) {
        final INodeFile f = INodeFile.valueOf(myFile, src);
        return prepareFileForWrite(src, f, holder, clientMachine, clientNode);
      } else {
        // Now we can add the name to the filesystem. This file has no
        // blocks associated with it.
        //
        checkFsObjectLimit();

        // increment global generation stamp
        //HOP[M] generationstamp is not used for inodes
        long genstamp = 0;
        INodeFileUnderConstruction newNode =
            dir.addFile(src, permissions, replication, blockSize, holder,
                clientMachine, clientNode, genstamp);
        if (newNode == null) {
          throw new IOException("DIR* NameSystem.startFile: " +
              "Unable to add file to namespace.");
        }
        leaseManager.addLease(newNode.getClientName(), src);

        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* NameSystem.startFile: " + "add " + src +
                  " to namespace for " + holder);
        }
      }
    } catch (IOException ie) {
      NameNode.stateChangeLog
          .warn("DIR* NameSystem.startFile: " + ie.getMessage());
      throw ie;
    }
    return null;
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
  LocatedBlock prepareFileForWrite(String src, INodeFile file,
      String leaseHolder, String clientMachine, DatanodeDescriptor clientNode)
      throws IOException, StorageException {
    INodeFileUnderConstruction cons =
        file.convertToUnderConstruction(leaseHolder, clientMachine, clientNode);
    leaseManager.addLease(cons.getClientName(), src);
    LocatedBlock ret = blockManager.convertLastBlockToUnderConstruction(cons);
    return ret;
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   *
   * @param src
   *     the path of the file to start lease recovery
   * @param holder
   *     the lease holder's name
   * @param clientMachine
   *     the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(final String src, final String holder,
      final String clientMachine) throws IOException {
    HopsTransactionalRequestHandler recoverLeaseHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.RECOVER_LEASE,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
                    INodeResolveType.PATH, src))
                .add(lf.getLeaseLock(LockType.WRITE, holder))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                .add(
                    lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            FSPermissionChecker pc = getPermissionChecker();
            if (isInSafeMode()) {
              throw new SafeModeException("Cannot recover the lease of " + src,
                  safeMode);
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

  private void recoverLeaseInternal(INode fileInode, String src, String holder,
      String clientMachine, boolean force)
      throws IOException, StorageException {
    if (fileInode != null && fileInode.isUnderConstruction()) {
      INodeFileUnderConstruction pendingFile =
          (INodeFileUnderConstruction) fileInode;
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
      lease = leaseManager.getLease(pendingFile.getClientName());
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
            " from client " + pendingFile.getClientName());
        internalReleaseLease(lease, src, holder);
      } else {
        assert lease.getHolder().equals(pendingFile.getClientName()) :
            "Current lease holder " + lease.getHolder() +
                " does not match file creator " + pendingFile.getClientName();
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (leaseManager.expiredSoftLimit(lease)) {
          LOG.info("startFile: recover " + lease + ", src=" + src + " client " +
              pendingFile.getClientName());
          boolean isClosed = internalReleaseLease(lease, src, null);
          if (!isClosed) {
            throw new RecoveryInProgressException.NonAbortingRecoveryInProgressException(
                "Failed to close file " + src +
                    ". Lease recovery is in progress. Try again later.");
          }
        } else {
          final BlockInfo lastBlock = pendingFile.getLastBlock();
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
                    pendingFile.getClientName() + "] on [" +
                    pendingFile.getClientMachine() + "]");
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
    HopsTransactionalRequestHandler appendFileHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.APPEND_FILE,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false,nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH,
                src)).add(lf.getBlockLock())
                .add(lf.getLeaseLock(LockType.WRITE, holder))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(
                lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR,
                    BLK.IV, BLK.PE));
            // Always needs to be read. Erasure coding might have been
            // enabled earlier and we don't want to end up in an inconsistent
            // state.
            locks.add(lf.getEncodingStatusLock(LockType.READ_COMMITTED, src));
          }


          @Override
          public Object performTask() throws IOException {
            try {
              INode target = getINode(src);
              if (target != null) {
                EncodingStatus status = EntityManager.find(
                    EncodingStatus.Finder.ByInodeId, target.getId());
                if (status != null) {
                  throw new IOException("Cannot append to erasure-coded file");
                }
              }
              return appendFileInt(src, holder, clientMachine);
            } catch (AccessControlException e) {
              logAuditEvent(false, "append", src);
              throw e;
            }
          }
        };
    return (LocatedBlock) appendFileHandler.handle(this);
  }

  private LocatedBlock appendFileInt(String src, String holder,
      String clientMachine) throws AccessControlException, SafeModeException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, IOException, UnresolvedLinkException,
      StorageException {
    if (!supportAppends) {
      throw new UnsupportedOperationException(
          "Append is not enabled on this NameNode. Use the " +
              DFS_SUPPORT_APPEND_KEY + " configuration option to enable it.");
    }
    LocatedBlock lb = null;
    FSPermissionChecker pc = getPermissionChecker();
    lb = startFileInternal(pc, src, null, holder, clientMachine,
        EnumSet.of(CreateFlag.APPEND), false, blockManager.maxReplication, 0);
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

  ExtendedBlock getExtendedBlock(Block blk) {
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
  LocatedBlock getAdditionalBlock(final String src, final String clientName,
      final ExtendedBlock previous, final HashMap<Node, Node> excludedNodes)
      throws IOException {
    HopsTransactionalRequestHandler additionalBlockHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_BLOCK, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
                    INodeResolveType.PATH, src))
                .add(lf.getLeaseLock(LockType.READ, clientName))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC));
          }

          @Override
          public Object performTask() throws IOException {
            long blockSize;
            int replication;
            DatanodeDescriptor clientNode = null;

            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.getAdditionalBlock: file " + src +
                      " for " + clientName);
            }

            // Part I. Analyze the state of the file with respect to the input data.
            LocatedBlock[] onRetryBlock = new LocatedBlock[1];
            final INode[] inodes =
                analyzeFileState(src, clientName, previous, onRetryBlock);
            final INodeFileUnderConstruction pendingFile =
                (INodeFileUnderConstruction) inodes[inodes.length - 1];

            if (onRetryBlock[0] != null) {
              // This is a retry. Just return the last block.
              return onRetryBlock[0];
            }

            blockSize = pendingFile.getPreferredBlockSize();
            //clientNode = pendingFile.getClientNode(); HOP
            clientNode = pendingFile.getClientNode() == null ? null :
                getBlockManager().getDatanodeManager()
                    .getDatanode(pendingFile.getClientNode());
            replication = pendingFile.getBlockReplication();


            // choose targets for the new block to be allocated.
            final DatanodeDescriptor targets[] = getBlockManager()
                .chooseTarget(src, replication, clientNode, excludedNodes,
                    blockSize);

            // Part II.
            // Allocate a new block, add it to the INode and the BlocksMap.
            Block newBlock = null;
            long offset;
            // Run the full analysis again, since things could have changed
            // while chooseTarget() was executing.
            LocatedBlock[] onRetryBlock2 = new LocatedBlock[1];
            INode[] inodes2 =
                analyzeFileState(src, clientName, previous, onRetryBlock2);
            final INodeFileUnderConstruction pendingFile2 =
                (INodeFileUnderConstruction) inodes2[inodes2.length - 1];

            if (onRetryBlock2[0] != null) {
              // This is a retry. Just return the last block.
              return onRetryBlock2[0];
            }

            // commit the last block and complete it if it has minimum replicas
            commitOrCompleteLastBlock(pendingFile2,
                ExtendedBlock.getLocalBlock(previous));

            // allocate new block, record block locations in INode.
            newBlock = createNewBlock(pendingFile2);
            saveAllocatedBlock(src, inodes2, newBlock, targets);


            dir.persistBlocks(src, pendingFile2);
            offset = pendingFile2.computeFileSize(true);

            // Return located block
            return makeLocatedBlock(newBlock, targets, offset);
          }
        };
    return (LocatedBlock) additionalBlockHandler.handle(this);
  }

  INode[] analyzeFileState(String src, String clientName,
      ExtendedBlock previous, LocatedBlock[] onRetryBlock)
      throws IOException, LeaseExpiredException, StorageException {

    checkBlock(previous);
    onRetryBlock[0] = null;
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot add block to " + src, safeMode);
    }

    // have we exceeded the configured limit of fs objects.
    checkFsObjectLimit();

    Block previousBlock = ExtendedBlock.getLocalBlock(previous);
    final INode[] inodes = dir.getRootDir().getExistingPathINodes(src, true);
    final INodeFileUnderConstruction pendingFile =
        checkLease(src, clientName, inodes[inodes.length - 1]);
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
            ((BlockInfoUnderConstruction) lastBlockInFile)
                .getExpectedLocations(getBlockManager().getDatanodeManager()),
            offset);
        return inodes;
      } else {
        // Case 3
        throw new IOException("Cannot allocate block in " + src + ": " +
            "passed 'previous' block " + previous + " does not match actual " +
            "last block in file " + lastBlockInFile);
      }
    }

    // Check if the penultimate block is minimally replicated
    if (!checkFileProgress(pendingFile, false)) {
      throw new NotReplicatedYetException("Not replicated yet: " + src);
    }
    return inodes;
  }

  LocatedBlock makeLocatedBlock(Block blk, DatanodeInfo[] locs, long offset)
      throws IOException {
    LocatedBlock lBlk = new LocatedBlock(getExtendedBlock(blk), locs, offset);
    getBlockManager()
        .setBlockToken(lBlk, BlockTokenSecretManager.AccessMode.WRITE);
    return lBlk;
  }

  /**
   * @see NameNodeRpcServer#getAdditionalDatanode(String, ExtendedBlock,
   * DatanodeInfo[],
   * DatanodeInfo[], int, String)
   */
  LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final HashMap<Node, Node> excludes,
      final int numAdditionalNodes, final String clientName)
      throws IOException {
    HopsTransactionalRequestHandler getAdditionalDatanodeHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_ADDITIONAL_DATANODE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(nameNode, INodeLockType.READ,
                INodeResolveType.PATH, src))
                .add(lf.getLeaseLock(LockType.READ, clientName));
          }

          @Override
          public Object performTask() throws IOException {
            //check if the feature is enabled
            dtpReplaceDatanodeOnFailure.checkEnabled();

            final DatanodeDescriptor clientnode;
            final long preferredblocksize;
            final List<DatanodeDescriptor> chosen;
            //check safe mode
            if (isInSafeMode()) {
              throw new SafeModeException(
                  "Cannot add datanode; src=" + src + ", blk=" + blk, safeMode);
            }

            //check lease
            final INodeFileUnderConstruction file = checkLease(src, clientName);
            //clientnode = file.getClientNode(); HOP
            clientnode = getBlockManager().getDatanodeManager()
                .getDatanode(file.getClientNode());
            preferredblocksize = file.getPreferredBlockSize();

            //find datanode descriptors
            chosen = new ArrayList<DatanodeDescriptor>();
            for (DatanodeInfo d : existings) {
              final DatanodeDescriptor descriptor =
                  blockManager.getDatanodeManager().getDatanode(d);
              if (descriptor != null) {
                chosen.add(descriptor);
              }
            }

            // choose new datanodes.
            final DatanodeInfo[] targets =
                blockManager.getBlockPlacementPolicy()
                    .chooseTarget(src, numAdditionalNodes, clientnode, chosen,
                        true, excludes, preferredblocksize);
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
  boolean abandonBlock(final ExtendedBlock b, final String src,
      final String holder) throws IOException {
    HopsTransactionalRequestHandler abandonBlockHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.ABANDON_BLOCK,
            src) {

          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH,
                src)).add(lf.getLeaseLock(LockType.READ)).add(lf.getBlockLock())
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
            if (isInSafeMode()) {
              throw new SafeModeException(
                  "Cannot abandon block " + b + " for fle" + src, safeMode);
            }
            INodeFileUnderConstruction file = checkLease(src, holder);
            dir.removeBlock(src, file, ExtendedBlock.getLocalBlock(b));
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug(
                  "BLOCK* NameSystem.abandonBlock: " + b +
                      " is removed from pendingCreates");
            }
            dir.persistBlocks(src, file);

            return true;
          }
        };
    return (Boolean) abandonBlockHandler.handle(this);
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder)
      throws LeaseExpiredException, UnresolvedLinkException, StorageException,
      TransactionContextException {
    return checkLease(src, holder, dir.getINode(src));
  }

  private INodeFileUnderConstruction checkLease(String src, String holder,
      INode file) throws LeaseExpiredException, StorageException,
      TransactionContextException {
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
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException(
          "Lease mismatch on " + src + " owned by " +
              pendingFile.getClientName() + " but is accessed by " + holder);
    }
    return pendingFile;
  }

  /**
   * Complete in-progress write to the given file.
   *
   * @return true if successful, false if the client should continue to retry
   * (e.g if not all blocks have reached minimum replication yet)
   * @throws IOException
   *     on error (eg lease mismatch, file not open, file deleted)
   */
  boolean completeFile(final String src, final String holder,
      final ExtendedBlock last) throws IOException {
    HopsTransactionalRequestHandler completeFileHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.COMPLETE_FILE,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip Inode Atrr*/,nameNode, INodeLockType.WRITE,
                INodeResolveType.PATH, src))
                .add(lf.getLeaseLock(LockType.WRITE, holder))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR,
                    BLK.IV));
          }

          @Override
          public Object performTask() throws IOException {
            checkBlock(last);
            return completeFileInternal(src, holder,
                ExtendedBlock.getLocalBlock(last));
          }
        };
    return (Boolean) completeFileHandler.handle(this);
  }

  private boolean completeFileInternal(String src, String holder, Block last)
      throws SafeModeException, UnresolvedLinkException, IOException,
      StorageException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " +
          src + " for " + holder);
    }
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot complete file " + src, safeMode);
    }

    INodeFileUnderConstruction pendingFile;
    try {
      pendingFile = checkLease(src, holder);
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

  /**
   * Save allocated block at the given pending filename
   *
   * @param src
   *     path to the file
   * @param inodes
   *     representing each of the components of src.
   *     The last INode is the INode for the file.
   * @throws QuotaExceededException
   *     If addition of block exceeds space quota
   */
  BlockInfo saveAllocatedBlock(String src, INode[] inodes, Block newBlock,
      DatanodeDescriptor targets[]) throws IOException, StorageException {
    BlockInfo b = dir.addBlock(src, inodes, newBlock, targets);
    NameNode.stateChangeLog.info(
        "BLOCK* allocateBlock: " + src + ". " + getBlockPoolId() + " " + b);
    for (DatanodeDescriptor dn : targets) {
      dn.incBlocksScheduled();
    }
    return b;
  }

  /**
   * Create new block with a unique block id and a new generation stamp.
   */
  Block createNewBlock(INodeFile pendingFile)
      throws IOException, StorageException {
    Block b = new Block(IDsGeneratorFactory.getInstance().getUniqueBlockID()
        , 0, 0); // HOP. previous code was getFSImage().getUniqueBlockId()
    // Increment the generation stamp for every new block.
    b.setGenerationStampNoPersistance(pendingFile.nextGenerationStamp());
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  boolean checkFileProgress(INodeFile v, boolean checkall)
      throws StorageException, IOException {
    if (checkall) {
      //
      // check all blocks of the file.
      //
      for (BlockInfo block : v.getBlocks()) {
        if (!block.isComplete()) {
          BlockInfo cBlock = blockManager
              .tryToCompleteBlock((MutableBlockCollection) v,
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
            .tryToCompleteBlock((MutableBlockCollection) v, b.getBlockIndex());
        b = v.getPenultimateBlock();
        if (!b.isComplete()) {
          LOG.info("BLOCK* checkFileProgress: " + b +
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
   * Change the indicated filename.
   *
   * @deprecated Use {@link #renameTo(String, String, Options.Rename...)}
   * instead.
   */
  @Deprecated
  boolean renameTo(final String src, final String dst) throws IOException {
    HopsTransactionalRequestHandler renameToHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.DEPRICATED_RENAME, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getLegacyRenameINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH, src, dst))
                .add(lf.getLeaseLock(LockType.WRITE))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.IV, BLK.CR, BLK.ER,
                    BLK.PE, BLK.UR));
            if (dir.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src, dst));
            }
          }

          @Override
          public Object performTask() throws IOException {
            try {
              return renameToInt(src, dst);
            } catch (AccessControlException e) {
              logAuditEvent(false, "rename", src, dst, null);
              throw e;
            }
          }
        };
    return (Boolean) renameToHandler.handle(this);
  }

  private boolean renameToInt(String src, String dst)
      throws IOException, UnresolvedLinkException, StorageException {
    boolean status = false;
    HdfsFileStatus resultingStat = null;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src +
          " to " + dst);
    }
    FSPermissionChecker pc = getPermissionChecker();
    status = renameToInternal(pc, src, dst);
    if (status) {
      resultingStat = getAuditFileInfo(dst, false);
    }

    if (status) {
      logAuditEvent(true, "rename", src, dst, resultingStat);
    }
    return status;
  }

  /**
   * @deprecated See {@link #renameTo(String, String)}
   */
  @Deprecated
  private boolean renameToInternal(FSPermissionChecker pc, String src,
      String dst)
      throws IOException, UnresolvedLinkException, StorageException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new IOException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      //We should not be doing this.  This is move() not renameTo().
      //but for now,
      //NOTE: yes, this is bad!  it's assuming much lower level behavior
      //      of rewriting the dst
      String actualdst =
          dir.isDir(dst) ? dst + Path.SEPARATOR + new Path(src).getName() : dst;
      checkParentAccess(pc, src, FsAction.WRITE);
      checkAncestorAccess(pc, actualdst, FsAction.WRITE);
    }

    if (dir.renameTo(src, dst)) {
      return true;
    }
    return false;
  }
  

  /**
   * Rename src to dst
   */
  void renameTo(final String src, final String dst,
      final Options.Rename... options) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(lf.getRenameINodeLock(nameNode,
            INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH, src, dst))
            .add(lf.getLeaseLock(LockType.WRITE))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.IV,
                BLK.PE, BLK.ER));
        if (dir.isQuotaEnabled()) {
          locks.add(lf.getQuotaUpdateLock(true, src, dst));
        }
      }

      @Override
      public Object performTask() throws IOException {
        HdfsFileStatus resultingStat = null;
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* NameSystem.renameTo: with options - " + src + " to " + dst);
        }
        FSPermissionChecker pc = getPermissionChecker();
        renameToInternal(pc, src, dst, options);
        resultingStat = getAuditFileInfo(dst, false);

        if (resultingStat != null) {
          StringBuilder cmd = new StringBuilder("rename options=");
          for (Rename option : options) {
            cmd.append(option.value()).append(" ");
          }
          logAuditEvent(true, cmd.toString(), src, dst, resultingStat);
        }
        return null;
      }
    }.handle(this);
  }

  private void renameToInternal(FSPermissionChecker pc, String src, String dst,
      Options.Rename... options) throws IOException, StorageException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
    }
    if (isPermissionEnabled) {
      checkParentAccess(pc, src, FsAction.WRITE);
      checkAncestorAccess(pc, dst, FsAction.WRITE);
    }

    dir.renameTo(src, dst, options);
  }

  /**
   * Remove the indicated file from namespace.
   *
   * @see ClientProtocol#delete(String, boolean) for detailed descriptoin and
   * description of exceptions
   */
  public boolean deleteWithTransaction(final String src,
      final boolean recursive) throws IOException {
    HopsTransactionalRequestHandler deleteHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.DELETE, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, false, src))
                .add(lf.getLeaseLock(LockType.WRITE))
                .add(lf.getLeasePathLock(LockType.READ_COMMITTED)).add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE,
                    BLK.IV));
            if (dir.isQuotaEnabled()) {
              locks.add(lf.getQuotaUpdateLock(true, src));
            }
            if (erasureCodingEnabled) {
              locks.add(lf.getEncodingStatusLock(LockType.WRITE, src));
            }
          }

          @Override
          public Object performTask() throws IOException {
            return delete(src, recursive);
          }
        };
    return (Boolean) deleteHandler.handle(this);
  }

  boolean delete(String src, boolean recursive)
      throws AccessControlException, SafeModeException, UnresolvedLinkException,
      IOException, StorageException {
    try {
      return deleteInt(src, recursive);
    } catch (AccessControlException e) {
      logAuditEvent(false, "delete", src);
      throw e;
    }
  }

  private boolean deleteInt(String src, boolean recursive)
      throws AccessControlException, SafeModeException, UnresolvedLinkException,
      IOException, StorageException {
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
      return new FSPermissionChecker(fsOwnerShortUserName, supergroup);
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
      throws AccessControlException, SafeModeException, UnresolvedLinkException,
      IOException, StorageException {
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();
    FSPermissionChecker pc = getPermissionChecker();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot delete " + src, safeMode);
    }
    if (!recursive && dir.isNonEmptyDirectory(src)) {
      throw new IOException(src + " is non empty");
    }
    if (enforcePermission && isPermissionEnabled) {
      checkPermission(pc, src, false, null, FsAction.WRITE, null, FsAction.ALL);
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
   * From the given list, incrementally remove the blocks from blockManager
   * Writelock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
   * ensure that other waiters on the lock can get in. See HDFS-2938
   */
  private void removeBlocks(List<Block> blocks)
      throws StorageException, TransactionContextException {
    int start = 0;
    int end = 0;
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      for (int i = start; i < end; i++) {
        blockManager.removeBlock(blocks.get(i));
      }
      start = end;
    }
  }
  
  void removePathAndBlocks(String src, List<Block> blocks)
      throws StorageException, IOException {
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    for (Block b : blocks) {
      blockManager.removeBlock(b);
    }
  }

  /**
   * Get the file info for a specific file.
   *
   * @param src
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
      throws AccessControlException, UnresolvedLinkException, IOException {
    HopsTransactionalRequestHandler getFileInfoHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.GET_FILE_INFO,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(true/*skip quota*/,nameNode, INodeLockType.READ,
                INodeResolveType.PATH, resolveLink, src));
          }

          @Override
          public Object performTask() throws IOException {
            HdfsFileStatus stat = null;
            FSPermissionChecker pc = getPermissionChecker();
            try {
              if (isPermissionEnabled) {
                checkTraverse(pc, src);
              }
              stat = dir.getFileInfo(src, resolveLink);
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
   * Create all the necessary directories
   */
  boolean mkdirs(final String src, final PermissionStatus permissions,
      final boolean createParent) throws IOException, UnresolvedLinkException {
    final boolean resolvedLink = false;
    HopsTransactionalRequestHandler mkdirsHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.MKDIRS, src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = getInstance();
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false,nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH,
                resolvedLink, src));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
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
      throws IOException, UnresolvedLinkException, StorageException {
    HdfsFileStatus resultingStat = null;
    boolean status = false;
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
          .debug(this.getNamenodeId() + ") DIR* NameSystem.mkdirs: " + src);
    }
    FSPermissionChecker pc = getPermissionChecker();
    status = mkdirsInternal(pc, src, permissions, createParent);
    if (status) {
      resultingStat = dir.getFileInfo(src, false);
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
      throws IOException, UnresolvedLinkException, StorageException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot create directory " + src, safeMode);
    }
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
    // heuristic because the mkdirs() operation migth need to 
    // create multiple inodes.
    checkFsObjectLimit();

    if (!dir.mkdirs(src, permissions, false, now())) {
      throw new IOException("Failed to create directory: " + src);
    }
    return true;
  }

  ContentSummary getContentSummary(final String src)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    if (isLegacyConentSummaryEnabled()) {
      throw new UnsupportedActionException("Legacy Content Summary is not supported");
    } else {
      return multiTransactionalGetContentSummary(src);
    }
  }

//  ContentSummary getContentSummaryLegacy(final String src)
//      throws AccessControlException, FileNotFoundException,
//      UnresolvedLinkException, IOException {
//    HopsTransactionalRequestHandler getContentSummaryHandler =
//        new HopsTransactionalRequestHandler(
//            HDFSOperationType.GET_CONTENT_SUMMARY, src) {
//          @Override
//          public void acquireLock(TransactionLocks locks) throws IOException {
//            LockFactory lf = getInstance();
//            locks.add(lf.getINodeLock(nameNode, INodeLockType.READ,
//                INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, src))
//                .add(lf.getBlockLock());
//          }
//
//          @Override
//          public Object performTask() throws IOException {
//            FSPermissionChecker pc =
//                new FSPermissionChecker(fsOwnerShortUserName, supergroup);
//            if (isPermissionEnabled) {
//              checkPermission(pc, src, false, null, null, null,
//                  FsAction.READ_EXECUTE);
//            }
//            return dir.getContentSummary(src);
//          }
//        };
//    return (ContentSummary) getContentSummaryHandler.handle(this);
//  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the
   * contract.
   */
//  void setQuota(final String path, final long nsQuota, final long dsQuota)
//      throws IOException, UnresolvedLinkException {
//    HopsTransactionalRequestHandler setQuotaHandler =
//        new HopsTransactionalRequestHandler(HDFSOperationType.SET_QUOTA, path) {
//          @Override
//          public void acquireLock(TransactionLocks locks) throws IOException {
//            LockFactory lf = getInstance();
//            locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
//                INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, path))
//                .add(lf.getBlockLock());
//          }
//
//          @Override
//          public Object performTask() throws StorageException, IOException {
//            checkSuperuserPrivilege();
//            if (isInSafeMode()) {
//              throw new SafeModeException("Cannot set quota on " + path,
//                  safeMode);
//            }
//            dir.setQuota(path, nsQuota, dsQuota);
//            return null;
//          }
//        };
//    setQuotaHandler.handle(this);
//  }
  
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
      final long lastBlockLength) throws IOException, UnresolvedLinkException {
    new HopsTransactionalRequestHandler(HDFSOperationType.FSYNC, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getINodeLock(nameNode, INodeLockType.WRITE, INodeResolveType.PATH,
                src)).add(lf.getLeaseLock(LockType.READ))
            .add(lf.getBlockLock());
      }

      @Override
      public Object performTask() throws IOException {
        NameNode.stateChangeLog
            .info("BLOCK* fsync: " + src + " for " + clientName);
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot fsync file " + src, safeMode);
        }
        INodeFileUnderConstruction pendingFile = checkLease(src, clientName);
        if (lastBlockLength > 0) {
          pendingFile.updateLengthOfLastBlock(lastBlockLength);
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
      throws AlreadyBeingCreatedException, IOException, UnresolvedLinkException,
      StorageException {
    LOG.info("Recovering " + lease + ", src=" + src);
    assert !isInSafeMode();

    final INodeFileUnderConstruction pendingFile =
        INodeFileUnderConstruction.valueOf(dir.getINode(src), src);
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
          uc.setExpectedLocations(blockManager.getNodes(lastBlock));
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
      INodeFileUnderConstruction pendingFile)
      throws StorageException, TransactionContextException {
    if (newHolder == null) {
      return lease;
    }
    return reassignLeaseInternal(lease, src, newHolder, pendingFile);
  }
  
  Lease reassignLeaseInternal(Lease lease, String src, String newHolder,
      INodeFileUnderConstruction pendingFile)
      throws StorageException, TransactionContextException {
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }

private void commitOrCompleteLastBlock(
      final INodeFileUnderConstruction fileINode, final Block commitBlock)
      throws IOException {
    
    if (!blockManager.commitOrCompleteLastBlock(fileINode, commitBlock)) {
      return;
    }

    fileINode.recomputeFileSize();     
    
    if (dir.isQuotaEnabled()) {
      final long diff = fileINode.getPreferredBlockSize()
          - commitBlock.getNumBytes();
      if (diff > 0) {
      // Adjust disk space consumption if required
      String path = leaseManager.findPath(fileINode);
      dir.updateSpaceConsumed(path, 0,
          -diff * fileINode.getBlockReplication());
      }
    }
    
    try {
      if (fileINode.isPathMetaEnabled()) {
        SizeLogDataAccess da = (SizeLogDataAccess)
            HdfsStorageFactory.getDataAccess(SizeLogDataAccess.class);
        da.add(new SizeLogEntry(fileINode.getId(), fileINode.getSize()));
      }
    } catch (StorageCallPreventedException e) {
      // Path is not available during block synchronization but it is OK
      // for us if search results are off by one block
    }
  }

  private void finalizeINodeFileUnderConstruction(String src,
      INodeFileUnderConstruction pendingFile)
      throws IOException, UnresolvedLinkException, StorageException {
    leaseManager.removeLease(pendingFile.getClientName(), src);

    // The file is no longer pending.
    // Create permanent INode, update blocks
    INodeFile newFile = pendingFile.convertToInodeFile();
    // close file and persist block allocations for this file
    dir.closeFile(src, newFile);

    blockManager.checkReplication(newFile);
  }

  void commitBlockSynchronization(final ExtendedBlock lastblock,
      final long newgenerationstamp, final long newlength,
      final boolean closeFile, final boolean deleteblock,
      final DatanodeID[] newtargets, final String[] newtargetstorages)
      throws IOException, UnresolvedLinkException {
    new HopsTransactionalRequestHandler(
        HDFSOperationType.COMMIT_BLOCK_SYNCHRONIZATION) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void setUp() throws StorageException {
        inodeIdentifier =
            INodeUtil.resolveINodeFromBlock(lastblock.getLocalBlock());
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier,
                true)).add(lf.getLeaseLock(LockType.WRITE))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
            .add(lf.getBlockLock(lastblock.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.RE, BLK.CR, BLK.ER, BLK.UC, BLK.UR));
      }

      @Override
      public Object performTask() throws IOException {
        String src = "";
        // If a DN tries to commit to the standby, the recovery will
        // fail, and the next retry will succeed on the new NN.

        if (isInSafeMode()) {
          throw new SafeModeException(
              "Cannot commitBlockSynchronization while in safe mode", safeMode);
        }
        LOG.info("commitBlockSynchronization(lastblock=" + lastblock +
            ", newgenerationstamp=" + newgenerationstamp + ", newlength=" +
            newlength + ", newtargets=" + Arrays.asList(newtargets) +
            ", closeFile=" + closeFile + ", deleteBlock=" + deleteblock + ")");
        final BlockInfo storedBlock =
            blockManager.getStoredBlock(ExtendedBlock.getLocalBlock(lastblock));
        if (storedBlock == null) {
          throw new IOException("Block (=" + lastblock + ") not found");
        }
        INodeFile iFile = (INodeFile) storedBlock.getBlockCollection();
        if (!iFile.isUnderConstruction() || storedBlock.isComplete()) {
          throw new IOException(
              "Unexpected block (=" + lastblock + ") since the file (=" +
                  iFile.getLocalName() + ") is not under construction");
        }

        long recoveryId =
            ((BlockInfoUnderConstruction) storedBlock).getBlockRecoveryId();
        if (recoveryId != newgenerationstamp) {
          throw new IOException("The recovery id " + newgenerationstamp +
              " does not match current recovery id " + recoveryId +
              " for block " + lastblock);
        }

        INodeFileUnderConstruction pendingFile =
            (INodeFileUnderConstruction) iFile;

        if (deleteblock) {
          pendingFile.removeLastBlock(ExtendedBlock.getLocalBlock(lastblock));
          blockManager.removeBlockFromMap(storedBlock);
        } else {
          // update last block
          storedBlock.setGenerationStamp(newgenerationstamp);
          storedBlock.setNumBytes(newlength);
          iFile.recomputeFileSize();
          // find the DatanodeDescriptor objects
          // There should be no locations in the blockManager till now because the
          // file is underConstruction
          DatanodeDescriptor[] descriptors = null;
          if (newtargets.length > 0) {
            descriptors = new DatanodeDescriptor[newtargets.length];
            for (int i = 0; i < newtargets.length; i++) {
              descriptors[i] =
                  blockManager.getDatanodeManager().getDatanode(newtargets[i]);
            }
          }
          if ((closeFile) && (descriptors != null)) {
            // the file is getting closed. Insert block locations into blockManager.
            // Otherwise fsck will report these blocks as MISSING, especially if the
            // blocksReceived from Datanodes take a long time to arrive.
            for (int i = 0; i < descriptors.length; i++) {
              descriptors[i].addBlock(storedBlock);
            }
          }
          // add pipeline locations into the INodeUnderConstruction
          pendingFile.setLastBlock(storedBlock, descriptors);
        }

        src = leaseManager.findPath(pendingFile);
        if (closeFile) {
          // commit the last block and complete it if it has minimum replicas
          commitOrCompleteLastBlock(pendingFile, storedBlock);

          //remove lease, close file
          finalizeINodeFileUnderConstruction(src, pendingFile);
        } else {
          // If this commit does not want to close the file, persist blocks
          dir.persistBlocks(src, pendingFile);
        }
        if (closeFile) {
          LOG.info(
              "commitBlockSynchronization(newblock=" + lastblock + ", file=" +
                  src + ", newgenerationstamp=" + newgenerationstamp +
                  ", newlength=" + newlength + ", newtargets=" +
                  Arrays.asList(newtargets) +
                  ") successful");
        } else {
          LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
        }
        return null;
      }

    }.handle(this);
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
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot renew lease for " + holder,
              safeMode);
        }
        leaseManager.renewLease(holder);
        return null;
      }
    }.handle(this);
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * @param src
   *     the directory name
   * @param startAfter
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
  DirectoryListing getListing(final String src, final byte[] startAfter,
      final boolean needLocation)
      throws AccessControlException, UnresolvedLinkException, IOException {
    HopsTransactionalRequestHandler getListingHandler =
        new HopsTransactionalRequestHandler(HDFSOperationType.GET_LISTING,
            src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getINodeLock(true/*skip INodeAttr*/, nameNode, INodeLockType.READ,
                INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, src));
            if(needLocation){
                locks
                .add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.ER, BLK.CR, BLK.UC));
            }
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
      throws AccessControlException, UnresolvedLinkException, IOException,
      StorageException {
    DirectoryListing dl;
    FSPermissionChecker pc = getPermissionChecker();
    if (isPermissionEnabled) {
      if (dir.isDir(src)) {
        checkPathAccess(pc, src, FsAction.READ_EXECUTE);
      } else {
        checkTraverse(pc, src);
      }
    }
    logAuditEvent(true, "listStatus", src);
    dl = dir.getListing(src, startAfter, needLocation);
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
  HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg, long capacity,
      long dfsUsed, long remaining, long blockPoolUsed, int xceiverCount,
      int xmitsInProgress, int failedVolumes) throws IOException {
    final int maxTransfer =
        blockManager.getMaxReplicationStreams() - xmitsInProgress;
    DatanodeCommand[] cmds = blockManager.getDatanodeManager()
        .handleHeartbeat(nodeReg, blockPoolId, capacity, dfsUsed, remaining,
            blockPoolUsed, xceiverCount, maxTransfer, failedVolumes);
    return new HeartbeatResponse(cmds);
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   *
   * @return true if there were sufficient resources available, false otherwise.
   */
  boolean nameNodeHasResourcesAvailable() {
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
   * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p/>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    
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
    private double replQueueThreshold;

    // internal fields
    /**
     * Time when threshold was reached.
     * <p/>
     * <br>-1 safe mode is off
     * <br> 0 safe mode is on, but threshold is not reached yet
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
    private int blockReplQueueThreshold;
    /**
     * time of the last status printout
     */
    private long lastStatusReport = 0;
    /**
     * flag indicating whether replication queues have been initialized
     */
    boolean initializedReplQueues = false;
    /**
     * Was safemode entered automatically because available resources were low.
     */
    private boolean resourcesLow = false;
    
    public ThreadLocal<Boolean> safeModePendingOperation =
        new ThreadLocal<Boolean>();
    
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
      this.replQueueThreshold =
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
    private SafeModeInfo(boolean resourcesLow) throws IOException {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.replQueueThreshold = 1.5f; // can never be reached
      this.blockTotal = -1;
      this.reached = -1;
      this.resourcesLow = resourcesLow;
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
    private boolean isPopulatingReplQueues() {
      return initializedReplQueues;
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
      // In the standby, do not populate repl queues
      if (!isPopulatingReplQueues() && shouldPopulateReplQueues()) {
        initializeReplQueues();
      }
      
      leaveInternal();
      
      HdfsVariables.exitClusterSafeMode();
      HdfsVariables.resetMisReplicatedIndex();
      clearSafeBlocks();
    }
    
    private void leaveInternal() throws IOException {
      long timeInSafemode = now() - startTime;
      NameNode.stateChangeLog.info(
          "STATE* Leaving safe mode after " + timeInSafemode / 1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime((int) timeInSafemode);
      
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
    }

    /**
     * Initialize replication queues.
     */
    private void initializeReplQueues() throws IOException {
      LOG.info("initializing replication queues");
      assert !isPopulatingReplQueues() : "Already initialized repl queues";
      long startTimeMisReplicatedScan = now();
      blockManager.processMisReplicatedBlocks();
      initializedReplQueues = true;
      NameNode.stateChangeLog.info("STATE* Replication Queue initialization " +
          "scan for invalid, over- and under-replicated blocks " +
          "completed in " + (now() - startTimeMisReplicatedScan) + " msec");
    }

    /**
     * Check whether we have reached the threshold for
     * initializing replication queues.
     */
    private boolean canInitializeReplQueues() throws IOException {
      return shouldPopulateReplQueues() &&
          blockSafe() >= blockReplQueueThreshold;
    }

    /**
     * Safe mode can be turned off iff
     * another namenode went out of safemode or
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
     * This NameNode tries to help the cluster to get out of safemode by
     * updaing the safeblock count.
     * This call will trigger the @link{SafeModeMonitor} if it's not already
     * started.
     * @throws IOException
     */
    private void tryToHelpToGetout() throws IOException {
      if (isManual()) {
        return;
      }
      startSafeModeMonitor();
    }

    /**
     * The cluster already left safemode, now it's time to for this namenode
     * to leave as well.
     * @throws IOException
     */
    private void clusterLeftSafeModeAlready() throws IOException {
      leaveInternal();
    }

    /**
     * There is no need to enter safe mode
     * if DFS is empty or {@link #threshold} == 0 or another namenode already
     * went out of safemode
     */
    private boolean needEnter() throws IOException {
      if (!isClusterInSafeMode()) {
        return false;
      }
      return (threshold != 0 && blockSafe() < blockThreshold) ||
          (getNumLiveDataNodes() < datanodeThreshold) ||
          (!nameNodeHasResourcesAvailable());
    }

    /**
     * Check and trigger safe mode if needed.
     */
    
    private void checkMode() throws IOException {
      // Have to have write-lock since leaving safemode initializes
      // repl queues, which requires write lock
      if (needEnter()) {
        enter();
        // check if we are ready to initialize replication queues
        if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
          initializeReplQueues();
        }
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached
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
      if (canInitializeReplQueues() && !isPopulatingReplQueues()) {
        initializeReplQueues();
      }
    }

    private synchronized void startSafeModeMonitor() {
      if (smmthread == null) {
        smmthread = new Daemon(new SafeModeMonitor());
        smmthread.start();
      }
    }

    /**
     * Set total number of blocks.
     */
    private synchronized void setBlockTotal(int total) throws IOException {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      this.blockReplQueueThreshold = (int) (blockTotal * replQueueThreshold);
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
      return extension == Integer.MAX_VALUE && !resourcesLow;
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
    String getTurnOffTip() {
      if (reached < 0) {
        return "Safe mode is OFF.";
      }
      String leaveMsg = "";
      if (areResourcesLow()) {
        leaveMsg = "Resources are low on NN. " +
            "Please add or free up more resources then turn off safe mode manually.  " +
            "NOTE:  If you turn off safe mode before adding resources, " +
            "the NN will immediately return to safe mode.";
      } else {
        leaveMsg = "Safe mode will be turned off automatically";
      }
      if (isManual()) {
        leaveMsg =
            "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off";
      }

      if (blockTotal < 0) {
        return leaveMsg + ".";
      }

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
                  " blocks to reach the threshold %.4f of total blocks %d.",
              blockSafe, (blockThreshold - blockSafe) + 1, threshold,
              blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg += String.format(
              "The number of live datanodes %d needs an additional %d live " +
                  "datanodes to reach the minimum number %d.", numLive,
              (datanodeThreshold - numLive), datanodeThreshold);
        }
        msg += " " + leaveMsg;
      } else {
        msg = String.format("The reported blocks %d has reached the threshold" +
                " %.4f of total blocks %d.", blockSafe, threshold, blockTotal);

        if (datanodeThreshold > 0) {
          msg += String.format(" The number of live datanodes %d has reached " +
                  "the minimum number %d.", numLive, datanodeThreshold);
        }
        msg += " " + leaveMsg;
      }
      if (reached == 0 ||
          isManual()) {  // threshold is not reached or manual
        return msg + ".";
      }
      // extension period is in progress
      return msg + " in " + Math.abs(reached + extension - now()) / 1000 +
          " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) throws IOException {
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
      LOG.debug("SafeModeX Some operation are put on hold");
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
        if (safeModePendingOperation.get().booleanValue() == true) {
          LOG.debug("SafeMode about to perform pending safemode operation");
          safeModePendingOperation.set(false);
          checkMode();
        }
      }
    }

    /**
     * Get number of safeblocks from the database
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
     * interval in msec for checking safe mode: {@value}
     */
    private static final long recheckInterval = 1000;
    
    /**
     */
    @Override
    public void run() {
      try {
        while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
          safeMode.checkMode();
          try {
            Thread.sleep(recheckInterval);
          } catch (InterruptedException ie) {
          }
        }
        if (!fsRunning) {
          LOG.info("NameNode is being shutdown, exit SafeModeMonitor thread");
        } else {
          try {
            // leave safe mode and stop the monitor
            leaveSafeMode();
          } catch (IOException ex) {
            LOG.error(ex);
          }
        }
        smmthread = null;
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
      safeMode.tryToHelpToGetout();
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
    return !safeMode.isManual() && safeMode.isOn();
  }

  @Override
  public boolean isPopulatingReplQueues() {
    if (!shouldPopulateReplQueues()) {
      return false;
    }
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return true;
    }
    return safeMode.isPopulatingReplQueues();
  }

  private boolean shouldPopulateReplQueues() {
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
      throws StorageException, IOException {
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
   *     the change i nnumber of total blocks expected
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
  public void setBlockTotal() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode == null) {
      return;
    }
    safeMode.setBlockTotal((int) blockManager.getTotalCompleteBlocks());
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
   * Enter safe mode manually.
   *
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    // Stop the secret manager, since rolling the master key would
    // try to write to the edit log
    stopSecretManager();

    if (!isInSafeMode()) {
      safeMode = new SafeModeInfo(resourcesLow);
      HdfsVariables.enterClusterSafeMode();
      return;
    }
    if (resourcesLow) {
      safeMode.setResourcesLow();
    }
    safeMode.setManual();

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

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup,
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
      FsAction access, FsAction subAccess)
      throws IOException {
    if (!pc.isSuperUser()) {
      pc.checkPermission(path, dir.getRootDir(), doCheckOwner, ancestorAccess,
          parentAccess, access, subAccess);
    }
  }
  
  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException, StorageException {
    if (maxFsObjects != 0 &&
        maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
          maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system.
   */
  long getMaxObjects() {
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
    }
  }
  

  @Override // FSNamesystemMBean
  public int getNumLiveDataNodes() {
    return getBlockManager().getDatanodeManager().getNumLiveDataNodes();
  }

  @Override // FSNamesystemMBean
  public int getNumDeadDataNodes() {
    return getBlockManager().getDatanodeManager().getNumDeadDataNodes();
  }
  
  @Override // FSNamesystemMBean
  @Metric({"StaleDataNodes",
      "Number of datanodes marked stale due to delayed heartbeat"})
  public int getNumStaleDataNodes() {
    return getBlockManager().getDatanodeManager().getNumStaleNodes();
  }

  private INodeFileUnderConstruction checkUCBlock(ExtendedBlock block,
      String clientName) throws IOException, StorageException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get a new generation stamp and an " +
          "access token for block " + block, safeMode);
    }
    
    // check stored block state
    BlockInfo storedBlock =
        blockManager.getStoredBlock(ExtendedBlock.getLocalBlock(block));
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
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) file;
    if (clientName == null || !clientName.equals(pendingFile.getClientName())) {
      throw new LeaseExpiredException("Lease mismatch: " + block +
          " is accessed by a non lease holder " + clientName);
    }

    return pendingFile;
  }
  
  /**
   * Client is reporting some bad block locations.
   */
  void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    NameNode.stateChangeLog.info("*DIR* reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      ExtendedBlock blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        blockManager
            .findAndMarkBlockAsCorrupt(blk, dn, "client machine reported it");
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
                lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
                .add(lf.getBlockLock(block.getBlockId(), inodeIdentifier));
          }

          @Override
          public Object performTask() throws IOException {
            LocatedBlock locatedBlock;
            // check vadility of parameters
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
      final ExtendedBlock newBlock, final DatanodeID[] newNodes)
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
        locks.add(
            lf.getIndividualINodeLock(INodeLockType.WRITE, inodeIdentifier))
            .add(lf.getLeaseLock(LockType.READ))
            .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
            .add(lf.getBlockLock(oldBlock.getBlockId(), inodeIdentifier))
            .add(lf.getBlockRelated(BLK.UC));
      }

      @Override
      public Object performTask() throws IOException {
        if (isInSafeMode()) {
          throw new SafeModeException("Pipeline not updated", safeMode);
        }
        assert
            newBlock.getBlockId() == oldBlock.getBlockId() :
            newBlock + " and " + oldBlock + " has different block identifier";
        LOG.info("updatePipeline(block=" + oldBlock + ", newGenerationStamp=" +
            newBlock.getGenerationStamp() + ", newLength=" +
            newBlock.getNumBytes() + ", newNodes=" + Arrays.asList(newNodes) +
            ", clientName=" + clientName + ")");
        updatePipelineInternal(clientName, oldBlock, newBlock, newNodes);
        LOG.info(
            "updatePipeline(" + oldBlock + ") successfully to " + newBlock);
        return null;
      }
    }.handle(this);
  }

  /**
   * @see #updatePipeline(String, ExtendedBlock, ExtendedBlock, DatanodeID[])
   */
  private void updatePipelineInternal(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException, StorageException {
    // check the vadility of the block and lease holder name
    final INodeFileUnderConstruction pendingFile =
        checkUCBlock(oldBlock, clientName);
    final BlockInfoUnderConstruction blockinfo =
        (BlockInfoUnderConstruction) pendingFile.getLastBlock();

    // check new GS & length: this is not expected
    if (newBlock.getGenerationStamp() <= blockinfo.getGenerationStamp() ||
        newBlock.getNumBytes() < blockinfo.getNumBytes()) {
      String msg = "Update " + oldBlock + " (len = " +
          blockinfo.getNumBytes() + ") to an older state: " + newBlock +
          " (len = " + newBlock.getNumBytes() + ")";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    // Update old block with the new generation stamp and new length
    blockinfo.setGenerationStamp(newBlock.getGenerationStamp());
    blockinfo.setNumBytes(newBlock.getNumBytes());
    pendingFile.recomputeFileSize();
    // find the DatanodeDescriptor objects
    final DatanodeManager dm = getBlockManager().getDatanodeManager();
    DatanodeDescriptor[] descriptors = null;
    if (newNodes.length > 0) {
      descriptors = new DatanodeDescriptor[newNodes.length];
      for (int i = 0; i < newNodes.length; i++) {
        descriptors[i] = dm.getDatanode(newNodes[i]);
      }
    }
    blockinfo.setExpectedLocations(descriptors);
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
    
    public CorruptFileBlockInfo(String p, Block b) {
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
   *     back is ordered by blockid; startBlockAfter tells where to start from
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
        new ArrayList<CorruptFileBlockInfo>();

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
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL, this);
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
          public Object performTask() throws StorageException, IOException {
            Token<DelegationTokenIdentifier> token;
            if (isInSafeMode()) {
              throw new SafeModeException("Cannot issue delegation token",
                  safeMode);
            }
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
            token = new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);
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
      throws InvalidToken, IOException {
    //FIXME This does not seem to be persisted
    HopsTransactionalRequestHandler renewDelegationTokenHandler =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.RENEW_DELEGATION_TOKEN) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            long expiryTime;
            if (isInSafeMode()) {
              throw new SafeModeException("Cannot renew delegation token",
                  safeMode);
            }
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
          public Object performTask() throws StorageException, IOException {
            if (isInSafeMode()) {
              throw new SafeModeException("Cannot cancel delegation token",
                  safeMode);
            }
            String canceller = getRemoteUser().getUserName();
            DelegationTokenIdentifier id =
                dtSecretManager.cancelToken(token, canceller);
            return null;
          }
        };
    cancelDelegationTokenHandler.handle(this);
  }
  
  /**
   * @param out
   *     save state of the secret manager
   */
  void saveSecretManagerState(DataOutputStream out) throws IOException {
    dtSecretManager.saveSecretManagerState(out);
  }

  /**
   * @param in
   *     load the state of secret manager from input stream
   */
  void loadSecretManagerState(DataInputStream in) throws IOException {
    dtSecretManager.loadSecretManagerState(in);
  }

  /**
   * Log the updateMasterKey operation to edit logs
   *
   * @param key
   *     new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
    
    assert !isInSafeMode() :
        "this should never be called while in safemode, since we stop " +
            "the DT manager before entering safemode!";
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
  // UGI.getCurrentUser which is synch'ed
  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = null;
    if (Server.isRpcInvocation()) {
      ugi = Server.getRemoteUser();
    }
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
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
    MBeans.register("NameNode", "NameNodeInfo", this);
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
    return "Safe mode is ON." + this.getSafeModeTip();
  }

  @Override // NameNodeMXBean
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
        new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(live, null, true);
    for (DatanodeDescriptor node : live) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("usedSpace", getDfsUsed(node));
      innerinfo.put("adminState", node.getAdminState().toString());
      innerinfo.put("nonDfsUsedSpace", node.getNonDfsUsed());
      innerinfo.put("capacity", node.getCapacity());
      innerinfo.put("numBlocks", node.numBlocks());
      info.put(node.getHostName(), innerinfo);
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
        new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    blockManager.getDatanodeManager().fetchDatanodes(null, dead, true);
    for (DatanodeDescriptor node : dead) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("decommissioned", node.isDecommissioned());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Map<String, Object>> info =
        new HashMap<String, Map<String, Object>>();
    final List<DatanodeDescriptor> decomNodeList =
        blockManager.getDatanodeManager().getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("underReplicatedBlocks",
          node.decommissioningStatus.getUnderReplicatedBlocks());
      innerinfo.put("decommissionOnlyReplicas",
          node.decommissioningStatus.getDecommissionOnlyReplicas());
      innerinfo.put("underReplicateInOpenFiles",
          node.decommissioningStatus.getUnderReplicatedInOpenFiles());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (Time.now() - alivenode.getLastUpdate()) / 1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
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
  
  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    throw new UnsupportedOperationException(
        "HOP: there are no name dirs any more");
  }

  /**
   * @return the block manager.
   */
  public BlockManager getBlockManager() {
    return blockManager;
  }
  
  /**
   * Verifies that the given identifier and password are valid and match.
   *
   * @param identifier
   *     Token identifier.
   * @param password
   *     Password in the token.
   * @throws InvalidToken
   */
  public synchronized void verifyToken(DelegationTokenIdentifier identifier,
      byte[] password) throws InvalidToken {
    getDelegationTokenSecretManager().verifyToken(identifier, password);
  }
  
  @Override
  public boolean isGenStampInFuture(long genStamp) throws StorageException {
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

  /**
   * Default AuditLogger implementation; used when no access logger is
   * defined in the config file. It can also be explicitly listed in the
   * config file.
   */
  private static class DefaultAuditLogger implements AuditLogger {

    @Override
    public void initialize(Configuration conf) {
      // Nothing to do.
    }

    @Override
    public void logAuditEvent(boolean succeeded, String userName,
        InetAddress addr, String cmd, String src, String dst,
        FileStatus status) {
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
        auditLog.info(sb);
      }
    }

  }

  public void hopSpecificInitialization(Configuration conf) throws IOException {
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
  
  public String getSupergroup() {
    return this.supergroup;
  }

  public void performPendingSafeModeOperation() throws IOException {
    // safeMode is volatile, and may be set to null at any time
    SafeModeInfo safeMode = this.safeMode;
    if (safeMode != null) {
      safeMode.performSafeModePendingOperation();
    }
  }


  public void changeConf(List<String> props, List<String> newVals)
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
      if (obj1Length == obj2Length) {
        return 0;
      } else if (obj1Length < obj2Length) {
        return 1;
      } else {
        return -1;
      }
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

  public String getFilePathAncestorLockType() {
    return conf.get(DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE,
        DFSConfigKeys.DFS_STORAGE_ANCESTOR_LOCK_TYPE_DEFAULT);
  }

  /**
   * Update safeblocks in the database
   * @param safeBlock
   *      block to be added to safeblocks
   * @throws IOException
   */
  private void addSafeBlock(final Long safeBlock) throws IOException {
    Set<Long> safeBlocks = new HashSet<Long>();
    safeBlocks.add(safeBlock);
    addSafeBlocks(safeBlocks);
  }

  /**
   * Remove a block that is not considered safe anymore
   * @param safeBlock
   *      block to be removed from safeblocks
   * @throws IOException
   */
  private void removeSafeBlock(final Long safeBlock) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.REMOVE_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.remove(safeBlock);
        return null;
      }
    };
  }

  /**
   * Update safeblocks in the database
   * @param safeBlocks
   *      list of blocks to be added to safeblocks
   * @throws IOException
   */
  private void addSafeBlocks(final Set<Long> safeBlocks) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ADD_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.insert(safeBlocks);
        return null;
      }
    }.handle();
  }

  /**
   * Get number of blocks to be considered safe in the current cluster
   * @return number of safeblocks
   * @throws IOException
   */
  private int getBlockSafe() throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.GET_SAFE_BLOCKS_COUNT) {
      @Override
      public Object performTask() throws StorageException, IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /**
   * Delete all safeblocks
   * @throws IOException
   */
  private void clearSafeBlocks() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.CLEAR_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        SafeBlocksDataAccess da = (SafeBlocksDataAccess) HdfsStorageFactory
            .getDataAccess(SafeBlocksDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  /**
   * Check if the cluster is in safemode?
   * @return true if the cluster in safemode, false otherwise.
   * @throws IOException
   */
  private boolean isClusterInSafeMode() throws IOException {
    return HdfsVariables.isClusterInSafeMode();
  }

  boolean isPermissionEnabled() {
    return isPermissionEnabled;
  }

  ExecutorService getSubtreeOperationsExecutor() {
    return subtreeOperationsExecutor;
  }
  
  boolean isLegacyDeleteEnabled() {
    return legacyDeleteEnabled;
  }

  boolean isLegacyRenameEnabled() {
    return legacyRenameEnabled;
  }

  boolean isLegacyConentSummaryEnabled() {
    return legacyContentSummaryEnabled;
  }

  boolean isLegacySetQuotaEnabled() {
    return legacySetQuotaEnabled;
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
   * @param path
   *    the path of the directory where the quota should be set
   * @param nsQuota
   *    the namespace quota to be set
   * @param dsQuota
   *    the diskspace quota to be set
   * @throws IOException, UnresolvedLinkException
   */
  void multiTransactionalSetQuota(final String path, final long nsQuota,
      final long dsQuota) throws IOException, UnresolvedLinkException {
    checkSuperuserPrivilege();
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot set quota on " + path, safeMode);
    }
    if (!isLeader()) {
      throw new RuntimeException("Asked non leading node to setQuota");
    }

    INodeIdentifier subtreeRoot = null;
    boolean removeSTOLock = false;
    
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
        subtreeRoot = lockSubtree(path, SubTreeOperation.StoOperationType.QUOTA_STO);      
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
          // Not sure if this can happend if we are not shutting down but we need to abort in case it happens.
          throw new IOException("Operation failed due to an Interrupt");
        }
      }

      HopsTransactionalRequestHandler setQuotaHandler =
          new HopsTransactionalRequestHandler(HDFSOperationType.SET_QUOTA,
              path) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
              LockFactory lf = LockFactory.getInstance();
              locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
                  INodeResolveType.PATH, true, true, path))
                  .add(lf.getBlockLock());
            }

            @Override
            public Object performTask() throws StorageException, IOException {
              dir.setQuota(path, nsQuota, dsQuota, fileTree.getNamespaceCount(),
                  fileTree.getDiskspaceCount());
              return null;
            }
          };
      setQuotaHandler.handle(this);
    } finally {
      if(removeSTOLock){
        unlockSubtree(path);
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
   * @param path
   *    the path
   * @return
   *    the content summary for the given path
   * @throws IOException
   */
  // [S] what if you call content summary on the root
  // I have removed sub tree locking from the content summary for now
  // TODO : fix content summary sub tree locking
  // 
    ContentSummary multiTransactionalGetContentSummary(final String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
  
      PathInformation pathInfo = getPathExistingINodesFromDB(path,
              false, null, null, null, null);
      if(pathInfo.getPathInodes()[pathInfo.getPathComponents().length-1] == null){
        throw new FileNotFoundException("File does not exist: " + path);
      }
      final INode subtreeRoot = pathInfo.getPathInodes()[pathInfo.getPathComponents().length-1];
      final INodeIdentifier subtreeRootIdentifer = new INodeIdentifier(subtreeRoot.getId(),subtreeRoot.getParentId(),
          subtreeRoot.getLocalName(),subtreeRoot.getPartitionId());
      subtreeRootIdentifer.setDepth(((short) (INodeDirectory.ROOT_DIR_DEPTH + pathInfo.getPathComponents().length-1 )));

      final AbstractFileTree.CountingFileTree fileTree =
              new AbstractFileTree.CountingFileTree(this, subtreeRootIdentifer, FsAction.READ_EXECUTE);
      fileTree.buildUp();
      return (ContentSummary) new LightWeightRequestHandler(
              HDFSOperationType.GET_SUBTREE_ATTRIBUTES) {
        @Override
        public Object performTask() throws StorageException, IOException {
          INodeAttributesDataAccess<INodeAttributes> dataAccess =
                  (INodeAttributesDataAccess<INodeAttributes>) HdfsStorageFactory
                  .getDataAccess(INodeAttributesDataAccess.class);
          INodeAttributes attributes =
                  dataAccess.findAttributesByPk(subtreeRoot.getId());
          if(attributes!=null){

//            assert fileTree.getDiskspaceCount() == attributes.getDiskspace(): "Diskspace count did not match fileTree "+fileTree.getDiskspaceCount()+" attributes "+attributes.getDiskspace();
//            assert fileTree.getNamespaceCount() == attributes.getNsCount(): "Namespace count did not match fileTree "+fileTree.getNamespaceCount()+" attributes "+attributes.getNsCount();
          }
          return new ContentSummary(fileTree.getFileSizeSummary(),
                  fileTree.getFileCount(), fileTree.getDirectoryCount(),
                  attributes == null ? subtreeRoot.getNsQuota()
                  : attributes.getNsQuota(), fileTree.getDiskspaceCount(),
                  attributes == null ? subtreeRoot.getDsQuota()
                  : attributes.getDsQuota());
        }
      }.handle(this);
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
   * @param src
   *    the source
   * @param dst
   *    the destination
   * @throws IOException
   */
  void multiTransactionalRename(final String src, final String dst,
          final Options.Rename... options) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
              "DIR* NameSystem.multiTransactionalRename: with options - " + src
              + " to " + dst);
    }

    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
    if (!DFSUtil.isValidName(dst)) {
      throw new InvalidPathException("Invalid name: " + dst);
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
    String error = null;
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
            && dst.equals(((INodeSymlink) srcInode).getLinkValue())) {
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
    
    INode srcDataset = getMetaEnabledParent(srcInodes);
    INode dstDataset = getMetaEnabledParent(dstInodes);
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;
    
    //--
    //TODO [S]  if src is a file then there is no need for sub tree locking 
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation.
    long srcNsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    long srcDsCount = srcInfo.getDsCount();
    long dstNsCount = dstInfo.getNsCount();
    long dstDsCount = dstInfo.getDsCount();
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
                FsAction.WRITE, null, null, SubTreeOperation.StoOperationType.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (pathIsMetaEnabled(srcInodes) || pathIsMetaEnabled(dstInodes)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(this,
                    srcSubTreeRoot, srcDataset, dstDataset);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(this,
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcNsCount = srcFileTree.getNamespaceCount();
          srcDsCount = srcFileTree.getDiskspaceCount();
        }
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      renameTo(src, dst, srcNsCount, srcDsCount, dstNsCount, dstDsCount,
              isUsingSubTreeLocks, subTreeLockDst, logEntries, options);
      renameTransactionCommitted = true;
    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          unlockSubtree(src);
        }
      }
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

  private void renameTo(final String src, final String dst, final long srcNsCount,
      final long srcDsCount, final long dstNsCount, final long dstDsCount,
      final boolean isUsingSubTreeLocks, final String subTreeLockDst,
      final Collection<MetadataLogEntry> logEntries,
      final Options.Rename... options
      )
      throws IOException, UnresolvedLinkException {
    new HopsTransactionalRequestHandler(
            isUsingSubTreeLocks?HDFSOperationType.SUBTREE_RENAME:
            HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getRenameINodeLock(nameNode,
            INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            INodeResolveType.PATH, true, src, dst))
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
      }

      @Override
      public Object performTask() throws IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug(
              "DIR* NameSystem.renameTo: with options - " + src + " to " + dst);
        }

        if (isInSafeMode()) {
          throw new SafeModeException("Cannot rename " + src, safeMode);
        }
        if (!DFSUtil.isValidName(dst)) {
          throw new InvalidPathException("Invalid name: " + dst);
        }
        for (MetadataLogEntry logEntry : logEntries) {
          EntityManager.add(logEntry);
        }

        for (Options.Rename op : options) {
          if (op == Rename.KEEP_ENCODING_STATUS) {
            INode[] srcNodes =
                dir.getRootDir().getExistingPathINodes(src, false);
            INode[] dstNodes =
                dir.getRootDir().getExistingPathINodes(dst, false);
            INode srcNode = srcNodes[srcNodes.length - 1];
            INode dstNode = dstNodes[dstNodes.length - 1];
            EncodingStatus status = EntityManager.find(
                EncodingStatus.Finder.ByInodeId, dstNode.getId());
            EncodingStatus newStatus = new EncodingStatus(status);
            newStatus.setInodeId(srcNode.getId());
            EntityManager.add(newStatus);
            EntityManager.remove(status);
            break;
          }
        }
        
        removeSubTreeLocksForRenameInternal(src, isUsingSubTreeLocks, 
                subTreeLockDst);
        
        dir.renameTo(src, dst, srcNsCount, srcDsCount, dstNsCount, dstDsCount,
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
      INode[] nodes = null;
      INode inode = null;
      if (!src.equals("/")) {
        EntityManager.remove(new SubTreeOperation(getSubTreeLockPathPrefix(src)));
        nodes = dir.getRootDir().getExistingPathINodes(src, false);
        inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSubtreeLocked()) {
          inode.setSubtreeLocked(false);
          EntityManager.update(inode);
        }
      }
    }
  }
  
  @Deprecated
  boolean multiTransactionalRename(final String src, final String dst)
      throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug(
          "DIR* NameSystem.multiTransactionalRename: with options - " + src +
              " to " + dst);
    }

    if (isInSafeMode()) {
      throw new SafeModeException("Cannot rename " + src, safeMode);
    }
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
    
    INode srcDataset = getMetaEnabledParent(srcInfo.getPathInodes());
    INode dstDataset = getMetaEnabledParent(dstInfo.getPathInodes());
    Collection<MetadataLogEntry> logEntries = Collections.EMPTY_LIST;
    
    //TODO [S]  if src is a file then there is no need for sub tree locking 
    //mechanism on the src and dst
    //However the quota is enabled then all the quota update on the dst
    //must be applied before the move operation. 
    long srcNsCount = srcInfo.getNsCount(); //if not dir then it will return zero
    long srcDsCount = srcInfo.getDsCount();
    long dstNsCount = dstInfo.getNsCount();
    long dstDsCount = dstInfo.getDsCount();  
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
                FsAction.WRITE, null, null, SubTreeOperation.StoOperationType.RENAME_STO);

        if (srcSubTreeRoot != null) {
          AbstractFileTree.QuotaCountingFileTree srcFileTree;
          if (pathIsMetaEnabled(srcInfo.pathInodes) || pathIsMetaEnabled(dstInfo.pathInodes)) {
            srcFileTree = new AbstractFileTree.LoggingQuotaCountingFileTree(this,
                    srcSubTreeRoot, srcDataset, dstDataset);
            srcFileTree.buildUp();
            logEntries = ((AbstractFileTree.LoggingQuotaCountingFileTree) srcFileTree).getMetadataLogEntries();
          } else {
            srcFileTree = new AbstractFileTree.QuotaCountingFileTree(this,
                    srcSubTreeRoot);
            srcFileTree.buildUp();
          }
          srcNsCount = srcFileTree.getNamespaceCount();
          srcDsCount = srcFileTree.getDiskspaceCount();
        }
      } else {
        LOG.debug("Rename src: " + src + " dst: " + dst + " does not require sub-tree locking mechanism");
      }

      boolean retValue = renameTo(src, dst, srcNsCount, srcDsCount, dstNsCount, dstDsCount,
              isUsingSubTreeLocks, subTreeLockDst, logEntries);

      // the rename Tx has commited. it has also remove the subTreelocks
      renameTransactionCommitted = true;

      return retValue;

    } finally {
      if (!renameTransactionCommitted) {
        if (srcSubTreeRoot != null) { //only unlock if locked
          unlockSubtree(src);
        }
     }
    }
  }

  /**
   * Change the indicated filename.
   *
   * @deprecated Use {@link #renameTo(String, String, Options.Rename...)}
   * instead.
   */
  @Deprecated
  boolean renameTo(final String src, final String dst, final long srcNsCount,
      final long srcDsCount, final long dstNsCount, final long dstDsCount,
         final boolean isUsingSubTreeLocks, final String subTreeLockDst,
         final Collection<MetadataLogEntry> logEntries)
      throws IOException, UnresolvedLinkException {
    HopsTransactionalRequestHandler renameToHandler =
        new HopsTransactionalRequestHandler(
            isUsingSubTreeLocks ? HDFSOperationType.SUBTREE_DEPRICATED_RENAME :
            HDFSOperationType.DEPRICATED_RENAME
            , src) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getLegacyRenameINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode,
                INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                INodeResolveType.PATH, true, src, dst))
                .add(lf.getBlockLock())
                .add(lf.getBlockRelated(BLK.RE, BLK.UC, BLK.IV, BLK.CR, BLK.ER,
                    BLK.PE, BLK.UR));
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

            if (isInSafeMode()) {
              throw new SafeModeException("Cannot rename " + src, safeMode);
            }
            if (!DFSUtil.isValidName(dst)) {
              throw new IOException("Invalid name: " + dst);
            }

            // remove the subtree locks
            removeSubTreeLocksForRenameInternal(src, isUsingSubTreeLocks, 
                    subTreeLockDst);

            for (MetadataLogEntry logEntry : logEntries) {
              EntityManager.add(logEntry);
            }
            
            return dir.renameTo(src, dst, srcNsCount, srcDsCount, dstNsCount,
                dstDsCount);
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
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog
              .debug("DIR* NameSystem.multiTransactionalDelete: " + path);
    }

    boolean ret;
    try {
      ret = multiTransactionalDeleteInternal(path, recursive);
      logAuditEvent(ret, "delete", path);
    } catch (IOException e) {
      logAuditEvent(false, "delete", path);
      throw e;
    }
    return ret;
  }

  private boolean multiTransactionalDeleteInternal(final String path,
          final boolean recursive) throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot delete " + path, safeMode);
    }

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
      return deleteWithTransaction(path, recursive);
    } else {
      //sub tree operation
      try {
        subtreeRoot = lockSubtreeAndCheckPathPermission(path, false, null,
                FsAction.WRITE, null, null, 
                SubTreeOperation.StoOperationType.DELETE_STO);

        AbstractFileTree.FileTree fileTree =
                new AbstractFileTree.FileTree(this, subtreeRoot, FsAction.ALL);
        fileTree.buildUp();

        if (dir.isQuotaEnabled()) {
          Iterator<Integer> idIterator =
                  fileTree.getAllINodesIds().iterator();
          synchronized (idIterator) {
            quotaUpdateManager.addPrioritizedUpdates(idIterator);
            try {
              idIterator.wait();
            } catch (InterruptedException e) {
              // Not sure if this can happend if we are not shutting down but we need to abort in case it happens.
              throw new IOException("Operation failed due to an Interrupt");
            }
          }
        }

        for (int i = fileTree.getHeight(); i > 0; i--) {
          if (deleteTreeLevel(path, fileTree, i) == false) {
            return false;
          }
        }
      } finally {
        if(subtreeRoot != null){
          unlockSubtree(path);
        }
      }
      return true;
    }
  }

  private boolean deleteTreeLevel(final String subtreeRootPath,
      final AbstractFileTree.FileTree fileTree, int level) {
    ArrayList<Future> barrier = new ArrayList<Future>();

     for (final ProjectedINode dir : fileTree.getDirsByLevel(level)) {
       if (fileTree.countChildren(dir.getId()) <= BIGGEST_DELETEABLE_DIR) {
         final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
         Future f = multiTransactionDeleteInternal(path);
         barrier.add(f);
       } else {
         //delete the content of the direcotry one by one.
         for (final ProjectedINode inode : fileTree.getChildren(dir.getId())) {
           if(!inode.isDirectory()) {
             final String path = fileTree.createAbsolutePath(subtreeRootPath, inode);
             Future f = multiTransactionDeleteInternal(path);
             barrier.add(f);
           }
         }
         // the dir is empty now. delete it.
         final String path = fileTree.createAbsolutePath(subtreeRootPath, dir);
         Future f = multiTransactionDeleteInternal(path);
         barrier.add(f);
       }
     }

    boolean result = true;
    for (Future f : barrier) {
      try {
        if (((Boolean) f.get()) == false) {
          result = false;
        }
      } catch (Exception e) {
        result = false;
        LOG.error("Exception was thrown during partial delete", e);
      }
    }
    return result;
  }

  private Future multiTransactionDeleteInternal(final String path){
   return  subtreeOperationsExecutor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          HopsTransactionalRequestHandler deleteHandler =
                  new HopsTransactionalRequestHandler(HDFSOperationType.SUBTREE_DELETE) {
                    @Override
                    public void acquireLock(TransactionLocks locks)
                            throws IOException {
                      LockFactory lf = LockFactory.getInstance();
                      locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode,
                              INodeLockType.WRITE_ON_TARGET_AND_PARENT,
                              INodeResolveType.PATH_AND_ALL_CHILDREN_RECURSIVELY, false, true, path))
                              .add(lf.getLeaseLock(LockType.WRITE))
                              .add(lf.getLeasePathLock(LockType.READ_COMMITTED))
                              .add(lf.getBlockLock()).add(
                              lf.getBlockRelated(BLK.RE, BLK.CR, BLK.UC, BLK.UR, BLK.PE,
                                      BLK.IV));
                      if (dir.isQuotaEnabled()) {
                        locks.add(lf.getQuotaUpdateLock(true, path));
                      }
                      if (erasureCodingEnabled) {
                        locks.add(lf.getEncodingStatusLock(true,LockType.WRITE, path));
                      }
                    }

                    @Override
                    public Object performTask() throws IOException {
                      return deleteInternal(path,true,false);
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
  INodeIdentifier lockSubtree(final String path, SubTreeOperation.StoOperationType stoType) throws IOException {
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
   *    the requested subaccess
   * @return
   *  the inode representing the root of the subtree
   * @throws IOException
   */
  @VisibleForTesting
  INodeIdentifier lockSubtreeAndCheckPathPermission(final String path,
      final boolean doCheckOwner, final FsAction ancestorAccess,
      final FsAction parentAccess, final FsAction access,
      final FsAction subAccess,
      final SubTreeOperation.StoOperationType stoType) throws IOException {

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
        locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/, nameNode, INodeLockType
            .WRITE,
            INodeResolveType.PATH, false, path)).
                //READ_COMMITTED because it is index scan and locking is bad idea
                //INode lock is sufficient
                add(lf.getSubTreeOpsLock(LockType.READ_COMMITTED, 
                getSubTreeLockPathPrefix(path))); // it is 
        
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        if (isPermissionEnabled && !pc.isSuperUser()) {
          pc.checkPermission(path, dir.getRootDir(), doCheckOwner,
              ancestorAccess, parentAccess, access, subAccess);
        }

        INode[] nodes = dir.getRootDir().getExistingPathINodes(path, false);
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
          return  iNodeIdentifier;
        }else{
          if(LOG.isInfoEnabled()) {
            LOG.info("No componenet was locked in the path using sub tree flag. "
                    + "Path: \"" + path + "\"");
          }
          return null;
        }
      }
    }.handle(this);
  }
  
  /**
   * adds / at the end of the path
   * suppose /aa/bb is locked and we want to lock an other foler /a. 
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
  private void checkSubTreeLocks(String path) throws TransactionContextException, StorageException{
      List<SubTreeOperation> ops = (List<SubTreeOperation>)
              EntityManager.findList(SubTreeOperation.Finder.ByPathPrefix, 
              path);  // THIS RETURNS ONLY ONE SUBTREE OP IN THE CHILD TREE. INCREASE THE LIMIT IN IMPL LAYER IF NEEDED
      Set<Long> activeNameNodeIds = new HashSet<Long>();
      for(ActiveNode node:nameNode.getActiveNameNodes().getActiveNodes()){
        activeNameNodeIds.add(node.getId());
      }
      
      for(SubTreeOperation op : ops){
        if(activeNameNodeIds.contains(op.getNameNodeId())){
          throw new SubtreeLockedException("There is atleat one on going subtree operation "
                  + "on the decendents. Path: "+op.getPath()
                  +" Operation "+op.getOpType()+" NameNodeId "+op.getNameNodeId());
          
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
  void unlockSubtree(final String path) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.RESET_SUBTREE_LOCK) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/,nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, false, true, path));
      }

      @Override
      public Object performTask() throws IOException {
        INode[] nodes = dir.getRootDir().getExistingPathINodes(path, false);
        INode inode = nodes[nodes.length - 1];
        if (inode != null && inode.isSubtreeLocked()) {
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
            locks.add(lf.getINodeLock(nameNode, INodeLockType.READ_COMMITTED,
                INodeResolveType.PATH, filePath)).add(
                lf.getEncodingStatusLock(LockType.READ_COMMITTED, filePath));
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
  public String getPath(int id) throws IOException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();
    boolean resovled[] = new boolean[1];
    INodeUtil.findPathINodesById(id, resolvedInodes, resovled);

    if (resovled[0] == false) {
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
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {

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
   * Add and encoding status for a file.
   *
   * @param sourcePath
   *    the file path
   * @param policy
   *    the policy to be used
   * @throws IOException
   */
  public void addEncodingStatus(final String sourcePath,
      final EncodingPolicy policy, final EncodingStatus.Status status)
      throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, sourcePath));
        locks.add(lf.getEncodingStatusLock(LockType.WRITE, sourcePath));
      }

      @Override
      public Object performTask() throws IOException {
        FSPermissionChecker pc = getPermissionChecker();
        try {
          if (isPermissionEnabled) {
            checkPathAccess(pc, sourcePath, FsAction.WRITE);
          }
        } catch (AccessControlException e){
          logAuditEvent(false, "encodeFile", sourcePath);
          throw e;
        }
        INode target = getINode(sourcePath);
        EncodingStatus existing = EntityManager.find(
            EncodingStatus.Finder.ByInodeId, target.getId());
        if (existing != null) {
          throw new IOException("Attempting to request encoding for an" +
              "encoded file");
        }
        int inodeId = dir.getINode(sourcePath).getId();
        EncodingStatus encodingStatus = new EncodingStatus(inodeId, status,
            policy, System.currentTimeMillis());
        EntityManager.add(encodingStatus);
        return null;
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
          public Object performTask() throws StorageException, IOException {
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
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, path))
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
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, filePath))
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
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, sourceFile))
            .add(lf.getEncodingStatusLock(LockType.WRITE, sourceFile));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
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
    INode[] inodes = dir.getExistingPathINodes(path);
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
        locks.add(lf.getINodeLock(nameNode, INodeLockType.WRITE,
            INodeResolveType.PATH, src));
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
        locks.add(
            lf.getINodeLock(nameNode, INodeLockType.READ, INodeResolveType.PATH,
                src)).add(lf.getBlockChecksumLock(src, blockIndex));
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


  /**
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#getRepairedBlockLocations
   */
  public LocatedBlock getRepairedBlockLocations(String clientMachine,
      String sourcePath, String parityPath, LocatedBlock block,
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

    HashMap<Node, Node> excluded = new HashMap<Node, Node>();
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
        excluded.put(node, node);
      }
    }

    // Exclude all nodes from the related parity blocks
    index = stripe * codec.getParityLength();
    for (int i = index; i < parityLocations.size()
        && i < index + codec.getParityLength(); i++) {
      DatanodeInfo[] nodes = parityLocations.get(i).getLocations();
      for (DatanodeInfo node : nodes) {
        excluded.put(node, node);
      }
    }

    BlockPlacementPolicyDefault placementPolicy = (BlockPlacementPolicyDefault)
        getBlockManager().getBlockPlacementPolicy();
    List<DatanodeDescriptor> chosenNodes = new LinkedList<DatanodeDescriptor>();
    DatanodeDescriptor[] descriptors = placementPolicy
        .chooseTarget(isParity ? parityPath : sourcePath,
            isParity ? 1 : status.getEncodingPolicy().getTargetReplication(),
            null, chosenNodes, false, excluded, block.getBlockSize());
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
            locks.add(lf.getINodeLock(!dir.isQuotaEnabled()?true:false/*skip INode Attr Lock*/,nameNode, INodeLockType.READ_COMMITTED,
                INodeResolveType.PATH, false, 
                path)).add(lf.getBlockLock()); // blk lock only if file
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            FSPermissionChecker pc = getPermissionChecker();
            if (isPermissionEnabled && !pc.isSuperUser()) {
              pc.checkPermission(path, dir.getRootDir(), doCheckOwner,
                      ancestorAccess, parentAccess, access, subAccess);
            }
        
            byte[][] pathComponents = INode.getPathComponents(path);
            INode[] pathInodes = new INode[pathComponents.length];
            boolean isDir = false;
            INode.DirCounts srcCounts  = new INode.DirCounts();
            int numExistingComp = dir.getRootDir().
                    getExistingPathINodes(pathComponents, pathInodes, false);
            
            if(pathInodes[pathInodes.length - 1] != null){  // complete path resolved
              if(pathInodes[pathInodes.length - 1] instanceof INodeFile ){
                isDir = false;
                //do ns and ds counts for file only
                pathInodes[pathInodes.length - 1].spaceConsumedInTree(srcCounts); 
              }else{
                isDir =true;
              }
            }

            return new PathInformation(path, pathComponents, 
                    pathInodes,numExistingComp,isDir, 
                    srcCounts.getNsCount(), srcCounts.getDsCount());
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
    
    public PathInformation(String path, 
            byte[][] pathComponents, INode[] pathInodes, int numExistingComp, 
            boolean dir, long nsCount, long dsCount) {
      this.path = path;
      this.pathComponents = pathComponents;
      this.pathInodes = pathInodes;
      this.dir = dir;
      this.nsCount = nsCount;
      this.dsCount = dsCount;
      this.numExistingComp = numExistingComp;
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
  }
  
  public ExecutorService getExecutorService(){
    return subtreeOperationsExecutor;
  }
}

