/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.annotations.VisibleForTesting;
import io.hops.security.Users;
import io.hops.exception.StorageException;
import io.hops.leaderElection.HdfsLeDescriptorFactory;
import io.hops.leaderElection.LeaderElection;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BRTrackingService;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.BRLoadBalancingException;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.ssl.RevocationListFetcherService;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import javax.management.ObjectName;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.FS_TRASH_INTERVAL_KEY;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressMetrics;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * ********************************************************
 * NameNode serves as both directory namespace manager and "inode table" for
 * the
 * Hadoop DFS. There is a single NameNode running in any DFS deployment. (Well,
 * except when there is a second backup/failover NameNode, or when using
 * federated NameNodes.)
 * <p/>
 * The NameNode controls two critical tables: 1) filename->blocksequence
 * (namespace) 2) block->machinelist ("inodes")
 * <p/>
 * The first table is stored on disk and is very precious. The second table is
 * rebuilt every time the NameNode comes up.
 * <p/>
 * 'NameNode' refers to both this class as well as the 'NameNode server'. The
 * 'FSNamesystem' class actually performs most of the filesystem management.
 * The
 * majority of the 'NameNode' class itself is concerned with exposing the IPC
 * interface and the HTTP server to the outside world, plus some configuration
 * management.
 * <p/>
 * NameNode implements the
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} interface, which
 * allows clients to ask for DFS services.
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} is not designed for
 * direct use by authors of DFS client code. End-users should instead use the
 * {@link org.apache.hadoop.fs.FileSystem} class.
 * <p/>
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol} interface,
 * used by DataNodes that actually store DFS data blocks. These methods are
 * invoked repeatedly and automatically by all the DataNodes in a DFS
 * deployment.
 * <p/>
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol} interface,
 * used by secondary namenodes or rebalancing processes to get partial NameNode
 * state, for example partial blocksMap etc.
 * ********************************************************
 */
@InterfaceAudience.Private
public class NameNode implements NameNodeStatusMXBean {

  static {
    HdfsConfiguration.init();
  }

  /**
   * HDFS configuration can have three types of parameters:
   * <ol>
   * <li>Parameters that are common for all the name services in the
   * cluster.</li>
   * <li>Parameters that are specific to a name service. These keys are
   * suffixed with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * <li>Parameters that are specific to a single name node. These keys are
   * suffixed with nameserviceId and namenodeId in the configuration. for
   * example, "dfs.namenode.rpc-address.nameservice1.namenode1"</li>
   * </ol>
   * <p/>
   * In the latter cases, operators may specify the configuration without any
   * suffix, with a nameservice suffix, or with a nameservice and namenode
   * suffix. The more specific suffix will take precedence.
   * <p/>
   * These keys are specific to a given namenode, and thus may be configured
   * globally, for a nameservice, or for a specific namenode within a
   * nameservice.
   */
  public static final String[] NAMENODE_SPECIFIC_KEYS =
      {DFS_NAMENODE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_BIND_HOST_KEY, DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY, DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_KEYTAB_FILE_KEY,
          DFS_NAMENODE_USER_NAME_KEY, DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY};

  private static final String USAGE =
      "Usage: java NameNode [" + 
          //StartupOption.BACKUP.getName() + "] | [" +
          //StartupOption.CHECKPOINT.getName() + "] | [" +
          StartupOption.FORMAT.getName() + " [" +
          StartupOption.CLUSTERID.getName() + " cid ] | [" +
          StartupOption.FORCE.getName() + "] [" +
          StartupOption.NONINTERACTIVE.getName() + "] ] | [" +
          //StartupOption.UPGRADE.getName() + "] | [" +
          //StartupOption.ROLLBACK.getName() + "] | [" +
          //StartupOption.FINALIZE.getName() + "] | [" +
          //StartupOption.IMPORT.getName() + "] | [" +
          //StartupOption.INITIALIZESHAREDEDITS.getName() + "] | [" +
          //StartupOption.BOOTSTRAPSTANDBY.getName() + "] | [" +
          //StartupOption.RECOVER.getName() + " [ " +
          //StartupOption.FORCE.getName() + " ] ] | [ "+
          StartupOption.SET_BLOCK_REPORT_PROCESS_SIZE.getName() + " noOfBlks ] | [" +
          StartupOption.FORMAT_ALL.getName() + " ] | [" +
          StartupOption.DROP_AND_CREATE_DB.getName() + "]" ;

  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID;
    } else if (protocol.equals(DatanodeProtocol.class.getName())) {
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())) {
      return NamenodeProtocol.versionID;
    } else if (protocol
        .equals(RefreshAuthorizationPolicyProtocol.class.getName())) {
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())) {
      return RefreshUserMappingsProtocol.versionID;
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())) {
      return GetUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  public static final int DEFAULT_PORT = 8020;
  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog =
      LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
  public static final Log blockStateChangeLog =
      LogFactory.getLog("BlockStateChange");

  protected FSNamesystem namesystem;
  protected final Configuration conf;

  /**
   * httpServer
   */
  protected NameNodeHttpServer httpServer;
  private Thread emptier;
  /**
   * only used for testing purposes
   */
  protected boolean stopRequested = false;
  /**
   * Registration information of this name-node
   */
  protected NamenodeRegistration nodeRegistration;
  /**
   * Activated plug-ins.
   */
  private List<ServicePlugin> plugins;

  private NameNodeRpcServer rpcServer;


  protected LeaderElection leaderElection;
  
  protected RevocationListFetcherService revocationListFetcherService;
  /**
   * for block report load balancing
   */
  private BRTrackingService brTrackingService;

  private ObjectName nameNodeStatusBeanName;

  /**
   * Format a new filesystem. Destroys any filesystem that may already exist
   * at this location.  *
   */
  public static void format(Configuration conf) throws IOException {
    formatHdfs(conf, false, true);
  }

  static NameNodeMetrics metrics;
  private static final StartupProgress startupProgress = new StartupProgress();
  
  /**
   * Return the {@link FSNamesystem} object.
   *
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  public NamenodeProtocols getRpcServer() {
    return rpcServer;
  }

  static void initMetrics(Configuration conf, NamenodeRole role) {
    metrics = NameNodeMetrics.create(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return metrics;
  }

  /**
   * Returns object used for reporting namenode startup progress.
   *
   * @return StartupProgress for reporting namenode startup progress
   */
  public static StartupProgress getStartupProgress() {
    return startupProgress;
  }

  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }

  /**
   * Set the configuration property for the service rpc address to address
   */
  public static void setServiceAddress(Configuration conf, String address) {
    LOG.info("Setting ADDRESS " + address);
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
  }

  /**
   * Fetches the address for services to use when connecting to namenode based
   * on the value of fallback returns null if the special address is not
   * specified or returns the default namenode address to be used by both
   * clients and services. Services here are datanodes, backup node, any non
   * client connection
   */
  public static InetSocketAddress getServiceAddress(Configuration conf,
      boolean fallback) {
    String addr = conf.get(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return fallback ? getAddress(conf) : null;
    }
    return getAddress(addr);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    return getAddress(filesystemURI);
  }

  /**
   * TODO:FEDERATION
   *
   * @param filesystemURI
   * @return address of file system
   */
  public static InetSocketAddress getAddress(URI filesystemURI) {
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!HdfsConstants.HDFS_URI_SCHEME
        .equalsIgnoreCase(filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          HdfsConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":" + port);
    return URI.create(
        HdfsConstants.HDFS_URI_SCHEME + "://" + namenode.getHostName() +
            portString);
  }

  //
  // Common NameNode methods implementation for the active name-node role.
  //
  public NamenodeRole getRole() {
    if (leaderElection != null && leaderElection.isLeader()) {
      return NamenodeRole.LEADER_NAMENODE;
    }
    return NamenodeRole.NAMENODE;
  }

  boolean isRole(NamenodeRole that) {
    NamenodeRole currentRole = getRole();
    return currentRole.equals(that);
  }

  /**
   * Given a configuration get the address of the service rpc server If the
   * service rpc is not configured returns null
   */
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    return NameNode.getServiceAddress(conf, false);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    return getAddress(conf);
  }

  /** Given a configuration get the bind host of the service rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getServiceRpcServerBindHost(Configuration conf) {
    String addr = conf.getTrimmed(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }

  /** Given a configuration get the bind host of the client rpc server
   *  If the bind host is not configured returns null.
   */
  protected String getRpcServerBindHost(Configuration conf) {
    String addr = conf.getTrimmed(DFS_NAMENODE_RPC_BIND_HOST_KEY);
    if (addr == null || addr.isEmpty()) {
      return null;
    }
    return addr;
  }

  /**
   * Modifies the configuration passed to contain the service rpc address
   * setting
   */
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress serviceRPCAddress) {
    setServiceAddress(conf, NetUtils.getHostPortString(serviceRPCAddress));
  }

  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress rpcAddress) {
    FileSystem.setDefaultUri(conf, getUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return getHttpAddress(conf);
  }

  /**
   * @return the NameNode HTTP address
   */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(conf.get(DFS_NAMENODE_HTTP_ADDRESS_KEY,
            DFS_NAMENODE_HTTP_ADDRESS_DEFAULT));
  }

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = FSNamesystem.loadFromDisk(conf, this);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() throws IOException {
    nodeRegistration = new NamenodeRegistration(
        NetUtils.getHostPortString(rpcServer.getRpcAddress()),
        NetUtils.getHostPortString(getHttpAddress()),
        StorageInfo.getStorageInfoFromDB(),
        getRole());   //HOP change. previous code was getFSImage().getStorage()
    return nodeRegistration;
  }

  /* optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed
   */
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Login as the configured user for the NameNode.
   */
  void loginAsNameNodeUser(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    SecurityUtil
        .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_USER_NAME_KEY,
            socAddr.getHostName());
  }

  /**
   * Initialize name-node.
   *
   * @param conf
   *     the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
   if (conf.get(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS) == null) {
      String intervals = conf.get(DFS_METRICS_PERCENTILES_INTERVALS_KEY);
      if (intervals != null) {
        conf.set(HADOOP_USER_GROUP_METRICS_PERCENTILES_INTERVALS,
          intervals);
      }
    }
    
    UserGroupInformation.setConfiguration(conf);
    loginAsNameNodeUser(conf);
    
    HdfsStorageFactory.setConfiguration(conf);

    int baseWaitTime = conf.getInt(DFSConfigKeys.DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_KEY,
            DFSConfigKeys.DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_DEFAULT);
    int retryCount = conf.getInt(DFSConfigKeys.DFS_NAMENODE_TX_RETRY_COUNT_KEY,
            DFSConfigKeys.DFS_NAMENODE_TX_RETRY_COUNT_DEFAULT);
    RequestHandler.setRetryBaseWaitTime(baseWaitTime);
    RequestHandler.setRetryCount(retryCount);

    this.brTrackingService = new BRTrackingService(conf.getLong(DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD_DEFAULT),
            conf.getLong(DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE,
                    DFSConfigKeys.DFS_BR_LB_TIME_WINDOW_SIZE_DEFAULT));


    String fsOwnerShortUserName = UserGroupInformation.getCurrentUser()
        .getShortUserName();
    String superGroup = conf.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY,
        DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT);

    Users.addUserToGroupTx(fsOwnerShortUserName, superGroup);
  
    try {
      createAndStartCRLFetcherService(conf);
    } catch (Exception ex) {
      LOG.error("Error starting CRL fetcher service", ex);
      throw new IOException(ex);
    }
    
    NameNode.initMetrics(conf, this.getRole());
    StartupProgressMetrics.register(startupProgress);
    
    startHttpServer(conf);
    loadNamesystem(conf);

    rpcServer = createRpcServer(conf);
    httpServer.setNameNodeAddress(getNameNodeAddress());

    startCommonServices(conf);
  }

  /**
   * Create the RPC server implementation. Used as an extension point for the
   * BackupNode.
   */
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new NameNodeRpcServer(conf, this);
  }

  /**
   * Start the services common to active and standby states
   */
  private void startCommonServices(Configuration conf) throws IOException {
    startLeaderElectionService();
    
    namesystem.startCommonServices(conf);
    registerNNSMXBean();
    rpcServer.start();
    plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY, ServicePlugin.class);
    for (ServicePlugin p : plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
    LOG.info(getRole() + " RPC up at: " + rpcServer.getRpcAddress());
    if (rpcServer.getServiceRpcAddress() != null) {
      LOG.info(getRole() + " service RPC up at: " +
          rpcServer.getServiceRpcAddress());
    }
  }

  /**
   * Register NameNodeStatusMXBean
   */
  private void registerNNSMXBean() {
    nameNodeStatusBeanName = MBeans.register("NameNode", "NameNodeStatus", this);
  }

  @Override // NameNodeStatusMXBean
  public String getNNRole() {
    String roleStr = "";
    NamenodeRole role = getRole();
    if (null != role) {
      roleStr = role.toString();
    }
    return roleStr;
  }

  @Override // NameNodeStatusMXBean
  public String getHostAndPort() {
    return getNameNodeAddressHostPortString();
  }

  @Override // NameNodeStatusMXBean
  public boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  private void stopCommonServices() {
    if (namesystem != null) {
      namesystem.close();
    }
    if (rpcServer != null) {
      rpcServer.stop();
    }
    if (leaderElection != null && leaderElection.isRunning()) {
      leaderElection.stopElectionThread();
    }
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    
    if (revocationListFetcherService != null) {
      try {
        revocationListFetcherService.serviceStop();
      } catch (Exception ex) {
        LOG.warn("Exception while stopping CRL fetcher service, but we are shutting down anyway");
      }
    }
    
    stopHttpServer();
  }

  private void startTrashEmptier(final Configuration conf) throws IOException {
    long trashInterval =
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    if (trashInterval == 0) {
      return;
    } else if (trashInterval < 0) {
      throw new IOException(
          "Cannot start trash emptier with negative interval." + " Set " +
              FS_TRASH_INTERVAL_KEY + " to a positive value.");
    }

    // This may be called from the transitionToActive code path, in which
    // case the current user is the administrator, not the NN. The trash
    // emptier needs to run as the NN. See HDFS-3972.
    FileSystem fs =
        SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<FileSystem>() {
              @Override
              public FileSystem run() throws IOException {
                return FileSystem.get(conf);
              }
            });
    this.emptier =
        new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void stopTrashEmptier() {
    if (this.emptier != null) {
      emptier.interrupt();
      emptier = null;
    }
  }

  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new NameNodeHttpServer(conf, this, getHttpServerAddress(conf));
    httpServer.start();
    httpServer.setStartupProgress(startupProgress);
  }

  private void stopHttpServer() {
    try {
      if (httpServer != null) {
        httpServer.stop();
      }
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
    }
  }

  /**
   * Start NameNode.
   * <p/>
   * The name-node can be started with one of the following startup options:
   * <ul>
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * @param conf
   *     confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    this(conf, NamenodeRole.NAMENODE);
  }

  protected NameNode(Configuration conf, NamenodeRole role) throws IOException {
    this.conf = conf;
    try {
      initializeGenericKeys(conf);
      initialize(conf);
      enterActiveState();
    } catch (IOException | HadoopIllegalArgumentException e) {
      this.stop();
      throw e;
    }
  }


  /**
   * Wait for service to finish. (Normally, it runs forever.)
   */
  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception ", ie);
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    synchronized (this) {
      if (stopRequested) {
        return;
      }
      stopRequested = true;
    }
    try {
      exitActiveServices();
    } catch (ServiceFailedException e) {
      LOG.warn("Encountered exception while exiting state ", e);
    } finally {
      stopCommonServices();
      if (metrics != null) {
        metrics.shutdown();
      }
      if (namesystem != null) {
        namesystem.shutdown();
      }
      if (nameNodeStatusBeanName != null) {
        MBeans.unregister(nameNodeStatusBeanName);
        nameNodeStatusBeanName = null;
      }
    }
  }
  

  synchronized boolean isStopRequested() {
    return stopRequested;
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() throws IOException {
    return namesystem.isInSafeMode();
  }

  /**
   * @return NameNode RPC address
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcServer.getRpcAddress();
  }

  /**
   * @return NameNode RPC address in "host:port" string form
   */
  public String getNameNodeAddressHostPortString() {
    return NetUtils.getHostPortString(rpcServer.getRpcAddress());
  }

  /**
   * @return NameNode service RPC address if configured, the NameNode RPC
   * address otherwise
   */
  public InetSocketAddress getServiceRpcAddress() {
    final InetSocketAddress serviceAddr = rpcServer.getServiceRpcAddress();
    return serviceAddr == null ? rpcServer.getRpcAddress() : serviceAddr;
  }

  /**
   * @return NameNode HTTP address, used by the Web UI, image transfer, and
   * HTTP-based file system clients like Hftp and WebHDFS
   */
  public InetSocketAddress getHttpAddress() {
    return httpServer.getHttpAddress();
  }
  
  public InetSocketAddress getHttpsAddress() {
    return httpServer.getHttpsAddress();
  }

  /**
   * Verify that configured directories exist, then Interactively confirm that
   * formatting is desired for each existing directory and format them.
   *
   * @param conf
   * @param force
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean formatHdfs(Configuration conf, boolean force,
      boolean isInteractive) throws IOException {
    initializeGenericKeys(conf);
    checkAllowFormat(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil
          .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_USER_NAME_KEY,
              socAddr.getHostName());
    }

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if (clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = StorageInfo.newClusterID();
    }
    System.out.println("Formatting using clusterid: " + clusterId);

    try {
      HdfsStorageFactory.setConfiguration(conf);
      if (force) {
        HdfsStorageFactory.formatHdfsStorageNonTransactional();
      } else {
        HdfsStorageFactory.formatHdfsStorage();
      }
      StorageInfo
          .storeStorageInfoToDB(clusterId);  //this adds new row to the db
    } catch (StorageException e) {
      throw new RuntimeException(e.getMessage());
    }

    return false;
  }

  @VisibleForTesting
  public static boolean formatAll(Configuration conf) throws IOException {
    System.out.println("Formatting HopsFS and HopsYarn");
    initializeGenericKeys(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil
              .login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY, DFS_NAMENODE_USER_NAME_KEY,
                      socAddr.getHostName());
    }

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if (clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = StorageInfo.newClusterID();
    }

    try {
      HdfsStorageFactory.setConfiguration(conf);
//      HdfsStorageFactory.formatAllStorageNonTransactional();
      HdfsStorageFactory.formatStorage();
      StorageInfo.storeStorageInfoToDB(clusterId);  //this adds new row to the db
    } catch (StorageException e) {
      throw new RuntimeException(e.getMessage());
    }

    return false;
  }

  public static void checkAllowFormat(Configuration conf) throws IOException {
    if (!conf.getBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY,
        DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT)) {
      throw new IOException(
          "The option " + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY +
              " is set to false for this filesystem, so it " +
              "cannot be formatted. You will need to set " +
              DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY + " parameter " +
              "to true in order to format this filesystem");
    }
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.SET_BLOCK_REPORT_PROCESS_SIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.SET_BLOCK_REPORT_PROCESS_SIZE;
        String msg = "Specify a maximum number of blocks that the NameNodes can process for block reporting.";
            if ((i + 1) >= argsLen) {
              // if no of blks not specified then return null
              
              LOG.fatal(msg);
              return null;
            }
            // Make sure an id is specified and not another flag
            long noOfBlks = 0;
            try{
              noOfBlks = Long.parseLong(args[i+1]);
              if(noOfBlks < 100000){
                LOG.fatal("The number should be >= 100K. ");
              return null;
              }
            }catch(NumberFormatException e){
              return null;
            }
            startOpt.setMaxBlkRptProcessSize(noOfBlks);
            return startOpt;
      }
      
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i >= argsLen) {
              // if no cluster id specified, return null
              LOG.fatal("Must specify a valid cluster ID after the " +
                  StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            String clusterId = args[i];
            // Make sure an id is specified and not another flag
            if (clusterId.isEmpty() ||
                clusterId.equalsIgnoreCase(StartupOption.FORCE.getName()) ||
                clusterId
                    .equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
              LOG.fatal("Must specify a valid cluster ID after the " +
                  StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            startOpt.setClusterId(clusterId);
          }
          
          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForceFormat(true);
          }

          if (args[i]
              .equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
            startOpt.setInteractiveFormat(false);
          }
        }
      } else if (StartupOption.DROP_AND_CREATE_DB.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.DROP_AND_CREATE_DB;
      } else if (StartupOption.FORMAT_ALL.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT_ALL;
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.GENCLUSTERID;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
        // might be followed by two args
        if (i + 2 < argsLen &&
            args[i + 1].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
          i += 2;
          startOpt.setClusterId(args[i]);
        }
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else if (StartupOption.BOOTSTRAPSTANDBY.getName()
          .equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BOOTSTRAPSTANDBY;
        return startOpt;
      } else if (StartupOption.INITIALIZESHAREDEDITS.getName()
          .equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INITIALIZESHAREDEDITS;
        for (i = i + 1; i < argsLen; i++) {
          if (StartupOption.NONINTERACTIVE.getName().equals(args[i])) {
            startOpt.setInteractiveFormat(false);
          } else if (StartupOption.FORCE.getName().equals(args[i])) {
            startOpt.setForceFormat(true);
          } else {
            LOG.fatal("Invalid argument: " + args[i]);
            return null;
          }
        }
        return startOpt;
      } else if (StartupOption.RECOVER.getName().equalsIgnoreCase(cmd)) {
        if (startOpt != StartupOption.REGULAR) {
          throw new RuntimeException(
              "Can't combine -recover with " + "other startup options.");
        }
        startOpt = StartupOption.RECOVER;
        while (++i < argsLen) {
          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForce(MetaRecoveryContext.FORCE_FIRST_CHOICE);
          } else {
            throw new RuntimeException("Error parsing recovery options: " +
                "can't understand option \"" + args[i] + "\"");
          }
        }
      } else {
        return null;
      }
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_NAMENODE_STARTUP_KEY, opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(
        conf.get(DFS_NAMENODE_STARTUP_KEY, StartupOption.REGULAR.toString()));
  }


  public static NameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      return null;
    }
    setStartupOption(conf, startOpt);

    switch (startOpt) {
      //HOP
      case DROP_AND_CREATE_DB: { //delete everything other than inode and blocks table. this is tmp fix for safe mode
        dropAndCreateDB(conf);
        return null;
      }
      case SET_BLOCK_REPORT_PROCESS_SIZE:
        HdfsVariables.setBrLbMasBlkPerMin(startOpt.getMaxBlkRptProcessSize());
        LOG.info("Set block processing size to "+startOpt.getMaxBlkRptProcessSize());
        return null;
      case FORMAT: {
        boolean aborted = formatHdfs(conf, startOpt.getForceFormat(),
            startOpt.getInteractiveFormat());
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case FORMAT_ALL: {
        boolean aborted = formatAll(conf);
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case GENCLUSTERID: {
        System.err.println("Generating new cluster id:");
        System.out.println(StorageInfo.newClusterID());
        terminate(0);
        return null;
      }
      case FINALIZE: {
        throw new UnsupportedOperationException(
            "HOP: FINALIZE is not supported anymore");
      }
      case BOOTSTRAPSTANDBY: {
        throw new UnsupportedOperationException(
            "HOP: BOOTSTRAPSTANDBY is not supported anymore");
      }
      case INITIALIZESHAREDEDITS: {
        throw new UnsupportedOperationException(
            "HOP: INITIALIZESHAREDEDITS is not supported anymore");
      }
      case BACKUP:
      case CHECKPOINT: {
        throw new UnsupportedOperationException(
            "HOP: BACKUP/CHECKPOINT is not supported anymore");
      }
      case RECOVER: {
        new UnsupportedOperationException(
            "Hops. Metadata recovery is not supported");
        return null;
      }
      default: {
        DefaultMetricsSystem.initialize("NameNode");
        return new NameNode(conf);
      }
    }
  }

  public static void initializeGenericKeys(Configuration conf) {
    // If the RPC address is set use it to (re-)configure the default FS
    if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" +
          conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      LOG.debug(
          "Setting " + FS_DEFAULT_NAME_KEY + " to " + defaultUri.toString());
    }
  }

  /**
   */
  public static void main(String argv[]) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, NameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null) {
        namenode.join();
      }
    } catch (Throwable e) {
      LOG.fatal("Exception in namenode join", e);
      terminate(1, e);
    }
  }

  private void enterActiveState() throws ServiceFailedException {
    try {
      startActiveServicesInternal();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to start active services", e);
    }
  }

  private void startActiveServicesInternal() throws IOException {
    try {
      namesystem.startActiveServices();
      startTrashEmptier(conf);
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }

  private void exitActiveServices() throws ServiceFailedException {
    try {
      stopActiveServicesInternal();
    } catch (IOException e) {
      throw new ServiceFailedException("Failed to stop active services", e);
    }
  }

  private void stopActiveServicesInternal() throws IOException {
    try {
      if (namesystem != null) {
        namesystem.stopActiveServices();
      }
      stopTrashEmptier();
    } catch (Throwable t) {
      doImmediateShutdown(t);
    }
  }


  /**
   * Shutdown the NN immediately in an ungraceful way. Used when it would be
   * unsafe for the NN to continue operating, e.g. during a failed HA state
   * transition.
   *
   * @param t
   *     exception which warrants the shutdown. Printed to the NN log
   *     before exit.
   * @throws ExitException
   *     thrown only for testing.
   */
  protected synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring NN shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.fatal(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }

  /**
   * Returns the id of this namenode
   */
  public long getId() {
    return leaderElection.getCurrentId();
  }

  /**
   * Return the {@link LeaderElection} object.
   *
   * @return {@link LeaderElection} object.
   */
  public LeaderElection getLeaderElectionInstance() {
    return leaderElection;
  }

  public boolean isLeader() {
    if (leaderElection != null) {
      return leaderElection.isLeader();
    } else {
      return false;
    }
  }

  public ActiveNode getNextNamenodeToSendBlockReport(final long noOfBlks) throws IOException {
    if(leaderElection.isLeader()) {
      LOG.debug("NN Id: "+leaderElection.getCurrentId()+") Received request to assign work ("+noOfBlks+" blks) ");
      ActiveNode an = brTrackingService.assignWork(leaderElection.getActiveNamenodes(), noOfBlks);
      return an;
    }else{
      String msg = "NN Id: "+leaderElection.getCurrentId()+") Received request to assign work ("+noOfBlks+" blks). Returning null as I am not the leader NN";
      LOG.debug(msg);
      throw new BRLoadBalancingException(msg);
    }
  }

  private static void dropAndCreateDB(Configuration conf) throws IOException {
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.getConnector().dropAndRecreateDB();
  }

  public boolean isNameNodeAlive(long namenodeId) {
    List<ActiveNode> activeNamenodes = getActiveNameNodes().getActiveNodes();
    return isNameNodeAlive(activeNamenodes, namenodeId);
  }

  public static boolean isNameNodeAlive(Collection<ActiveNode> activeNamenodes,
      long namenodeId) {
    if (activeNamenodes == null) {
      // We do not know yet, be conservative
      return true;
    }

    for (ActiveNode namenode : activeNamenodes) {
      if (namenode.getId() == namenodeId) {
        return true;
      }
    }
    return false;
  }

  public long getLeCurrentId() {
    return leaderElection.getCurrentId();
  }

  public SortedActiveNodeList getActiveNameNodes() {
    return leaderElection.getActiveNamenodes();
  }
  
  private void startLeaderElectionService() throws IOException {
    // Initialize the leader election algorithm (only once rpc server is
    // created and httpserver is started)
    long leadercheckInterval =
        conf.getInt(DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
            DFSConfigKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
    int missedHeartBeatThreshold =
        conf.getInt(DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_KEY,
            DFSConfigKeys.DFS_LEADER_MISSED_HB_THRESHOLD_DEFAULT);
    int leIncrement = conf.getInt(DFSConfigKeys.DFS_LEADER_TP_INCREMENT_KEY,
        DFSConfigKeys.DFS_LEADER_TP_INCREMENT_DEFAULT);

    String rpcAddresses = "";
    rpcAddresses = rpcServer.getRpcAddress().getAddress().getHostAddress() + ":" +rpcServer.getRpcAddress().getPort()+",";
    if(rpcServer.getServiceRpcAddress() != null){
      rpcAddresses = rpcAddresses + rpcServer.getServiceRpcAddress().getAddress().getHostAddress() + ":" +
              rpcServer.getServiceRpcAddress().getPort();
    }

    String httpAddress;
    if (DFSUtil.getHttpPolicy(conf).isHttpEnabled()) {
      httpAddress = httpServer.getHttpAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpAddress()
          .getPort() ;
    } else {
      httpAddress = httpServer.getHttpsAddress().getAddress().getHostAddress() + ":" + httpServer.getHttpsAddress()
          .getPort() ;
    }
    
    leaderElection =
        new LeaderElection(new HdfsLeDescriptorFactory(), leadercheckInterval,
            missedHeartBeatThreshold, leIncrement, httpAddress,
            rpcAddresses);
    leaderElection.start();

    try {
      leaderElection.waitActive();
    } catch (InterruptedException e) {
      LOG.warn("NN was interrupted");
    }
  }
  
  private void createAndStartCRLFetcherService(Configuration conf) throws Exception {
    if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      if (conf.getBoolean(CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_KEY,
          CommonConfigurationKeysPublic.HOPS_CRL_VALIDATION_ENABLED_DEFAULT)) {
        LOG.info("Creating CertificateRevocationList Fetcher service");
        revocationListFetcherService = new RevocationListFetcherService();
        revocationListFetcherService.serviceInit(conf);
        revocationListFetcherService.serviceStart();
      } else {
        LOG.warn("RPC TLS is enabled but CRL validation is disabled");
      }
    }
  }
}

