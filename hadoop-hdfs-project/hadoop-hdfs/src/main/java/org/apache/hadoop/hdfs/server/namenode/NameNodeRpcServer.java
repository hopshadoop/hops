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

import com.google.protobuf.BlockingService;
import io.hops.exception.ForeignKeyConstraintViolationException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.security.Users;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
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
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshAuthorizationPolicyProtocolService;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MAX_PATH_LENGTH;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.tukaani.xz.UnsupportedOptionsException;

/**
 * This class is responsible for handling all of the RPC calls to the NameNode.
 * It is created, started, and stopped by {@link NameNode}.
 */
class NameNodeRpcServer implements NamenodeProtocols {
  
  private static final Log LOG = NameNode.LOG;
  private static final Log stateChangeLog = NameNode.stateChangeLog;
  private static final Log blockStateChangeLog = NameNode.blockStateChangeLog;
  
  // Dependencies from other parts of NN.
  protected final FSNamesystem namesystem;
  protected final NameNode nn;
  private final NameNodeMetrics metrics;
  
  private final boolean serviceAuthEnabled;

  /**
   * The RPC server that listens to requests from DataNodes
   */
  private final RPC.Server serviceRpcServer;
  private final InetSocketAddress serviceRPCAddress;
  
  /**
   * The RPC server that listens to requests from clients
   */
  protected final RPC.Server clientRpcServer;
  protected final InetSocketAddress clientRpcAddress;
  
  private final String minimumDataNodeVersion;

  public NameNodeRpcServer(Configuration conf, NameNode nn) throws IOException {
    this.nn = nn;
    this.namesystem = nn.getNamesystem();
    this.metrics = NameNode.getNameNodeMetrics();

    int handlerCount = conf.getInt(DFS_NAMENODE_HANDLER_COUNT_KEY,
        DFS_NAMENODE_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine.class);

    ClientNamenodeProtocolServerSideTranslatorPB
        clientProtocolServerTranslator =
        new ClientNamenodeProtocolServerSideTranslatorPB(this);
    BlockingService clientNNPbService = ClientNamenodeProtocol.
        newReflectiveBlockingService(clientProtocolServerTranslator);
    
    DatanodeProtocolServerSideTranslatorPB dnProtoPbTranslator =
        new DatanodeProtocolServerSideTranslatorPB(this);
    BlockingService dnProtoPbService = DatanodeProtocolService
        .newReflectiveBlockingService(dnProtoPbTranslator);

    NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator =
        new NamenodeProtocolServerSideTranslatorPB(this);
    BlockingService NNPbService = NamenodeProtocolService
        .newReflectiveBlockingService(namenodeProtocolXlator);

    RefreshAuthorizationPolicyProtocolServerSideTranslatorPB
        refreshAuthPolicyXlator =
        new RefreshAuthorizationPolicyProtocolServerSideTranslatorPB(this);
    BlockingService refreshAuthService =
        RefreshAuthorizationPolicyProtocolService
            .newReflectiveBlockingService(refreshAuthPolicyXlator);

    RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator =
        new RefreshUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService refreshUserMappingService =
        RefreshUserMappingsProtocolService
            .newReflectiveBlockingService(refreshUserMappingXlator);

    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator =
        new GetUserMappingsProtocolServerSideTranslatorPB(this);
    BlockingService getUserMappingService = GetUserMappingsProtocolService
        .newReflectiveBlockingService(getUserMappingXlator);

    WritableRpcEngine.ensureInitialized();

    InetSocketAddress serviceRpcAddr = nn.getServiceRpcServerAddress(conf);
    if (serviceRpcAddr != null) {
      int serviceHandlerCount =
          conf.getInt(DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
              DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      this.serviceRpcServer = new RPC.Builder(conf).setProtocol(
          org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
          .setInstance(clientNNPbService)
          .setBindAddress(serviceRpcAddr.getHostName())
          .setPort(serviceRpcAddr.getPort()).setNumHandlers(serviceHandlerCount)
          .setVerbose(false)
          .setSecretManager(namesystem.getDelegationTokenSecretManager())
          .build();

      // Add all the RPC protocols that the namenode implements
      DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
          serviceRpcServer);
      DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
          serviceRpcServer);
      DFSUtil.addPBProtocol(conf, RefreshAuthorizationPolicyProtocolPB.class,
          refreshAuthService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class,
          refreshUserMappingService, serviceRpcServer);
      DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class,
          getUserMappingService, serviceRpcServer);

      serviceRPCAddress = serviceRpcServer.getListenerAddress();
      serviceRpcServer.setThreadNamePrefix("Service");
      nn.setRpcServiceServerAddress(conf, serviceRPCAddress);
    } else {
      serviceRpcServer = null;
      serviceRPCAddress = null;
    }
    InetSocketAddress rpcAddr = nn.getRpcServerAddress(conf);
    this.clientRpcServer = new RPC.Builder(conf).setProtocol(
        org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService).setBindAddress(rpcAddr.getHostName())
        .setPort(rpcAddr.getPort()).setNumHandlers(handlerCount)
        .setVerbose(false)
        .setSecretManager(namesystem.getDelegationTokenSecretManager()).build();

    // Add all the RPC protocols that the namenode implements
    DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
        clientRpcServer);
    DFSUtil.addPBProtocol(conf, DatanodeProtocolPB.class, dnProtoPbService,
        clientRpcServer);
    DFSUtil.addPBProtocol(conf, RefreshAuthorizationPolicyProtocolPB.class,
        refreshAuthService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class,
        refreshUserMappingService, clientRpcServer);
    DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class,
        getUserMappingService, clientRpcServer);

    // set service-level authorization security policy
    if (serviceAuthEnabled =
        conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
            false)) {
      clientRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      if (serviceRpcServer != null) {
        serviceRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      }
    }

    // The rpc-server port can be ephemeral... ensure we have the correct info
    clientRpcAddress = clientRpcServer.getListenerAddress();
    clientRpcServer.setThreadNamePrefix("RPC");
    nn.setRpcServerAddress(conf, clientRpcAddress);
    
    minimumDataNodeVersion =
        conf.get(DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY,
            DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT);
    
     // Set terse exception whose stack trace won't be logged
    clientRpcServer.addTerseExceptions(SafeModeException.class,
        FileNotFoundException.class,
        HadoopIllegalArgumentException.class,
        FileAlreadyExistsException.class,
        InvalidPathException.class,
        ParentNotDirectoryException.class,
        UnresolvedLinkException.class,
        AlreadyBeingCreatedException.class,
        QuotaExceededException.class,
        RecoveryInProgressException.class,
        AccessControlException.class,
        InvalidToken.class);
  }
  
  /**
   * Start client and service RPC servers.
   */
  void start() {
    clientRpcServer.start();
    if (serviceRpcServer != null) {
      serviceRpcServer.start();
    }
  }
  
  /**
   * Wait until the RPC servers have shutdown.
   */
  void join() throws InterruptedException {
    clientRpcServer.join();
    if (serviceRpcServer != null) {
      serviceRpcServer.join();
    }
  }

  /**
   * Stop client and service RPC servers.
   */
  void stop() {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
    if (serviceRpcServer != null) {
      serviceRpcServer.stop();
    }
  }
  
  InetSocketAddress getServiceRpcAddress() {
    return serviceRPCAddress;
  }

  InetSocketAddress getRpcAddress() {
    return clientRpcAddress;
  }

  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    if (size <= 0) {
      throw new IllegalArgumentException(
          "Unexpected not positive size: " + size);
    }
    namesystem.checkSuperuserPrivilege();
    return namesystem.getBlockManager().getBlocks(datanode, size);
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    namesystem.checkSuperuserPrivilege();
    return namesystem.getBlockManager().getBlockKeys();
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namesystem.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return namesystem.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    namesystem.cancelDelegationToken(token);
  }
  
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, long offset, long length)
      throws IOException {
    metrics.incrGetBlockLocations();
    return namesystem
        .getBlockLocations(getClientMachine(), src, offset, length);
  }
  
  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
          "*DIR* NameNode.create: file " + src + " for " + clientName + " at " +
              clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException(
          "create: Pathname too long.  Limit " + MAX_PATH_LENGTH +
              " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    HdfsFileStatus stat = namesystem.startFile(src, new PermissionStatus(
            UserGroupInformation.getCurrentUser().getShortUserName(), null,
            masked), clientName, clientMachine, flag.get(), createParent,
        replication, blockSize);
    metrics.incrFilesCreated();
    metrics.incrCreateFileOps();
    return stat;
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
          "*DIR* NameNode.append: file " + src + " for " + clientName + " at " +
              clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    metrics.incrFilesAppended();
    return info;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName)
      throws IOException {
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication)
      throws IOException {
    return namesystem.setReplication(src, replication);
  }

  @Override
  public void setStoragePolicy(String src, String policyName)
      throws IOException {
    namesystem.setStoragePolicy(src, policyName);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(byte storagePolicyID) throws IOException {
    return namesystem.getStoragePolicy(storagePolicyID);
  }
  
  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    return namesystem.getStoragePolicies();
  }

  @Override // ClientProtocol
  public void setMetaEnabled(String src, boolean metaEnabled)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException {
    namesystem.setMetaEnabled(src, metaEnabled);
  }

  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    namesystem.setPermissionSTO(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    try{
      namesystem.setOwnerSTO(src, username, groupname);
    }catch (ForeignKeyConstraintViolationException ex){
      LOG.debug("setOwner: cache is outdated, flush old values and restart " +
          "the operation - " + ex);
      Users.flushCache(username, groupname);
      namesystem.setOwnerSTO(src, username, groupname);
    }
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src, String clientName,
      ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId, String[] favoredNodes) throws IOException {

    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
          "*BLOCK* NameNode.addBlock: file " + src + " fileId=" + fileId + " for " + clientName);
    }
    HashSet<Node> excludedNodesSet = null;

    if (excludedNodes != null) {
      excludedNodesSet = new HashSet<>(excludedNodes.length);
      for (Node node : excludedNodes) {
        excludedNodesSet.add(node);
      }
    }
    List<String> favoredNodesList = (favoredNodes == null) ? null
        : Arrays.asList(favoredNodes);
    LocatedBlock locatedBlock = namesystem
        .getAdditionalBlock(src, fileId, clientName, previous, excludedNodesSet, favoredNodesList);
    if (locatedBlock != null) {
      metrics.incrAddBlockOps();
    }
    return locatedBlock;
  }

  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src,
      final ExtendedBlock blk,
      final DatanodeInfo[] existings,
      final String[] existingStorageIDs,
      final DatanodeInfo[] excludes,
      final int numAdditionalNodes,
      final String clientName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAdditionalDatanode: src=" + src
          + ", blk=" + blk
          + ", existings=" + Arrays.asList(existings)
          + ", excludes=" + Arrays.asList(excludes)
          + ", numAdditionalNodes=" + numAdditionalNodes
          + ", clientName=" + clientName);
    }

    metrics.incrGetAdditionalDatanodeOps();

    HashSet<Node> excludeSet = null;
    if (excludes != null) {
      excludeSet = new HashSet<>(excludes.length);
      for (Node node : excludes) {
        excludeSet.add(node);
      }
    }
    return namesystem.getAdditionalDatanode(src, blk, existings,
        existingStorageIDs, excludeSet, numAdditionalNodes, clientName);
  }

  /**
   * The client needs to give up on the block.
   */
  @Override // ClientProtocol
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog
          .debug("*BLOCK* NameNode.abandonBlock: " + b + " of file " + src);
    }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last, final byte[] data)
      throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog
          .debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    }

    return namesystem.completeFile(src, clientName, last, data);
  }

  /**
   * The client has detected an error on the specified located blocks
   * and is reporting them to the server.  For now, the namenode will
   * mark the block as corrupt.  In the future we might
   * check the blocks are actually corrupt.
   */
  @Override // ClientProtocol, DatanodeProtocol
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namesystem.reportBadBlocks(blocks);
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
      String clientName) throws IOException {
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
      throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs);
  }
  
  @Override // DatanodeProtocol
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets, String[] newtargetstorages)
      throws IOException {
    namesystem.commitBlockSynchronization(block, newgenerationstamp, newlength,
        closeFile, deleteblock, newtargets, newtargetstorages);
  }
  
  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }

  @Deprecated
  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException(
          "rename: Pathname too long.  Limit " + MAX_PATH_LENGTH +
              " characters, " + MAX_PATH_DEPTH + " levels.");
    }

    boolean ret;
    ret = namesystem.multiTransactionalRename(src, dst);
    if (ret) {
      metrics.incrFilesRenamed();
    }
    return ret;
  }
  
  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    namesystem.concat(trg, src);
  }
  
  @Override // ClientProtocol
  public void rename2(String src, String dst, Options.Rename... options)
      throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException(
          "rename: Pathname too long.  Limit " + MAX_PATH_LENGTH +
              " characters, " + MAX_PATH_DEPTH + " levels.");
    }

    namesystem.multiTransactionalRename(src, dst, options);
    metrics.incrFilesRenamed();
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug(
          "*DIR* Namenode.delete: src=" + src + ", recursive=" + recursive);
    }

    boolean ret;
    ret = namesystem.multiTransactionalDelete(src, recursive);

    if (ret) {
      metrics.incrDeleteFileOps();
    }
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long
   * or depth is too great.
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
        srcPath.depth() <= MAX_PATH_DEPTH);
  }

  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException(
          "mkdirs: Pathname too long.  Limit " + MAX_PATH_LENGTH +
              " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src, new PermissionStatus(
            UserGroupInformation.getCurrentUser().getShortUserName(), null,
            masked), createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation) throws IOException {
    DirectoryListing files =
        namesystem.getListing(src, startAfter, needLocation);
    if (files != null) {
      metrics.incrGetListingOps();
      metrics.incrFilesInGetListingOps(files.getPartialListing().length);
    }
    return files;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, true);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, false);
  }
  
  @Override // ClientProtocol
  public long[] getStats() throws IOException {
    return namesystem.getStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }

  @Override // ClientProtocol
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) throws IOException {
    final DatanodeStorageReport[] reports = namesystem.getDatanodeStorageReport(type);
    if (reports == null ) {
      throw new IOException("Failed to get datanode storage report for " + type
          + " datanodes.");
    }
    return reports;
  }

  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action, boolean isChecked)
      throws IOException {
    return namesystem.setSafeMode(action);
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes();
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    String[] cookieTab = new String[]{cookie};
    Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
        namesystem.listCorruptFileBlocks(path, cookieTab);

    String[] files = new String[fbs.size()];
    int i = 0;
    for (FSNamesystem.CorruptFileBlockInfo fb : fbs) {
      files[i++] = fb.path;
    }
    return new CorruptFileBlocks(files, cookieTab[0]);
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   *
   * @param bandwidth
   *     Balancer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  @Override // ClientProtocol
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    namesystem.setBalancerBandwidth(bandwidth);
  }
  
  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
      throws IOException {
      namesystem
          .multiTransactionalSetQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override // ClientProtocol
  public void fsync(String src, String clientName, long lastBlockLength)
      throws IOException {
    namesystem.fsync(src, clientName, lastBlockLength);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    metrics.incrCreateSymlinkOps();
    /* We enforce the MAX_PATH_LENGTH limit even though a symlink
     * URI may refer to a non-HDFS file system. 
     */
    if (!checkPathLength(link)) {
      throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
          " character limit");

    }
    if ("".equals(target)) {
      throw new IOException("Invalid symlink target");
    }
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    namesystem.createSymlink(target, link,
        new PermissionStatus(ugi.getShortUserName(), null, dirPerms),
        createParent);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    metrics.incrGetLinkTargetOps();
    try {
      HdfsFileStatus stat = namesystem.getFileInfo(path, false);
      if (stat != null) {
        // NB: getSymlink throws IOException if !stat.isSymlink() 
        return stat.getSymlink().toString();
      }
    } catch (UnresolvedPathException e) {
      return e.getResolvedPath().toString();
    } catch (UnresolvedLinkException e) {
      // The NameNode should only throw an UnresolvedPathException
      throw new AssertionError("UnresolvedLinkException thrown");
    }
    return null;
  }


  @Override // DatanodeProtocol
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifyLayoutVersion(nodeReg.getVersion());
    verifySoftwareVersion(nodeReg);
    namesystem.registerDatanode(nodeReg);
    return nodeReg;
  }

  @Override // DatanodeProtocol
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, int xmitsInProgress, int xceiverCount,
      int failedVolumes) throws IOException {
    verifyRequest(nodeReg);

    return namesystem.handleHeartbeat(nodeReg, reports, xceiverCount, xmitsInProgress, failedVolumes);
  }

  @Override // DatanodeProtocol
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
      String poolId, StorageBlockReport[] reports) throws IOException {
    verifyRequest(nodeReg);
    if (blockStateChangeLog.isDebugEnabled()) {
      blockStateChangeLog.debug(
          "*BLOCK* NameNode.blockReport: from " + nodeReg +
              ", reports.length=" + reports.length);
    }

    final BlockManager bm = namesystem.getBlockManager();
    boolean noStaleStorages = false;
    for(StorageBlockReport r : reports) {
      final BlockReport blocks = r.getReport();
      noStaleStorages =  bm.processReport(nodeReg, r.getStorage(), blocks);
      metrics.incrStorageBlockReportOps();
    }

    if(noStaleStorages) {
      return new FinalizeCommand(poolId);
    } else {
      return null;
    }
  }

  @Override // DatanodeProtocol
  public void blockReceivedAndDeleted(DatanodeRegistration nodeReg,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
    verifyRequest(nodeReg);
    metrics.incrBlockReceivedAndDeletedOps();
    if (blockStateChangeLog.isDebugEnabled()) {
      blockStateChangeLog.debug(
          "*BLOCK* NameNode.blockReceivedAndDeleted: " + "from " + nodeReg +
              " " + receivedAndDeletedBlocks.length + " blocks.");
    }

    for(StorageReceivedDeletedBlocks r : receivedAndDeletedBlocks) {
      namesystem.getBlockManager().processIncrementalBlockReport(nodeReg, r);
    }
  }
  
  @Override // DatanodeProtocol
  public void errorReport(DatanodeRegistration nodeReg, int errorCode,
      String msg) throws IOException {
    String dnName = (nodeReg == null) ? "Unknown DataNode" : nodeReg.toString();

    if (errorCode == DatanodeProtocol.NOTIFY) {
      LOG.info("Error report from " + dnName + ": " + msg);
      return;
    }
    verifyRequest(nodeReg);

    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Disk error on " + dnName + ": " + msg);
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      LOG.warn("Fatal disk error on " + dnName + ": " + msg);
      namesystem.getBlockManager().getDatanodeManager().removeDatanode(nodeReg);
    } else {
      LOG.info("Error report from " + dnName + ": " + msg);
    }
  }

  @Override // DatanodeProtocol, NamenodeProtocol
  public NamespaceInfo versionRequest() throws IOException {
    namesystem.checkSuperuserPrivilege();
    return namesystem.getNamespaceInfo();
  }

  @Override
  public byte[] getSmallFileData(int id) throws IOException {
    return namesystem.getSmallFileData(id);
  }

  /**
   * Verifies the given registration.
   *
   * @param nodeReg
   *     node registration
   * @throws UnregisteredNodeException
   *     if the registration is invalid
   */
  private void verifyRequest(NodeRegistration nodeReg) throws IOException {
    // verify registration ID
    final String id = nodeReg.getRegistrationID();
    final String expectedID = namesystem.getRegistrationID();
    if (!expectedID.equals(id)) {
      LOG.warn("Registration IDs mismatched: the "
          + nodeReg.getClass().getSimpleName() + " ID is " + id
          + " but the expected ID is " + expectedID);
      throw new UnregisteredNodeException(nodeReg);
    }
  }


  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException(
          "Service Level Authorization not enabled!");
    }

    this.clientRpcServer
        .refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    if (this.serviceRpcServer != null) {
      this.serviceRpcServer
          .refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " +
        UserGroupInformation.getCurrentUser().getShortUserName());
    Groups.getUserToGroupsMappingService().refresh();
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing SuperUser proxy group mapping list ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }
  
  @Override // GetUserMappingsProtocol
  public String[] getGroupsForUser(String user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting groups for user " + user);
    }
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  /**
   * Verify version.
   *
   * @param version
   * @throws IOException
   */
  void verifyLayoutVersion(int version) throws IOException {
    if (version != HdfsConstants.LAYOUT_VERSION) {
      throw new IncorrectVersionException(version, "data node");
    }
  }
  
  private void verifySoftwareVersion(DatanodeRegistration dnReg)
      throws IncorrectVersionException, IOException {
    String dnVersion = dnReg.getSoftwareVersion();
    if (VersionUtil.compareVersions(dnVersion, minimumDataNodeVersion) < 0) {
      IncorrectVersionException ive =
          new IncorrectVersionException(minimumDataNodeVersion, dnVersion,
              "DataNode", "NameNode");
      LOG.warn(ive.getMessage() + " DN: " + dnReg);
      throw ive;
    }
    String nnVersion = VersionInfo.getVersion();
    if (!dnVersion.equals(nnVersion)) {
      String messagePrefix = "Reported DataNode version '" + dnVersion +
          "' of DN " + dnReg + " does not match NameNode version '" +
          nnVersion + "'";
      long nnCTime = StorageInfo.getStorageInfoFromDB().getCTime();
      long dnCTime = dnReg.getStorageInfo().getCTime();
      if (nnCTime != dnCTime) {
        IncorrectVersionException ive = new IncorrectVersionException(
            messagePrefix + " and CTime of DN ('" + dnCTime +
                "') does not match CTime of NN ('" + nnCTime + "')");
        LOG.warn(ive);
        throw ive;
      } else {
        LOG.info(
            messagePrefix + ". Note: This is normal during a rolling upgrade.");
      }
    }
  }

  private static String getClientMachine() {
    String clientMachine = NamenodeWebHdfsMethods.getRemoteAddress();
    if (clientMachine == null) { //not a web client
      clientMachine = Server.getRemoteAddress();
    }
    if (clientMachine == null) { //not a RPC client
      clientMachine = "";
    }
    return clientMachine;
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    return namesystem.getBlockManager().generateDataEncryptionKey();
  }


  @Override
  public SortedActiveNodeList getActiveNamenodes() throws IOException {
    return nn.getActiveNameNodes();
  }

  @Override
  public ActiveNode getNextNamenodeToSendBlockReport(long noOfBlks) throws IOException {
    return nn.getNextNamenodeToSendBlockReport(noOfBlks);
  }
  
  @Override
  public void ping() throws IOException {
  }
  
  @Override
  public SortedActiveNodeList getActiveNamenodesForClient() throws IOException {
    return nn.getActiveNameNodes();
  }

  @Override
  public void changeConf(List<String> props, List<String> newVals)
      throws IOException {
    namesystem.changeConf(props, newVals);
  }

  @Override
  public void flushCache(String userName, String groupName) throws IOException {
    namesystem.flushCache(userName, groupName);
  }

  @Override // ClientProtocol
  public HdfsFileStatus create(String src, FsPermission masked,
      String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent,
      short replication, long blockSize, EncodingPolicy policy)
      throws IOException {
    HdfsFileStatus stat =
        create(src, masked, clientName, flag, createParent, replication,
            blockSize);
    if (policy != null) {
      if (!namesystem.isErasureCodingEnabled()) {
        throw new IOException("Requesting encoding although erasure coding" +
            " was disabled");
      }
      LOG.info("Create file " + src + " with policy " + policy.toString());
      namesystem.addEncodingStatus(src, policy,
          EncodingStatus.Status.ENCODING_REQUESTED);
    }
    return stat;
  }

  @Override // ClientProtocol
  public EncodingStatus getEncodingStatus(String filePath) throws IOException {
    EncodingStatus status = namesystem.getEncodingStatus(filePath);

    if (status.getStatus() == EncodingStatus.Status.DELETED) {
      throw new IOException("Trying to read encoding status of a deleted file");
    }

    return status;
  }

  @Override // ClientProtocol
  public void encodeFile(String filePath, EncodingPolicy policy)
      throws IOException {
    if (!namesystem.isErasureCodingEnabled()) {
      throw new IOException("Requesting encoding although erasure coding" +
          " was disabled");
    }
    namesystem.addEncodingStatus(filePath, policy,
        EncodingStatus.Status.COPY_ENCODING_REQUESTED);
  }

  @Override
  public void revokeEncoding(String filePath, short replication)
      throws IOException {
    if (!namesystem.isErasureCodingEnabled()) {
      throw new IOException("Requesting revoke although erasure coding" +
          " was disabled");
    }
    namesystem.revokeEncoding(filePath, replication);
  }

  @Override // ClientProtocol
  public LocatedBlocks getMissingBlockLocations(String filePath)
      throws IOException {
    return namesystem.getMissingBlockLocations(getClientMachine(), filePath);
  }

  @Override // ClientProtocol
  public void addBlockChecksum(String src, int blockIndex, long checksum)
      throws IOException {
    namesystem.addBlockChecksum(src, blockIndex, checksum);
  }

  @Override // ClientProtocol
  public long getBlockChecksum(String src, int blockIndex) throws IOException {
    return namesystem.getBlockChecksum(src, blockIndex);
  }

  @Override // ClientProtocol
  public LocatedBlock getRepairedBlockLocations(String sourcePath,
      String parityPath, LocatedBlock block, boolean isParity)
      throws IOException {
    return namesystem.getRepairedBlockLocations(getClientMachine(), sourcePath,
        parityPath, block, isParity);
  }

  @Override // ClientProtocol
  public void checkAccess(String path, FsAction mode) throws IOException {
    namesystem.checkAccess(path, mode);
  }

  @Override // ClientProtocol
  public LastUpdatedContentSummary getLastUpdatedContentSummary(String path)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    return namesystem.getLastUpdatedContentSummary(path);
  }
}
