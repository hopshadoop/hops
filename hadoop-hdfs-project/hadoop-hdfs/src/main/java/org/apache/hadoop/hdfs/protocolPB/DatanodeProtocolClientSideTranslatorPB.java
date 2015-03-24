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
package org.apache.hadoop.hdfs.protocolPB;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.leader_election.proto.ActiveNodeProtos;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ActiveNamenodeListRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ActiveNamenodeListResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.NameNodeAddressRequestForBlockReportingProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportCompletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageBlockReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo.Capability;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link DatanodeProtocol} interfaces to the RPC server implementing
 * {@link DatanodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DatanodeProtocolClientSideTranslatorPB
    implements ProtocolMetaInterface, DatanodeProtocol, Closeable {

  /**
   * RpcController is not used and hence is set to null
   */
  private final DatanodeProtocolPB rpcProxy;
  private static final VersionRequestProto VOID_VERSION_REQUEST =
      VersionRequestProto.newBuilder().build();
  private final static RpcController NULL_CONTROLLER = null;
  
  @VisibleForTesting
  public DatanodeProtocolClientSideTranslatorPB(DatanodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  public DatanodeProtocolClientSideTranslatorPB(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DatanodeProtocolPB.class,
        ProtobufRpcEngine.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = createNamenode(nameNodeAddr, conf, ugi);
  }

  private static DatanodeProtocolPB createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProtocolProxy(DatanodeProtocolPB.class,
        RPC.getProtocolVersion(DatanodeProtocolPB.class), nameNodeAddr, ugi,
        conf, NetUtils.getSocketFactory(conf, DatanodeProtocolPB.class),
        org.apache.hadoop.ipc.Client.getPingInterval(conf), null).getProxy();
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public DatanodeRegistration registerDatanode(
      DatanodeRegistration registration) throws IOException {
    RegisterDatanodeRequestProto.Builder builder =
        RegisterDatanodeRequestProto.newBuilder()
            .setRegistration(PBHelper.convert(registration));
    RegisterDatanodeResponseProto resp;
    try {
      resp = rpcProxy.registerDatanode(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return PBHelper.convert(resp.getRegistration());
  }

  @Override
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
      StorageReport[] reports, long cacheCapacity, long cacheUsed, int xmitsInProgress, int xceiverCount,
      int failedVolumes, VolumeFailureSummary volumeFailureSummary) throws IOException {
    HeartbeatRequestProto.Builder builder = HeartbeatRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setXmitsInProgress(xmitsInProgress).setXceiverCount(xceiverCount)
        .setFailedVolumes(failedVolumes);
    builder.addAllReports(PBHelper.convertStorageReports(reports));
    if (cacheCapacity != 0) {
      builder.setCacheCapacity(cacheCapacity);
    }
    if (cacheUsed != 0) {
      builder.setCacheUsed(cacheUsed);
    }
    if (volumeFailureSummary != null) {
      builder.setVolumeFailureSummary(PBHelper.convertVolumeFailureSummary(
          volumeFailureSummary));
    }
    HeartbeatResponseProto resp;
    try {
      resp = rpcProxy.sendHeartbeat(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    DatanodeCommand[] cmds = new DatanodeCommand[resp.getCmdsList().size()];
    int index = 0;
    for (DatanodeCommandProto p : resp.getCmdsList()) {
      cmds[index] = PBHelper.convert(p);
      index++;
    }
    RollingUpgradeStatus rollingUpdateStatus = null;
    if (resp.hasRollingUpgradeStatus()) {
      rollingUpdateStatus = PBHelper.convert(resp.getRollingUpgradeStatus());
    }
    return new HeartbeatResponse(cmds, rollingUpdateStatus);
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, StorageBlockReport[] reports, BlockReportContext context)
        throws IOException {
    BlockReportRequestProto.Builder builder = BlockReportRequestProto
        .newBuilder().setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    
    boolean useBlocksBuffer = registration.getNamespaceInfo()
        .isCapabilitySupported(Capability.STORAGE_BLOCK_REPORT_BUFFERS);
    
    for (StorageBlockReport r : reports) {
      StorageBlockReportProto.Builder reportBuilder =
          StorageBlockReportProto.newBuilder()
              .setStorage(PBHelper.convert(r.getStorage()))
              .setReport(PBHelper.convert(r.getReport(), useBlocksBuffer));
      builder.addReports(reportBuilder.build());
    }
    builder.setContext(PBHelper.convert(context));
    BlockReportResponseProto resp;
    try {
      resp = rpcProxy.blockReport(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    return resp.hasCmd() ? PBHelper.convert(resp.getCmd()) : null;
  }

  @Override
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds, long cacheCapacity, long cacheUsed) throws IOException {
    CacheReportRequestProto.Builder builder =
        CacheReportRequestProto.newBuilder()
        .setRegistration(PBHelper.convert(registration))
        .setBlockPoolId(poolId);
    for (Long blockId : blockIds) {
      builder.addBlocks(blockId);
    }
    builder.setCacheCapacity(cacheCapacity);
    builder.setCacheUsed(cacheUsed);
    
    CacheReportResponseProto resp;
    try {
      resp = rpcProxy.cacheReport(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
    if (resp.hasCmd()) {
      return PBHelper.convert(resp.getCmd());
    }
    return null;
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks)
      throws IOException {
    BlockReceivedAndDeletedRequestProto.Builder builder =
        BlockReceivedAndDeletedRequestProto.newBuilder()
            .setRegistration(PBHelper.convert(registration))
            .setBlockPoolId(poolId);
    for (StorageReceivedDeletedBlocks storageBlock : receivedAndDeletedBlocks) {
      StorageReceivedDeletedBlocksProto.Builder repBuilder =
          StorageReceivedDeletedBlocksProto.newBuilder();
      repBuilder.setStorageUuid(storageBlock.getStorage().getStorageID());  // Set for wire compatibility.
      repBuilder.setStorage(PBHelper.convert(storageBlock.getStorage()));
      for (ReceivedDeletedBlockInfo rdBlock : storageBlock.getBlocks()) {
        repBuilder.addBlocks(PBHelper.convert(rdBlock));
      }
      builder.addBlocks(repBuilder.build());
    }
    try {
      rpcProxy.blockReceivedAndDeleted(NULL_CONTROLLER, builder.build());
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode,
      String msg) throws IOException {
    ErrorReportRequestProto req = ErrorReportRequestProto.newBuilder()
        .setRegistartion(PBHelper.convert(registration)).setErrorCode(errorCode)
        .setMsg(msg).build();
    try {
      rpcProxy.errorReport(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    try {
      return PBHelper.convert(
          rpcProxy.versionRequest(NULL_CONTROLLER, VOID_VERSION_REQUEST)
              .getInfo());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    ReportBadBlocksRequestProto.Builder builder =
        ReportBadBlocksRequestProto.newBuilder();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(i, PBHelper.convert(blocks[i]));
    }
    ReportBadBlocksRequestProto req = builder.build();
    try {
      rpcProxy.reportBadBlocks(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets, String[] newtargetstorages)
      throws IOException {
    CommitBlockSynchronizationRequestProto.Builder builder =
        CommitBlockSynchronizationRequestProto.newBuilder()
            .setBlock(PBHelper.convert(block))
            .setNewGenStamp(newgenerationstamp).setNewLength(newlength)
            .setCloseFile(closeFile).setDeleteBlock(deleteblock);
    for (int i = 0; i < newtargets.length; i++) {
      builder.addNewTargets(PBHelper.convert(newtargets[i]));
      builder.addNewTargetStorages(newtargetstorages[i]);
    }
    CommitBlockSynchronizationRequestProto req = builder.build();
    try {
      rpcProxy.commitBlockSynchronization(NULL_CONTROLLER, req);
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override // ProtocolMetaInterface
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, DatanodeProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(DatanodeProtocolPB.class), methodName);
  }


  @Override
  public SortedActiveNodeList getActiveNamenodes() throws IOException {

    try {
      ActiveNamenodeListRequestProto.Builder request =
          ActiveNamenodeListRequestProto.newBuilder();
      ActiveNamenodeListResponseProto response =
          rpcProxy.getActiveNamenodes(NULL_CONTROLLER, request.build());
      SortedActiveNodeList anl = PBHelper.convert(response);
      return anl;
    } catch (ServiceException se) {
      throw ProtobufHelper.getRemoteException(se);
    }
  }

  @Override
  public ActiveNode getNextNamenodeToSendBlockReport(long noOfBlks, DatanodeRegistration nodeReg) throws IOException {

    NameNodeAddressRequestForBlockReportingProto.Builder request =
        NameNodeAddressRequestForBlockReportingProto.newBuilder();
    request.setNoOfBlks(noOfBlks);
    request.setRegistration(PBHelper.convert(nodeReg));
    try {
      ActiveNodeProtos.ActiveNodeProto response = rpcProxy
          .getNextNamenodeToSendBlockReport(NULL_CONTROLLER, request.build());
      ActiveNode aNamenode = PBHelper.convert(response);
      return aNamenode;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void blockReportCompleted(DatanodeRegistration nodeReg) throws IOException {

    BlockReportCompletedRequestProto.Builder request =
                BlockReportCompletedRequestProto.newBuilder();
    request.setRegistration(PBHelper.convert(nodeReg));
    try {
      rpcProxy.blockReportCompleted (NULL_CONTROLLER, request.build());
      return;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  /**
   * Read the small file data
   *
   * @param id
   * @return data
   * @throws IOException
   */
  @Override
  public byte[] getSmallFileData(int id) throws IOException {
    DatanodeProtocolProtos.GetSmallFileDataProto.Builder request =
            DatanodeProtocolProtos.GetSmallFileDataProto.newBuilder();
    request.setId(id);
    try{
     DatanodeProtocolProtos.SmallFileDataResponseProto response = rpcProxy.getSmallFileData(NULL_CONTROLLER, request.build());
     return PBHelper.convert(response);

    } catch (ServiceException e) {
    throw ProtobufHelper.getRemoteException(e);
    }
  }


}
