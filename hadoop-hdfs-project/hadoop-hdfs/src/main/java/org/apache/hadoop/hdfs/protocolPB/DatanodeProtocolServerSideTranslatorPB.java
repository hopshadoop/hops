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

package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.leader_election.proto.ActiveNodeProtos;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ActiveNamenodeListRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ActiveNamenodeListResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReceivedAndDeletedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CacheReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.CommitBlockSynchronizationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ErrorReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.HeartbeatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.NameNodeAddressRequestForBlockReportingProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.NameNodeAddressRequestForCacheReportingProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageBlockReportProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.StorageReceivedDeletedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.*;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;

public class DatanodeProtocolServerSideTranslatorPB
    implements DatanodeProtocolPB {

  private final DatanodeProtocol impl;
  private static final ErrorReportResponseProto
      VOID_ERROR_REPORT_RESPONSE_PROTO =
      ErrorReportResponseProto.newBuilder().build();
  private static final BlockReceivedAndDeletedResponseProto
      VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE =
      BlockReceivedAndDeletedResponseProto.newBuilder().build();
  private static final ReportBadBlocksResponseProto
      VOID_REPORT_BAD_BLOCK_RESPONSE =
      ReportBadBlocksResponseProto.newBuilder().build();
  private static final CommitBlockSynchronizationResponseProto
      VOID_COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO =
      CommitBlockSynchronizationResponseProto.newBuilder().build();

  public DatanodeProtocolServerSideTranslatorPB(DatanodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RegisterDatanodeResponseProto registerDatanode(
      RpcController controller, RegisterDatanodeRequestProto request)
      throws ServiceException {
    DatanodeRegistration registration =
        PBHelper.convert(request.getRegistration());
    DatanodeRegistration registrationResp;
    try {
      registrationResp = impl.registerDatanode(registration);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return RegisterDatanodeResponseProto.newBuilder()
        .setRegistration(PBHelper.convert(registrationResp)).build();
  }

  @Override
  public HeartbeatResponseProto sendHeartbeat(RpcController controller,
      HeartbeatRequestProto request) throws ServiceException {
    HeartbeatResponse response;

    try {
      final StorageReport[] report = PBHelper.convertStorageReports(
          request.getReportsList());

      response = impl.sendHeartbeat(PBHelper.convert(request.getRegistration()),
          report, request.getCacheCapacity(), request.getCacheUsed(),
          request.getXmitsInProgress(), request.getXceiverCount(),
          request.getFailedVolumes());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    HeartbeatResponseProto.Builder builder = HeartbeatResponseProto.newBuilder();
    DatanodeCommand[] cmds = response.getCommands();
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        if (cmd != null) {
          builder.addCmds(PBHelper.convert(cmd));
        }
      }
    }
    RollingUpgradeStatus rollingUpdateStatus = response
        .getRollingUpdateStatus();
    if (rollingUpdateStatus != null) {
      builder.setRollingUpgradeStatus(PBHelper
          .convertRollingUpgradeStatus(rollingUpdateStatus));
    }
    return builder.build();
  }

  @Override
  public BlockReportResponseProto blockReport(RpcController controller,
      BlockReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    StorageBlockReport[] storageBlockReports =
        new StorageBlockReport[request.getReportsCount()];

    int index = 0;
    for (StorageBlockReportProto s : request.getReportsList()) {
      DatanodeProtocolProtos.BlockReportProto report = s.getReport();
      storageBlockReports[index++] =
          new StorageBlockReport(PBHelper.convert(s.getStorage()),
              PBHelper.convert(report));
    }
    try {
      cmd = impl.blockReport(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), storageBlockReports);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    BlockReportResponseProto.Builder builder =
        BlockReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }

  @Override
  public CacheReportResponseProto cacheReport(RpcController controller,
      CacheReportRequestProto request) throws ServiceException {
    DatanodeCommand cmd = null;
    try {
      cmd = impl.cacheReport(
          PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(),
          request.getBlocksList(),
          request.getCacheCapacity(),
          request.getCacheUsed());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    CacheReportResponseProto.Builder builder =
        CacheReportResponseProto.newBuilder();
    if (cmd != null) {
      builder.setCmd(PBHelper.convert(cmd));
    }
    return builder.build();
  }

  @Override
  public BlockReceivedAndDeletedResponseProto blockReceivedAndDeleted(
      RpcController controller, BlockReceivedAndDeletedRequestProto request)
      throws ServiceException {
    List<StorageReceivedDeletedBlocksProto> sBlocks = request.getBlocksList();
    StorageReceivedDeletedBlocks[] info =
        new StorageReceivedDeletedBlocks[sBlocks.size()];

    ArrayList<Long> blockIds = new ArrayList<Long>();

    for (int i = 0; i < sBlocks.size(); i++) {
      StorageReceivedDeletedBlocksProto sBlock = sBlocks.get(i);
      List<ReceivedDeletedBlockInfoProto> list = sBlock.getBlocksList();
      ReceivedDeletedBlockInfo[] rdBlocks =
          new ReceivedDeletedBlockInfo[list.size()];
      for (int j = 0; j < list.size(); j++) {
        rdBlocks[j] = PBHelper.convert(list.get(j));
        blockIds.add(rdBlocks[j].getBlock().getBlockId());
      }
      if (sBlock.hasStorage()) {
        info[i] = new StorageReceivedDeletedBlocks(
            PBHelper.convert(sBlock.getStorage()), rdBlocks);
      } else {
        info[i] = new StorageReceivedDeletedBlocks(sBlock.getStorageUuid(), rdBlocks);
      }
    }
    try {
      impl.blockReceivedAndDeleted(PBHelper.convert(request.getRegistration()),
          request.getBlockPoolId(), info);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_BLOCK_RECEIVED_AND_DELETE_RESPONSE;
  }

  @Override
  public ErrorReportResponseProto errorReport(RpcController controller,
      ErrorReportRequestProto request) throws ServiceException {
    try {
      impl.errorReport(PBHelper.convert(request.getRegistartion()),
          request.getErrorCode(), request.getMsg());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ERROR_REPORT_RESPONSE_PROTO;
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder().setInfo(PBHelper.convert(info))
        .build();
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto request) throws ServiceException {
    List<LocatedBlockProto> lbps = request.getBlocksList();
    LocatedBlock[] blocks = new LocatedBlock[lbps.size()];
    for (int i = 0; i < lbps.size(); i++) {
      blocks[i] = PBHelper.convert(lbps.get(i));
    }
    try {
      impl.reportBadBlocks(blocks);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REPORT_BAD_BLOCK_RESPONSE;
  }

  @Override
  public CommitBlockSynchronizationResponseProto commitBlockSynchronization(
      RpcController controller, CommitBlockSynchronizationRequestProto request)
      throws ServiceException {
    List<DatanodeIDProto> dnprotos = request.getNewTargetsList();
    DatanodeID[] dns = new DatanodeID[dnprotos.size()];
    for (int i = 0; i < dnprotos.size(); i++) {
      dns[i] = PBHelper.convert(dnprotos.get(i));
    }
    final List<String> sidprotos = request.getNewTargetStoragesList();
    final String[] storageIDs = sidprotos.toArray(new String[sidprotos.size()]);
    try {
      impl.commitBlockSynchronization(PBHelper.convert(request.getBlock()),
          request.getNewGenStamp(), request.getNewLength(),
          request.getCloseFile(), request.getDeleteBlock(), dns, storageIDs);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_COMMIT_BLOCK_SYNCHRONIZATION_RESPONSE_PROTO;
  }

  //HOP_CODE_START
  @Override
  public ActiveNamenodeListResponseProto getActiveNamenodes(
      RpcController controller, ActiveNamenodeListRequestProto request)
      throws ServiceException {
    
    try {
      SortedActiveNodeList anl = impl.getActiveNamenodes();
      ActiveNamenodeListResponseProto response = PBHelper.convert(anl);
      return response;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    
  }

  @Override
  public ActiveNodeProtos.ActiveNodeProto getNextNamenodeToSendBlockReport(
      RpcController controller,
      NameNodeAddressRequestForBlockReportingProto request)
      throws ServiceException {
    try {
      ActiveNode response = impl.getNextNamenodeToSendBlockReport(request.getNoOfBlks(), PBHelper.convert(request.
          getRegistration()));
      ActiveNodeProtos.ActiveNodeProto responseProto =
          PBHelper.convert(response);
      return responseProto;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ActiveNodeProtos.ActiveNodeProto getNextNamenodeToSendCacheReport(
      RpcController controller,
      NameNodeAddressRequestForCacheReportingProto request)
      throws ServiceException {
    try {
      ActiveNode response = impl.getNextNamenodeToSendCacheReport(request.getNoOfBlks(), PBHelper.convert(request.
          getRegistration()));
      ActiveNodeProtos.ActiveNodeProto responseProto =
          PBHelper.convert(response);
      return responseProto;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public DatanodeProtocolProtos.SmallFileDataResponseProto getSmallFileData(RpcController controller, DatanodeProtocolProtos.GetSmallFileDataProto request) throws ServiceException {
    try{
      byte[] data = impl.getSmallFileData(request.getId());
      return  PBHelper.convert(data);
    } catch (IOException e){
      throw new ServiceException(e);
    }
  }

}
