/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocolPB;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.leader_election.proto.ActiveNodeProtos.ActiveNodeProto;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateFlagProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DatanodeReportTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SafeModeActionProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ActiveNamenodeListResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BalancerBandwidthCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.BlockRecoveryCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeStorageProto.StorageState;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.FinalizeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.KeyUpdateCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.ReceivedDeletedBlockInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.RegisterCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlocksWithLocationsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ContentSummaryProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.CorruptFileBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DataEncryptionKeyProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto.AdminState;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfosProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DirectoryListingProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExportedBlockKeysProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsPermissionProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.FsServerDefaultsProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlocksProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageTypeProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeCommandProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamenodeRegistrationProto.NamenodeRoleProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.NamespaceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RecoveringBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ReplicaStateProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.StorageReportProto;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * Utilities for converting protobuf classes to and from implementation classes
 * and other helper utilities to help in dealing with protobuf.
 * <p/>
 * Note that when converting from an internal type to protobuf type, the
 * converter never return null for protobuf type. The check for internal type
 * being null must be done before calling the convert() method.
 */
public class PBHelper {
  private static final RegisterCommandProto REG_CMD_PROTO =
      RegisterCommandProto.newBuilder().build();
  private static final RegisterCommand REG_CMD = new RegisterCommand();

  private PBHelper() {
    /** Hidden constructor */
  }

  public static ByteString getByteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public static NamenodeRole convert(NamenodeRoleProto role) {
    switch (role) {
      case NAMENODE:
        return NamenodeRole.NAMENODE;
      case BACKUP:
        return NamenodeRole.BACKUP;
      case CHECKPOINT:
        return NamenodeRole.CHECKPOINT;
    }
    return null;
  }

  public static NamenodeRoleProto convert(NamenodeRole role) {
    switch (role) {
      case NAMENODE:
        return NamenodeRoleProto.NAMENODE;
      case BACKUP:
        return NamenodeRoleProto.BACKUP;
      case CHECKPOINT:
        return NamenodeRoleProto.CHECKPOINT;
    }
    return null;
  }

  public static StorageInfoProto convert(StorageInfo info) {
    return StorageInfoProto.newBuilder().setClusterID(info.getClusterID())
        .setCTime(info.getCTime()).setLayoutVersion(info.getLayoutVersion())
        .setNamespaceID(info.getNamespaceID()).setBlockpoolID(
            info.getBlockPoolId())                //HOP added this line
        .setDEFAULTROWID(info.getDefaultRowId())
        .build();     //HOP added this line
  }

  public static StorageInfo convert(StorageInfoProto info) {
    return new StorageInfo(info.getLayoutVersion(), info.getNamespaceID(),
        info.getClusterID(), info.getCTime(), info.getBlockpoolID());
  }

  public static NamenodeRegistrationProto convert(NamenodeRegistration reg) {
    return NamenodeRegistrationProto.newBuilder()
        .setHttpAddress(reg.getHttpAddress()).setRole(convert(reg.getRole()))
        .setRpcAddress(reg.getAddress())
        .setStorageInfo(convert((StorageInfo) reg)).build();
  }

  public static NamenodeRegistration convert(NamenodeRegistrationProto reg) {
    return new NamenodeRegistration(reg.getRpcAddress(), reg.getHttpAddress(),
        convert(reg.getStorageInfo()), convert(reg.getRole()));
  }

  // DatanodeId
  public static DatanodeID convert(DatanodeIDProto dn) {
    return new DatanodeID(dn.getIpAddr(), dn.getHostName(), dn.getDatanodeUuid(),
        dn.getXferPort(), dn.getInfoPort(), dn.getIpcPort());
  }

  public static DatanodeIDProto convert(DatanodeID dn) {
    // For wire compatibility with older versions we transmit the StorageID
    // which is the same as the DatanodeUuid. Since StorageID is a required
    // field we pass the empty string if the DatanodeUuid is not yet known.
    return DatanodeIDProto.newBuilder()
        .setIpAddr(dn.getIpAddr())
        .setHostName(dn.getHostName())
        .setDatanodeUuid(dn.getDatanodeUuid() != null ? dn.getDatanodeUuid() : "")
        .setXferPort(dn.getXferPort())
        .setInfoPort(dn.getInfoPort())
        .setIpcPort(dn.getIpcPort()).build();
  }

  // Arrays of DatanodeId
  public static DatanodeIDProto[] convert(DatanodeID[] did) {
    if (did == null) {
      return null;
    }
    final int len = did.length;
    DatanodeIDProto[] result = new DatanodeIDProto[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(did[i]);
    }
    return result;
  }
  
  public static DatanodeID[] convert(DatanodeIDProto[] did) {
    if (did == null) {
      return null;
    }
    final int len = did.length;
    DatanodeID[] result = new DatanodeID[len];
    for (int i = 0; i < len; ++i) {
      result[i] = convert(did[i]);
    }
    return result;
  }
  
  // Block
  public static BlockProto convert(Block b) {
    return BlockProto.newBuilder().setBlockId(b.getBlockId())
        .setGenStamp(b.getGenerationStamp()).setNumBytes(b.getNumBytes())
        .build();
  }

  public static Block convert(BlockProto b) {
    return new Block(b.getBlockId(), b.getNumBytes(), b.getGenStamp());
  }

  public static BlockWithLocationsProto convert(BlockWithLocations blk) {
    return BlockWithLocationsProto.newBuilder()
        .setBlock(convert(blk.getBlock()))
        .addAllDatanodeUuids(Arrays.asList(blk.getDatanodeUuids()))
        .addAllStorageUuids(Arrays.asList(blk.getStorageIDs()))
        .addAllStorageTypes(convertStorageTypes(blk.getStorageTypes()))
        .build();
  }

  public static BlockWithLocations convert(BlockWithLocationsProto b) {
    final List<String> datanodeUuids = b.getDatanodeUuidsList();
    final List<String> storageUuids = b.getStorageUuidsList();
    final List<StorageTypeProto> storageTypes = b.getStorageTypesList();
    return new BlockWithLocations(convert(b.getBlock()),
        datanodeUuids.toArray(new String[datanodeUuids.size()]),
        storageUuids.toArray(new String[storageUuids.size()]),
        convertStorageTypes(storageTypes, storageUuids.size()));
  }

  public static BlocksWithLocationsProto convert(BlocksWithLocations blks) {
    BlocksWithLocationsProto.Builder builder =
        BlocksWithLocationsProto.newBuilder();
    for (BlockWithLocations b : blks.getBlocks()) {
      builder.addBlocks(convert(b));
    }
    return builder.build();
  }

  public static BlocksWithLocations convert(BlocksWithLocationsProto blocks) {
    List<BlockWithLocationsProto> b = blocks.getBlocksList();
    BlockWithLocations[] ret = new BlockWithLocations[b.size()];
    int i = 0;
    for (BlockWithLocationsProto entry : b) {
      ret[i++] = convert(entry);
    }
    return new BlocksWithLocations(ret);
  }

  public static BlockKeyProto convert(BlockKey key) {
    byte[] encodedKey = key.getEncodedKey();
    ByteString keyBytes =
        ByteString.copyFrom(encodedKey == null ? new byte[0] : encodedKey);
    return BlockKeyProto.newBuilder().setKeyId(key.getKeyId())
        .setKeyBytes(keyBytes).setExpiryDate(key.getExpiryDate()).build();
  }

  public static BlockKey convert(BlockKeyProto k) {
    return new BlockKey(k.getKeyId(), k.getExpiryDate(),
        k.getKeyBytes().toByteArray());
  }

  public static ExportedBlockKeysProto convert(ExportedBlockKeys keys) {
    ExportedBlockKeysProto.Builder builder =
        ExportedBlockKeysProto.newBuilder();
    builder.setIsBlockTokenEnabled(keys.isBlockTokenEnabled())
        .setKeyUpdateInterval(keys.getKeyUpdateInterval())
        .setTokenLifeTime(keys.getTokenLifetime())
        .setCurrentKey(convert(keys.getCurrentKey()));
    for (BlockKey k : keys.getAllKeys()) {
      builder.addAllKeys(convert(k));
    }
    return builder.build();
  }

  public static ExportedBlockKeys convert(ExportedBlockKeysProto keys) {
    return new ExportedBlockKeys(keys.getIsBlockTokenEnabled(),
        keys.getKeyUpdateInterval(), keys.getTokenLifeTime(),
        convert(keys.getCurrentKey()), convertBlockKeys(keys.getAllKeysList()));
  }

  public static NamenodeCommandProto convert(NamenodeCommand cmd) {
    return NamenodeCommandProto.newBuilder()
        .setType(NamenodeCommandProto.Type.NamenodeCommand)
        .setAction(cmd.getAction()).build();
  }

  public static BlockKey[] convertBlockKeys(List<BlockKeyProto> list) {
    BlockKey[] ret = new BlockKey[list.size()];
    int i = 0;
    for (BlockKeyProto k : list) {
      ret[i++] = convert(k);
    }
    return ret;
  }

  public static NamespaceInfo convert(NamespaceInfoProto info) {
    StorageInfoProto storage = info.getStorageInfo();
    return new NamespaceInfo(storage.getNamespaceID(), storage.getClusterID(),
        info.getBlockPoolID(), storage.getCTime(), info.getBuildVersion(),
        info.getSoftwareVersion());
  }

  public static NamenodeCommand convert(NamenodeCommandProto cmd) {
    if (cmd == null) {
      return null;
    }
    return new NamenodeCommand(cmd.getAction());
  }
  
  public static ExtendedBlock convert(ExtendedBlockProto eb) {
    if (eb == null) {
      return null;
    }
    return new ExtendedBlock(eb.getPoolId(), eb.getBlockId(), eb.getNumBytes(),
        eb.getGenerationStamp());
  }
  
  public static ExtendedBlockProto convert(final ExtendedBlock b) {
    if (b == null) {
      return null;
    }
    return ExtendedBlockProto.newBuilder().
        setPoolId(b.getBlockPoolId()).
        setBlockId(b.getBlockId()).
        setNumBytes(b.getNumBytes()).
        setGenerationStamp(b.getGenerationStamp()).
        build();
  }
  
  public static RecoveringBlockProto convert(RecoveringBlock b) {
    if (b == null) {
      return null;
    }
    LocatedBlockProto lb = PBHelper.convert((LocatedBlock) b);
    return RecoveringBlockProto.newBuilder().setBlock(lb)
        .setNewGenStamp(b.getNewGenerationStamp()).build();
  }

  public static RecoveringBlock convert(RecoveringBlockProto b) {
    ExtendedBlock block = convert(b.getBlock().getB());
    DatanodeInfo[] locs = convert(b.getBlock().getLocsList());
    return new RecoveringBlock(block, locs, b.getNewGenStamp());
  }
  
  public static DatanodeInfoProto.AdminState convert(
      final DatanodeInfo.AdminStates inAs) {
    switch (inAs) {
      case NORMAL:
        return DatanodeInfoProto.AdminState.NORMAL;
      case DECOMMISSION_INPROGRESS:
        return DatanodeInfoProto.AdminState.DECOMMISSION_INPROGRESS;
      case DECOMMISSIONED:
        return DatanodeInfoProto.AdminState.DECOMMISSIONED;
      default:
        return DatanodeInfoProto.AdminState.NORMAL;
    }
  }
  
  static public DatanodeInfo convert(DatanodeInfoProto di) {
    if (di == null) {
      return null;
    }
    return new DatanodeInfo(PBHelper.convert(di.getId()),
        di.hasLocation() ? di.getLocation() : null, di.getCapacity(),
        di.getDfsUsed(), di.getRemaining(), di.getBlockPoolUsed(),
        di.getLastUpdate(), di.getXceiverCount(),
        PBHelper.convert(di.getAdminState()));
  }
  
  static public DatanodeInfoProto convertDatanodeInfo(DatanodeInfo di) {
    if (di == null) {
      return null;
    }
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    if (di.getNetworkLocation() != null) {
      builder.setLocation(di.getNetworkLocation());
    }

    return builder.
        setId(PBHelper.convert((DatanodeID) di)).
        setCapacity(di.getCapacity()).
        setDfsUsed(di.getDfsUsed()).
        setRemaining(di.getRemaining()).
        setBlockPoolUsed(di.getBlockPoolUsed()).
        setLastUpdate(di.getLastUpdate()).
        setXceiverCount(di.getXceiverCount()).
        setAdminState(PBHelper.convert(di.getAdminState())).
        build();
  }
  
  
  static public DatanodeInfo[] convert(DatanodeInfoProto di[]) {
    if (di == null) {
      return null;
    }
    DatanodeInfo[] result = new DatanodeInfo[di.length];
    for (int i = 0; i < di.length; i++) {
      result[i] = convert(di[i]);
    }
    return result;
  }

  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
      DatanodeInfo[] dnInfos) {
    return convert(dnInfos, 0);
  }
  
  /**
   * Copy from {@code dnInfos} to a target of list of same size starting at
   * {@code startIdx}.
   */
  public static List<? extends HdfsProtos.DatanodeInfoProto> convert(
      DatanodeInfo[] dnInfos, int startIdx) {
    if (dnInfos == null) {
      return null;
    }
    ArrayList<HdfsProtos.DatanodeInfoProto> protos =
        Lists.newArrayListWithCapacity(dnInfos.length);
    for (int i = startIdx; i < dnInfos.length; i++) {
      protos.add(convert(dnInfos[i]));
    }
    return protos;
  }

  public static DatanodeInfo[] convert(List<DatanodeInfoProto> list) {
    DatanodeInfo[] info = new DatanodeInfo[list.size()];
    for (int i = 0; i < info.length; i++) {
      info[i] = convert(list.get(i));
    }
    return info;
  }
  
  public static DatanodeInfoProto convert(DatanodeInfo info) {
    DatanodeInfoProto.Builder builder = DatanodeInfoProto.newBuilder();
    builder.setBlockPoolUsed(info.getBlockPoolUsed());
    builder.setAdminState(PBHelper.convert(info.getAdminState()));
    builder.setCapacity(info.getCapacity()).setDfsUsed(info.getDfsUsed())
        .setId(PBHelper.convert((DatanodeID) info))
        .setLastUpdate(info.getLastUpdate())
        .setLocation(info.getNetworkLocation())
        .setRemaining(info.getRemaining())
        .setXceiverCount(info.getXceiverCount()).build();
    return builder.build();
  }

  public static ClientNamenodeProtocolProtos.DatanodeStorageReportProto convertDatanodeStorageReport(
      DatanodeStorageReport report) {
    return ClientNamenodeProtocolProtos.DatanodeStorageReportProto.newBuilder()
        .setDatanodeInfo(convert(report.getDatanodeInfo()))
        .addAllStorageReports(convertStorageReports(report.getStorageReports()))
        .build();
  }

  public static List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> convertDatanodeStorageReports(
      DatanodeStorageReport[] reports) {
    final List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> protos
        = new ArrayList<ClientNamenodeProtocolProtos.DatanodeStorageReportProto>(reports.length);
    for(int i = 0; i < reports.length; i++) {
      protos.add(convertDatanodeStorageReport(reports[i]));
    }
    return protos;
  }

  public static DatanodeStorageReport convertDatanodeStorageReport(
      ClientNamenodeProtocolProtos.DatanodeStorageReportProto proto) {
    return new DatanodeStorageReport(
        convert(proto.getDatanodeInfo()),
        convertStorageReports(proto.getStorageReportsList()));
  }

  public static DatanodeStorageReport[] convertDatanodeStorageReports(
      List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> protos) {
    final DatanodeStorageReport[] reports
        = new DatanodeStorageReport[protos.size()];
    for(int i = 0; i < reports.length; i++) {
      reports[i] = convertDatanodeStorageReport(protos.get(i));
    }
    return reports;
  }

  public static AdminStates convert(AdminState adminState) {
    switch (adminState) {
      case DECOMMISSION_INPROGRESS:
        return AdminStates.DECOMMISSION_INPROGRESS;
      case DECOMMISSIONED:
        return AdminStates.DECOMMISSIONED;
      case NORMAL:
      default:
        return AdminStates.NORMAL;
    }
  }
  
  public static LocatedBlockProto convert(LocatedBlock b) {
    if (b == null) return null;
    Builder builder = LocatedBlockProto.newBuilder();
    DatanodeInfo[] locs = b.getLocations();
    for (int i = 0; i < locs.length; i++) {
      DatanodeInfo loc = locs[i];
      builder.addLocs(i, PBHelper.convert(loc));
    }

    StorageType[] storageTypes = b.getStorageTypes();
    if (storageTypes != null) {
      for (int i = 0; i < storageTypes.length; ++i) {
        builder.addStorageTypes(PBHelper.convertStorageType(storageTypes[i]));
      }
    }
    final String[] storageIDs = b.getStorageIDs();
    if (storageIDs != null) {
      builder.addAllStorageIDs(Arrays.asList(storageIDs));
    }

    return builder.setB(PBHelper.convert(b.getBlock()))
        .setBlockToken(PBHelper.convert(b.getBlockToken()))
        .setCorrupt(b.isCorrupt()).setOffset(b.getStartOffset()).build();
  }
  
  public static LocatedBlock convert(LocatedBlockProto proto) {
    if (proto == null) return null;
    List<DatanodeInfoProto> locs = proto.getLocsList();
    DatanodeInfo[] targets = new DatanodeInfo[locs.size()];
    for (int i = 0; i < locs.size(); i++) {
      targets[i] = PBHelper.convert(locs.get(i));
    }

    final StorageType[] storageTypes =
        convertStorageTypes(proto.getStorageTypesList(), locs.size());

    final int storageIDsCount = proto.getStorageIDsCount();
    final String[] storageIDs;
    if (storageIDsCount == 0) {
      storageIDs = null;
    } else {
      assert storageIDsCount == locs.size();
      storageIDs = proto.getStorageIDsList().toArray(new String[storageIDsCount]);
    }

    LocatedBlock lb = new LocatedBlock(PBHelper.convert(proto.getB()), targets,
        storageIDs, storageTypes, proto.getOffset(), proto.getCorrupt());
    lb.setBlockToken(PBHelper.convert(proto.getBlockToken()));

    return lb;
  }

  public static TokenProto convert(Token<?> tok) {
    return TokenProto.newBuilder().
        setIdentifier(ByteString.copyFrom(tok.getIdentifier())).
        setPassword(ByteString.copyFrom(tok.getPassword())).
        setKind(tok.getKind().toString()).
        setService(tok.getService().toString()).build();
  }
  
  public static Token<BlockTokenIdentifier> convert(TokenProto blockToken) {
    return new Token<BlockTokenIdentifier>(
        blockToken.getIdentifier().toByteArray(),
        blockToken.getPassword().toByteArray(), new Text(blockToken.getKind()),
        new Text(blockToken.getService()));
  }

  
  public static Token<DelegationTokenIdentifier> convertDelegationToken(
      TokenProto blockToken) {
    return new Token<DelegationTokenIdentifier>(
        blockToken.getIdentifier().toByteArray(),
        blockToken.getPassword().toByteArray(), new Text(blockToken.getKind()),
        new Text(blockToken.getService()));
  }

  public static ReplicaState convert(ReplicaStateProto state) {
    switch (state) {
      case RBW:
        return ReplicaState.RBW;
      case RUR:
        return ReplicaState.RUR;
      case RWR:
        return ReplicaState.RWR;
      case TEMPORARY:
        return ReplicaState.TEMPORARY;
      case FINALIZED:
      default:
        return ReplicaState.FINALIZED;
    }
  }

  public static ReplicaStateProto convert(ReplicaState state) {
    switch (state) {
      case RBW:
        return ReplicaStateProto.RBW;
      case RUR:
        return ReplicaStateProto.RUR;
      case RWR:
        return ReplicaStateProto.RWR;
      case TEMPORARY:
        return ReplicaStateProto.TEMPORARY;
      case FINALIZED:
      default:
        return ReplicaStateProto.FINALIZED;
    }
  }
  
  public static DatanodeRegistrationProto convert(
      DatanodeRegistration registration) {
    DatanodeRegistrationProto.Builder builder = DatanodeRegistrationProto.newBuilder();
    return builder
        .setDatanodeID(PBHelper.convert((DatanodeID) registration))
        .setStorageInfo(PBHelper.convert(registration.getStorageInfo()))
        .setKeys(PBHelper.convert(registration.getExportedKeys()))
        .setSoftwareVersion(registration.getSoftwareVersion()).build();
  }

  public static DatanodeRegistration convert(DatanodeRegistrationProto proto) {
    return new DatanodeRegistration(PBHelper.convert(proto.getDatanodeID()),
        PBHelper.convert(proto.getStorageInfo()),
        PBHelper.convert(proto.getKeys()), proto.getSoftwareVersion());
  }

  public static DatanodeCommand convert(DatanodeCommandProto proto) {
    switch (proto.getCmdType()) {
      case BalancerBandwidthCommand:
        return PBHelper.convert(proto.getBalancerCmd());
      case BlockCommand:
        return PBHelper.convert(proto.getBlkCmd());
      case BlockRecoveryCommand:
        return PBHelper.convert(proto.getRecoveryCmd());
      case FinalizeCommand:
        return PBHelper.convert(proto.getFinalizeCmd());
      case KeyUpdateCommand:
        return PBHelper.convert(proto.getKeyUpdateCmd());
      case RegisterCommand:
        return REG_CMD;
    }
    return null;
  }
  
  public static BalancerBandwidthCommandProto convert(
      BalancerBandwidthCommand bbCmd) {
    return BalancerBandwidthCommandProto.newBuilder()
        .setBandwidth(bbCmd.getBalancerBandwidthValue()).build();
  }

  public static KeyUpdateCommandProto convert(KeyUpdateCommand cmd) {
    return KeyUpdateCommandProto.newBuilder()
        .setKeys(PBHelper.convert(cmd.getExportedKeys())).build();
  }

  public static BlockRecoveryCommandProto convert(BlockRecoveryCommand cmd) {
    BlockRecoveryCommandProto.Builder builder =
        BlockRecoveryCommandProto.newBuilder();
    for (RecoveringBlock b : cmd.getRecoveringBlocks()) {
      builder.addBlocks(PBHelper.convert(b));
    }
    return builder.build();
  }

  public static FinalizeCommandProto convert(FinalizeCommand cmd) {
    return FinalizeCommandProto.newBuilder()
        .setBlockPoolId(cmd.getBlockPoolId()).build();
  }

  public static BlockCommandProto convert(BlockCommand cmd) {
    BlockCommandProto.Builder builder =
        BlockCommandProto.newBuilder().setBlockPoolId(cmd.getBlockPoolId());
    switch (cmd.getAction()) {
      case DatanodeProtocol.DNA_TRANSFER:
        builder.setAction(BlockCommandProto.Action.TRANSFER);
        break;
      case DatanodeProtocol.DNA_INVALIDATE:
        builder.setAction(BlockCommandProto.Action.INVALIDATE);
        break;
      case DatanodeProtocol.DNA_SHUTDOWN:
        builder.setAction(BlockCommandProto.Action.SHUTDOWN);
        break;
      default:
        throw new AssertionError("Invalid action");
    }
    Block[] blocks = cmd.getBlocks();
    for (int i = 0; i < blocks.length; i++) {
      builder.addBlocks(PBHelper.convert(blocks[i]));
    }
    builder.addAllTargets(PBHelper.convert(cmd.getTargets()));
    return builder.build();
  }

  private static List<DatanodeInfosProto> convert(DatanodeInfo[][] targets) {
    DatanodeInfosProto[] ret = new DatanodeInfosProto[targets.length];
    for (int i = 0; i < targets.length; i++) {
      ret[i] = DatanodeInfosProto.newBuilder()
          .addAllDatanodes(PBHelper.convert(targets[i])).build();
    }
    return Arrays.asList(ret);
  }

  public static DatanodeCommandProto convert(DatanodeCommand datanodeCommand) {
    DatanodeCommandProto.Builder builder = DatanodeCommandProto.newBuilder();
    if (datanodeCommand == null) {
      return builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand)
          .build();
    }
    switch (datanodeCommand.getAction()) {
      case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
        builder.setCmdType(DatanodeCommandProto.Type.BalancerBandwidthCommand)
            .setBalancerCmd(
                PBHelper.convert((BalancerBandwidthCommand) datanodeCommand));
        break;
      case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
        builder.setCmdType(DatanodeCommandProto.Type.KeyUpdateCommand)
            .setKeyUpdateCmd(
                PBHelper.convert((KeyUpdateCommand) datanodeCommand));
        break;
      case DatanodeProtocol.DNA_RECOVERBLOCK:
        builder.setCmdType(DatanodeCommandProto.Type.BlockRecoveryCommand)
            .setRecoveryCmd(
                PBHelper.convert((BlockRecoveryCommand) datanodeCommand));
        break;
      case DatanodeProtocol.DNA_FINALIZE:
        builder.setCmdType(DatanodeCommandProto.Type.FinalizeCommand)
            .setFinalizeCmd(
                PBHelper.convert((FinalizeCommand) datanodeCommand));
        break;
      case DatanodeProtocol.DNA_REGISTER:
        builder.setCmdType(DatanodeCommandProto.Type.RegisterCommand)
            .setRegisterCmd(REG_CMD_PROTO);
        break;
      case DatanodeProtocol.DNA_TRANSFER:
      case DatanodeProtocol.DNA_INVALIDATE:
      case DatanodeProtocol.DNA_SHUTDOWN:
        builder.setCmdType(DatanodeCommandProto.Type.BlockCommand)
            .setBlkCmd(PBHelper.convert((BlockCommand) datanodeCommand));
        break;
      case DatanodeProtocol.DNA_UNKNOWN: //Not expected
      default:
        builder.setCmdType(DatanodeCommandProto.Type.NullDatanodeCommand);
    }
    return builder.build();
  }

  public static KeyUpdateCommand convert(KeyUpdateCommandProto keyUpdateCmd) {
    return new KeyUpdateCommand(PBHelper.convert(keyUpdateCmd.getKeys()));
  }

  public static FinalizeCommand convert(FinalizeCommandProto finalizeCmd) {
    return new FinalizeCommand(finalizeCmd.getBlockPoolId());
  }

  public static BlockRecoveryCommand convert(
      BlockRecoveryCommandProto recoveryCmd) {
    List<RecoveringBlockProto> list = recoveryCmd.getBlocksList();
    List<RecoveringBlock> recoveringBlocks =
        new ArrayList<RecoveringBlock>(list.size());
    
    for (RecoveringBlockProto rbp : list) {
      recoveringBlocks.add(PBHelper.convert(rbp));
    }
    return new BlockRecoveryCommand(recoveringBlocks);
  }

  public static BlockCommand convert(BlockCommandProto blkCmd) {
    List<BlockProto> blockProtoList = blkCmd.getBlocksList();
    Block[] blocks = new Block[blockProtoList.size()];
    for (int i = 0; i < blockProtoList.size(); i++) {
      blocks[i] = PBHelper.convert(blockProtoList.get(i));
    }
    List<DatanodeInfosProto> targetList = blkCmd.getTargetsList();
    DatanodeInfo[][] targets = new DatanodeInfo[targetList.size()][];
    for (int i = 0; i < targetList.size(); i++) {
      targets[i] = PBHelper.convert(targetList.get(i));
    }

    StorageType[][] targetStorageTypes = new StorageType[targetList.size()][];
    List<HdfsProtos.StorageTypesProto> targetStorageTypesList = blkCmd.getTargetStorageTypesList();
    if (targetStorageTypesList.isEmpty()) { // missing storage types
      for(int i = 0; i < targetStorageTypes.length; i++) {
        targetStorageTypes[i] = new StorageType[targets[i].length];
        Arrays.fill(targetStorageTypes[i], StorageType.DEFAULT);
      }
    } else {
      for(int i = 0; i < targetStorageTypes.length; i++) {
        List<StorageTypeProto> p = targetStorageTypesList.get(i).getStorageTypesList();
        targetStorageTypes[i] = p.toArray(new StorageType[p.size()]);
      }
    }

    List<HdfsProtos.StorageUuidsProto> targetStorageUuidsList = blkCmd.getTargetStorageUuidsList();

    String[][] targetStorageIDs = new String[targetStorageUuidsList.size()][];
    for(int i = 0; i < targetStorageIDs.length; i++) {
      List<String> storageIDs = targetStorageUuidsList.get(i).getStorageUuidsList();
      targetStorageIDs[i] = storageIDs.toArray(new String[storageIDs.size()]);
    }

    int action = DatanodeProtocol.DNA_UNKNOWN;
    switch (blkCmd.getAction()) {
      case TRANSFER:
        action = DatanodeProtocol.DNA_TRANSFER;
        break;
      case INVALIDATE:
        action = DatanodeProtocol.DNA_INVALIDATE;
        break;
      case SHUTDOWN:
        action = DatanodeProtocol.DNA_SHUTDOWN;
        break;
      default:
        throw new AssertionError("Unknown action type: " + blkCmd.getAction());
    }
    return new BlockCommand(action, blkCmd.getBlockPoolId(), blocks, targets,
        targetStorageTypes, targetStorageIDs);
  }

  public static DatanodeInfo[] convert(DatanodeInfosProto datanodeInfosProto) {
    List<DatanodeInfoProto> proto = datanodeInfosProto.getDatanodesList();
    DatanodeInfo[] infos = new DatanodeInfo[proto.size()];
    for (int i = 0; i < infos.length; i++) {
      infos[i] = PBHelper.convert(proto.get(i));
    }
    return infos;
  }

  public static BalancerBandwidthCommand convert(
      BalancerBandwidthCommandProto balancerCmd) {
    return new BalancerBandwidthCommand(balancerCmd.getBandwidth());
  }

  public static ReceivedDeletedBlockInfoProto convert(
      ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
    ReceivedDeletedBlockInfoProto.Builder builder =
        ReceivedDeletedBlockInfoProto.newBuilder();
    
    ReceivedDeletedBlockInfoProto.BlockStatus status;
    switch (receivedDeletedBlockInfo.getStatus()) {
      case RECEIVING_BLOCK:
        status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVING;
        break;
      case RECEIVED_BLOCK:
        status = ReceivedDeletedBlockInfoProto.BlockStatus.RECEIVED;
        break;
      case DELETED_BLOCK:
        status = ReceivedDeletedBlockInfoProto.BlockStatus.DELETED;
        break;
      default:
        throw new IllegalArgumentException(
            "Bad status: " + receivedDeletedBlockInfo.getStatus());
    }
    builder.setStatus(status);
    
    if (receivedDeletedBlockInfo.getDelHints() != null) {
      builder.setDeleteHint(receivedDeletedBlockInfo.getDelHints());
    }
    return builder
        .setBlock(PBHelper.convert(receivedDeletedBlockInfo.getBlock()))
        .build();
  }

  public static ReceivedDeletedBlockInfo convert(
      ReceivedDeletedBlockInfoProto proto) {
    ReceivedDeletedBlockInfo.BlockStatus status = null;
    switch (proto.getStatus()) {
      case RECEIVING:
        status = BlockStatus.RECEIVING_BLOCK;
        break;
      case RECEIVED:
        status = BlockStatus.RECEIVED_BLOCK;
        break;
      case DELETED:
        status = BlockStatus.DELETED_BLOCK;
        break;
    }
    return new ReceivedDeletedBlockInfo(PBHelper.convert(proto.getBlock()),
        status, proto.hasDeleteHint() ? proto.getDeleteHint() : null);
  }
  
  public static NamespaceInfoProto convert(NamespaceInfo info) {
    return NamespaceInfoProto.newBuilder().setBlockPoolID(info.getBlockPoolID())
        .setBuildVersion(info.getBuildVersion()).setUnused(0)
        .setStorageInfo(PBHelper.convert((StorageInfo) info))
        .setSoftwareVersion(info.getSoftwareVersion()).build();
  }
  
  // Located Block Arrays and Lists
  public static LocatedBlockProto[] convertLocatedBlock(LocatedBlock[] lb) {
    if (lb == null) {
      return null;
    }
    return convertLocatedBlock2(Arrays.asList(lb))
        .toArray(new LocatedBlockProto[lb.length]);
  }
  
  public static LocatedBlock[] convertLocatedBlock(LocatedBlockProto[] lb) {
    if (lb == null) {
      return null;
    }
    return convertLocatedBlock(Arrays.asList(lb))
        .toArray(new LocatedBlock[lb.length]);
  }
  
  public static List<LocatedBlock> convertLocatedBlock(
      List<LocatedBlockProto> lb) {
    if (lb == null) {
      return null;
    }
    final int len = lb.size();
    List<LocatedBlock> result = new ArrayList<LocatedBlock>(len);
    for (int i = 0; i < len; ++i) {
      result.add(PBHelper.convert(lb.get(i)));
    }
    return result;
  }
  
  public static List<LocatedBlockProto> convertLocatedBlock2(
      List<LocatedBlock> lb) {
    if (lb == null) {
      return null;
    }
    final int len = lb.size();
    List<LocatedBlockProto> result = new ArrayList<LocatedBlockProto>(len);
    for (int i = 0; i < len; ++i) {
      result.add(PBHelper.convert(lb.get(i)));
    }
    return result;
  }
  
  
  // LocatedBlocks
  public static LocatedBlocks convert(LocatedBlocksProto lb) {
    return new LocatedBlocks(lb.getFileLength(), lb.getUnderConstruction(),
        PBHelper.convertLocatedBlock(lb.getBlocksList()),
        lb.hasLastBlock() ? PBHelper.convert(lb.getLastBlock()) : null,
        lb.getIsLastBlockComplete());
  }
  
  public static LocatedBlocksProto convert(LocatedBlocks lb) {
    if (lb == null) {
      return null;
    }
    LocatedBlocksProto.Builder builder = LocatedBlocksProto.newBuilder();
    if (lb.getLastLocatedBlock() != null) {
      builder.setLastBlock(PBHelper.convert(lb.getLastLocatedBlock()));
    }
    return builder.setFileLength(lb.getFileLength())
        .setUnderConstruction(lb.isUnderConstruction())
        .addAllBlocks(PBHelper.convertLocatedBlock2(lb.getLocatedBlocks()))
        .setIsLastBlockComplete(lb.isLastBlockComplete()).build();
  }
  
  // DataEncryptionKey
  public static DataEncryptionKey convert(DataEncryptionKeyProto bet) {
    String encryptionAlgorithm = bet.getEncryptionAlgorithm();
    return new DataEncryptionKey(bet.getKeyId(), bet.getBlockPoolId(),
        bet.getNonce().toByteArray(), bet.getEncryptionKey().toByteArray(),
        bet.getExpiryDate(),
        encryptionAlgorithm.isEmpty() ? null : encryptionAlgorithm);
  }
  
  public static DataEncryptionKeyProto convert(DataEncryptionKey bet) {
    DataEncryptionKeyProto.Builder b =
        DataEncryptionKeyProto.newBuilder().setKeyId(bet.keyId)
            .setBlockPoolId(bet.blockPoolId)
            .setNonce(ByteString.copyFrom(bet.nonce))
            .setEncryptionKey(ByteString.copyFrom(bet.encryptionKey))
            .setExpiryDate(bet.expiryDate);
    if (bet.encryptionAlgorithm != null) {
      b.setEncryptionAlgorithm(bet.encryptionAlgorithm);
    }
    return b.build();
  }
  
  public static FsServerDefaults convert(FsServerDefaultsProto fs) {
    if (fs == null) {
      return null;
    }
    return new FsServerDefaults(fs.getBlockSize(), fs.getBytesPerChecksum(),
        fs.getWritePacketSize(), (short) fs.getReplication(),
        fs.getFileBufferSize(), fs.getEncryptDataTransfer(),
        fs.getTrashInterval(), PBHelper.convert(fs.getChecksumType()));
  }
  
  public static FsServerDefaultsProto convert(FsServerDefaults fs) {
    if (fs == null) {
      return null;
    }
    return FsServerDefaultsProto.newBuilder().
        setBlockSize(fs.getBlockSize()).
        setBytesPerChecksum(fs.getBytesPerChecksum()).
        setWritePacketSize(fs.getWritePacketSize())
        .setReplication(fs.getReplication())
        .setFileBufferSize(fs.getFileBufferSize())
        .setEncryptDataTransfer(fs.getEncryptDataTransfer())
        .setTrashInterval(fs.getTrashInterval())
        .setChecksumType(PBHelper.convert(fs.getChecksumType())).build();
  }
  
  public static FsPermissionProto convert(FsPermission p) {
    if (p == null) {
      return null;
    }
    return FsPermissionProto.newBuilder().setPerm(p.toShort()).build();
  }
  
  public static FsPermission convert(FsPermissionProto p) {
    if (p == null) {
      return null;
    }
    return new FsPermission((short) p.getPerm());
  }
  
  
  // The creatFlag field in PB is a bitmask whose values are the same a the 
  // emum values of CreateFlag
  public static int convertCreateFlag(EnumSetWritable<CreateFlag> flag) {
    int value = 0;
    if (flag.contains(CreateFlag.APPEND)) {
      value |= CreateFlagProto.APPEND.getNumber();
    }
    if (flag.contains(CreateFlag.CREATE)) {
      value |= CreateFlagProto.CREATE.getNumber();
    }
    if (flag.contains(CreateFlag.OVERWRITE)) {
      value |= CreateFlagProto.OVERWRITE.getNumber();
    }
    return value;
  }
  
  public static EnumSetWritable<CreateFlag> convert(int flag) {
    EnumSet<CreateFlag> result = EnumSet.noneOf(CreateFlag.class);
    if ((flag & CreateFlagProto.APPEND_VALUE) == CreateFlagProto.APPEND_VALUE) {
      result.add(CreateFlag.APPEND);
    }
    if ((flag & CreateFlagProto.CREATE_VALUE) == CreateFlagProto.CREATE_VALUE) {
      result.add(CreateFlag.CREATE);
    }
    if ((flag & CreateFlagProto.OVERWRITE_VALUE) ==
        CreateFlagProto.OVERWRITE_VALUE) {
      result.add(CreateFlag.OVERWRITE);
    }
    return new EnumSetWritable<CreateFlag>(result);
  }
  
  
  public static HdfsFileStatus convert(HdfsFileStatusProto fs) {
    if (fs == null) {
      return null;
    }
    return new HdfsLocatedFileStatus(
        fs.getLength(),
        fs.getFileType().equals(FileType.IS_DIR),
        fs.getBlockReplication(),
        fs.getBlocksize(),
        fs.getModificationTime(),
        fs.getAccessTime(),
        PBHelper.convert(fs.getPermission()),
        fs.getOwner(),
        fs.getGroup(),
        fs.getFileType().equals(FileType.IS_SYMLINK) ? fs.getSymlink().toByteArray() : null,
        fs.getPath().toByteArray(),
        fs.hasLocations() ? PBHelper.convert(fs.getLocations()) : null,
        fs.hasStoragePolicy() ? (byte) fs.getStoragePolicy(): BlockStoragePolicySuite.ID_UNSPECIFIED);
  }

  public static HdfsFileStatusProto convert(HdfsFileStatus fs) {
    if (fs == null) {
      return null;
    }
    FileType fType = FileType.IS_FILE;
    if (fs.isDir()) {
      fType = FileType.IS_DIR;
    } else if (fs.isSymlink()) {
      fType = FileType.IS_SYMLINK;
    }

    HdfsFileStatusProto.Builder builder = HdfsFileStatusProto.newBuilder().
        setLength(fs.getLen()).
        setFileType(fType).
        setBlockReplication(fs.getReplication()).
        setBlocksize(fs.getBlockSize()).
        setModificationTime(fs.getModificationTime()).
        setAccessTime(fs.getAccessTime()).
        setPermission(PBHelper.convert(fs.getPermission())).
        setOwner(fs.getOwner()).
        setGroup(fs.getGroup()).
        setPath(ByteString.copyFrom(fs.getLocalNameInBytes())).
        setStoragePolicy(fs.getStoragePolicy());
    if (fs.isSymlink()) {
      builder.setSymlink(ByteString.copyFrom(fs.getSymlinkInBytes()));
    }
    if (fs instanceof HdfsLocatedFileStatus) {
      LocatedBlocks locations =
          ((HdfsLocatedFileStatus) fs).getBlockLocations();
      if (locations != null) {
        builder.setLocations(PBHelper.convert(locations));
      }
    }
    return builder.build();
  }
  
  public static HdfsFileStatusProto[] convert(HdfsFileStatus[] fs) {
    if (fs == null) {
      return null;
    }
    final int len = fs.length;
    HdfsFileStatusProto[] result = new HdfsFileStatusProto[len];
    for (int i = 0; i < len; ++i) {
      result[i] = PBHelper.convert(fs[i]);
    }
    return result;
  }
  
  public static HdfsFileStatus[] convert(HdfsFileStatusProto[] fs) {
    if (fs == null) {
      return null;
    }
    final int len = fs.length;
    HdfsFileStatus[] result = new HdfsFileStatus[len];
    for (int i = 0; i < len; ++i) {
      result[i] = PBHelper.convert(fs[i]);
    }
    return result;
  }
  
  public static DirectoryListing convert(DirectoryListingProto dl) {
    if (dl == null) {
      return null;
    }
    List<HdfsFileStatusProto> partList = dl.getPartialListingList();
    return new DirectoryListing(
        partList.isEmpty() ? new HdfsLocatedFileStatus[0] : PBHelper.convert(
            partList.toArray(new HdfsFileStatusProto[partList.size()])),
        dl.getRemainingEntries());
  }

  public static DirectoryListingProto convert(DirectoryListing d) {
    if (d == null) {
      return null;
    }
    return DirectoryListingProto.newBuilder().
        addAllPartialListing(
            Arrays.asList(PBHelper.convert(d.getPartialListing()))).
        setRemainingEntries(d.getRemainingEntries()).
        build();
  }

  public static long[] convert(GetFsStatsResponseProto res) {
    long[] result = new long[6];
    result[ClientProtocol.GET_STATS_CAPACITY_IDX] = res.getCapacity();
    result[ClientProtocol.GET_STATS_USED_IDX] = res.getUsed();
    result[ClientProtocol.GET_STATS_REMAINING_IDX] = res.getRemaining();
    result[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX] =
        res.getUnderReplicated();
    result[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX] =
        res.getCorruptBlocks();
    result[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX] =
        res.getMissingBlocks();
    return result;
  }
  
  public static GetFsStatsResponseProto convert(long[] fsStats) {
    GetFsStatsResponseProto.Builder result =
        GetFsStatsResponseProto.newBuilder();
    if (fsStats.length >= ClientProtocol.GET_STATS_CAPACITY_IDX + 1) {
      result.setCapacity(fsStats[ClientProtocol.GET_STATS_CAPACITY_IDX]);
    }
    if (fsStats.length >= ClientProtocol.GET_STATS_USED_IDX + 1) {
      result.setUsed(fsStats[ClientProtocol.GET_STATS_USED_IDX]);
    }
    if (fsStats.length >= ClientProtocol.GET_STATS_REMAINING_IDX + 1) {
      result.setRemaining(fsStats[ClientProtocol.GET_STATS_REMAINING_IDX]);
    }
    if (fsStats.length >= ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX + 1) {
      result.setUnderReplicated(
          fsStats[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX]);
    }
    if (fsStats.length >= ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX + 1) {
      result.setCorruptBlocks(
          fsStats[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX]);
    }
    if (fsStats.length >= ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX + 1) {
      result.setMissingBlocks(
          fsStats[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX]);
    }
    return result.build();
  }
  
  public static DatanodeReportTypeProto convert(DatanodeReportType t) {
    switch (t) {
      case ALL:
        return DatanodeReportTypeProto.ALL;
      case LIVE:
        return DatanodeReportTypeProto.LIVE;
      case DEAD:
        return DatanodeReportTypeProto.DEAD;
      default:
        throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }
  
  public static DatanodeReportType convert(DatanodeReportTypeProto t) {
    switch (t) {
      case ALL:
        return DatanodeReportType.ALL;
      case LIVE:
        return DatanodeReportType.LIVE;
      case DEAD:
        return DatanodeReportType.DEAD;
      default:
        throw new IllegalArgumentException("Unexpected data type report:" + t);
    }
  }

  public static SafeModeActionProto convert(SafeModeAction a) {
    switch (a) {
      case SAFEMODE_LEAVE:
        return SafeModeActionProto.SAFEMODE_LEAVE;
      case SAFEMODE_ENTER:
        return SafeModeActionProto.SAFEMODE_ENTER;
      case SAFEMODE_GET:
        return SafeModeActionProto.SAFEMODE_GET;
      default:
        throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }
  
  public static SafeModeAction convert(
      ClientNamenodeProtocolProtos.SafeModeActionProto a) {
    switch (a) {
      case SAFEMODE_LEAVE:
        return SafeModeAction.SAFEMODE_LEAVE;
      case SAFEMODE_ENTER:
        return SafeModeAction.SAFEMODE_ENTER;
      case SAFEMODE_GET:
        return SafeModeAction.SAFEMODE_GET;
      default:
        throw new IllegalArgumentException("Unexpected SafeModeAction :" + a);
    }
  }
  
  public static CorruptFileBlocks convert(CorruptFileBlocksProto c) {
    if (c == null) {
      return null;
    }
    List<String> fileList = c.getFilesList();
    return new CorruptFileBlocks(fileList.toArray(new String[fileList.size()]),
        c.getCookie());
  }

  public static CorruptFileBlocksProto convert(CorruptFileBlocks c) {
    if (c == null) {
      return null;
    }
    return CorruptFileBlocksProto.newBuilder().
        addAllFiles(Arrays.asList(c.getFiles())).
        setCookie(c.getCookie()).
        build();
  }
  
  public static ContentSummary convert(ContentSummaryProto cs) {
    if (cs == null) {
      return null;
    }
    return new ContentSummary(cs.getLength(), cs.getFileCount(),
        cs.getDirectoryCount(), cs.getQuota(), cs.getSpaceConsumed(),
        cs.getSpaceQuota());
  }
  
  public static ContentSummaryProto convert(ContentSummary cs) {
    if (cs == null) {
      return null;
    }
    return ContentSummaryProto.newBuilder().
        setLength(cs.getLength()).
        setFileCount(cs.getFileCount()).
        setDirectoryCount(cs.getDirectoryCount()).
        setQuota(cs.getQuota()).
        setSpaceConsumed(cs.getSpaceConsumed()).
        setSpaceQuota(cs.getSpaceQuota()).
        build();
  }

  public static DatanodeStorageProto convert(DatanodeStorage s) {
    return DatanodeStorageProto.newBuilder()
        .setState(PBHelper.convertState(s.getState()))
        .setStorageType(PBHelper.convertStorageType(s.getStorageType()))
        .setStorageUuid(s.getStorageID()).build();
  }

  private static StorageState convertState(State state) {
    switch (state) {
      case READ_ONLY:
        return StorageState.READ_ONLY;
      case NORMAL:
      default:
        return StorageState.NORMAL;
    }
  }

  public static List<StorageTypeProto> convertStorageTypes(
      StorageType[] types) {
    return convertStorageTypes(types, 0);
  }

  public static List<StorageTypeProto> convertStorageTypes(
      StorageType[] types, int startIdx) {
    if (types == null) {
      return null;
    }
    final List<StorageTypeProto> protos = new ArrayList<StorageTypeProto>(
        types.length);
    for (int i = startIdx; i < types.length; ++i) {
      protos.add(convertStorageType(types[i]));
    }
    return protos;
  }

  public static StorageTypeProto convertStorageType(StorageType type) {
    switch(type) {
      case DISK:
        return StorageTypeProto.DISK;
      case SSD:
        return StorageTypeProto.SSD;
      case RAID5:
        return StorageTypeProto.RAID5;
      case ARCHIVE:
        return StorageTypeProto.ARCHIVE;
      default:
        Preconditions.checkState(
            false,
            "Failed to update StorageTypeProto with new StorageType " +
                type.toString());
        return StorageTypeProto.DISK;
    }
  }

  public static StorageType convertStorageType(StorageTypeProto type) {
    switch(type) {
      case DISK:
        return StorageType.DISK;
      case SSD:
        return StorageType.SSD;
      case RAID5:
        return StorageType.RAID5;
      case ARCHIVE:
        return StorageType.ARCHIVE;
      default:
        throw new IllegalStateException(
            "BUG: StorageTypeProto not found, type=" + type);
    }
  }

  public static StorageType[] convertStorageTypes(
      List<StorageTypeProto> storageTypesList, int expectedSize) {
    final StorageType[] storageTypes = new StorageType[expectedSize];
    if (storageTypesList.size() != expectedSize) { // missing storage types
      Preconditions.checkState(storageTypesList.isEmpty());
      Arrays.fill(storageTypes, StorageType.DEFAULT);
    } else {
      for (int i = 0; i < storageTypes.length; ++i) {
        storageTypes[i] = convertStorageType(storageTypesList.get(i));
      }
    }
    return storageTypes;
  }

  public static DatanodeStorage convert(DatanodeStorageProto s) {
    return new DatanodeStorage(s.getStorageUuid(),
        PBHelper.convertState(s.getState()),
        PBHelper.convertStorageType(s.getStorageType()));
  }

  private static State convertState(StorageState state) {
    switch (state) {
      case READ_ONLY:
        return DatanodeStorage.State.READ_ONLY;
      case NORMAL:
      default:
        return DatanodeStorage.State.NORMAL;
    }
  }

  public static StorageReportProto convert(StorageReport r) {
    return StorageReportProto.newBuilder()
        .setBlockPoolUsed(r.getBlockPoolUsed()).setCapacity(r.getCapacity())
        .setDfsUsed(r.getDfsUsed()).setRemaining(r.getRemaining())
        .setStorageUuid(r.getStorage().getStorageID())
        .setStorage(convert(r.getStorage())).build();
  }

  public static StorageReport convert(StorageReportProto p) {
    /*
    TODO HDP_2.6
    Hadoop 2.6 sends a DatanodeStorageProto (optional) instead of the
    storageUuid.
    To support that, check whether p has a storage (p.hasStorage()) and use
    that one instead of the new DatanodeStorage(p.getStorageID()).
     */
//    return new StorageReport(
//            new DatanodeStorage(p.getStorageUuid()),
//        p.getFailed(), p.getCapacity(), p.getDfsUsed(), p.getRemaining(),
//        p.getBlockPoolUsed());

    return new StorageReport(
        p.hasStorage() ?
            convert(p.getStorage()) :
            new DatanodeStorage(p.getStorageUuid()),
        p.getFailed(), p.getCapacity(), p.getDfsUsed(), p.getRemaining(),
        p.getBlockPoolUsed());
  }

  public static StorageReport[] convertStorageReports(
      List<StorageReportProto> list) {
    final StorageReport[] report = new StorageReport[list.size()];
    for (int i = 0; i < report.length; i++) {
      report[i] = convert(list.get(i));
    }
    return report;
  }

  public static List<StorageReportProto> convertStorageReports(StorageReport[] storages) {
    final List<StorageReportProto> protos = new ArrayList<StorageReportProto>(
        storages.length);
    for(int i = 0; i < storages.length; i++) {
      protos.add(convert(storages[i]));
    }
    return protos;
  }

  public static DataChecksum.Type convert(HdfsProtos.ChecksumTypeProto type) {
    return DataChecksum.Type.valueOf(type.getNumber());
  }

  public static HdfsProtos.ChecksumTypeProto convert(DataChecksum.Type type) {
    return HdfsProtos.ChecksumTypeProto.valueOf(type.id);
  }

  public static InputStream vintPrefixed(final InputStream input)
      throws IOException {
    final int firstByte = input.read();
    if (firstByte == -1) {
      throw new EOFException("Premature EOF: no length prefix available");
    }

    int size = CodedInputStream.readRawVarint32(firstByte, input);
    assert size >= 0;
    return new ExactSizeInputStream(input, size);
  }
  
  //HOP_CODE_START
  public static SortedActiveNodeList convert(
      ActiveNamenodeListResponseProto p) {
    List<ActiveNode> anl = new ArrayList<ActiveNode>();
    List<ActiveNodeProto> anlp = p.getNamenodesList();
    for (int i = 0; i < anlp.size(); i++) {
      ActiveNode an = PBHelper.convert(anlp.get(i));
      anl.add(an);
    }
    return new SortedActiveNodeListPBImpl(anl);
  }
  
  public static ActiveNode convert(ActiveNodeProto p) {
    ActiveNode an =
        new ActiveNodePBImpl(p.getId(), p.getHostname(), p.getIpAddress(),
            p.getPort(), p.getHttpAddress());
    return an;
  }
  
  public static ActiveNodeProto convert(ActiveNode p) {
    ActiveNodeProto.Builder anp = ActiveNodeProto.newBuilder();
    anp.setId(p.getId());
    anp.setHostname(p.getHostname());
    anp.setIpAddress(p.getIpAddress());
    anp.setPort(p.getPort());
    anp.setHttpAddress(p.getHttpAddress());

    return anp.build();
  }
  
  public static ActiveNamenodeListResponseProto convert(
      SortedActiveNodeList anlWrapper) {
    List<ActiveNode> anl = anlWrapper.getActiveNodes();
    ActiveNamenodeListResponseProto.Builder anlrpb =
        ActiveNamenodeListResponseProto.newBuilder();
    for (int i = 0; i < anl.size(); i++) {
      ActiveNodeProto anp = PBHelper.convert(anl.get(i));
      anlrpb.addNamenodes(anp);
    }
    return anlrpb.build();
  }

  public static EncodingStatus convert(
      ClientNamenodeProtocolProtos.EncodingStatusProto encodingStatusProto) {
    EncodingStatus status = new EncodingStatus();
    if (encodingStatusProto.hasInodeId()) {
      status.setInodeId(encodingStatusProto.getInodeId());
    }
    if (encodingStatusProto.hasParityInodeId()) {
      status.setParityInodeId(encodingStatusProto.getParityInodeId());
    }
    status.setStatus(EncodingStatus.Status.values()[
        encodingStatusProto.getStatus()]);
    if (encodingStatusProto.hasParityStatus()) {
      status.setParityStatus(EncodingStatus.ParityStatus.values()[
          encodingStatusProto.getParityStatus()]);
    }
    if (encodingStatusProto.hasPolicy()) {
      status.setEncodingPolicy(
          PBHelper.convert(encodingStatusProto.getPolicy()));
    }
    if (encodingStatusProto.hasStatusModificationTime()) {
      status.setStatusModificationTime(
          encodingStatusProto.getStatusModificationTime());
    }
    if (encodingStatusProto.hasParityStatusModificationTime()) {
      status.setParityStatusModificationTime(
          encodingStatusProto.getParityStatusModificationTime());
    }
    if (encodingStatusProto.hasParityFileName()) {
      status.setParityFileName(encodingStatusProto.getParityFileName());
    }
    if (encodingStatusProto.hasLostBlocks()) {
      status.setLostBlocks(encodingStatusProto.getLostBlocks());
    }
    if (encodingStatusProto.hasLostParityBlocks()) {
      status.setLostParityBlocks(encodingStatusProto.getLostParityBlocks());
    }
    if (encodingStatusProto.hasRevoked()) {
      status.setRevoked(encodingStatusProto.getRevoked());
    }
    return status;
  }

  public static ClientNamenodeProtocolProtos.EncodingStatusProto convert(
      EncodingStatus encodingStatus) {
    ClientNamenodeProtocolProtos.EncodingStatusProto.Builder builder =
        ClientNamenodeProtocolProtos.EncodingStatusProto.newBuilder();
    if (encodingStatus.getInodeId() != null) {
      builder.setInodeId(encodingStatus.getInodeId());
    }
    if (encodingStatus.getParityInodeId() != null) {
      builder.setParityInodeId(encodingStatus.getParityInodeId());
    }
    builder.setStatus(encodingStatus.getStatus().ordinal());
    if (encodingStatus.getParityStatus() != null) {
      builder.setParityStatus(encodingStatus.getParityStatus().ordinal());
    }
    if (encodingStatus.getEncodingPolicy() != null) {
      builder.setPolicy(PBHelper.convert(
          encodingStatus.getEncodingPolicy()));
    }
    if (encodingStatus.getStatusModificationTime() != null) {
      builder.setStatusModificationTime(
          encodingStatus.getStatusModificationTime());
    }
    if (encodingStatus.getParityStatusModificationTime() != null) {
      builder.setParityStatusModificationTime(
          encodingStatus.getParityStatusModificationTime());
    }
    if (encodingStatus.getParityFileName() != null) {
      builder.setParityFileName(encodingStatus.getParityFileName());
    }
    if (encodingStatus.getLostBlocks() != null) {
      builder.setLostBlocks(encodingStatus.getLostBlocks());
    }
    if (encodingStatus.getLostParityBlocks() != null) {
      builder.setLostParityBlocks(encodingStatus.getLostParityBlocks());
    }
    if (encodingStatus.getRevoked() != null) {
      builder.setRevoked(encodingStatus.getRevoked());
    }
    return builder.build();
  }

  public static ClientNamenodeProtocolProtos.EncodingPolicyProto convert(
      EncodingPolicy encodingPolicy) {
    ClientNamenodeProtocolProtos.EncodingPolicyProto.Builder builder =
        ClientNamenodeProtocolProtos.EncodingPolicyProto.newBuilder();
    builder.setCodec(encodingPolicy.getCodec());
    builder.setTargetReplication(encodingPolicy.getTargetReplication());
    return builder.build();
  }

  public static EncodingPolicy convert(
      ClientNamenodeProtocolProtos.EncodingPolicyProto encodingPolicyProto) {
    return new EncodingPolicy(encodingPolicyProto.getCodec(),
        (short) encodingPolicyProto.getTargetReplication());
  }

  public static BlockStoragePolicy[] convertStoragePolicies(
      List<HdfsProtos.BlockStoragePolicyProto> policyProtos) {
    if (policyProtos == null || policyProtos.size() == 0) {
      return new BlockStoragePolicy[0];
    }
    BlockStoragePolicy[] policies = new BlockStoragePolicy[policyProtos.size()];
    int i = 0;
    for (HdfsProtos.BlockStoragePolicyProto proto : policyProtos) {
      policies[i++] = convert(proto);
    }
    return policies;
  }

  public static BlockStoragePolicy convert(HdfsProtos.BlockStoragePolicyProto proto) {
    List<StorageTypeProto> cList = proto.getCreationPolicy()
        .getStorageTypesList();
    StorageType[] creationTypes = convertStorageTypes(cList, cList.size());
    List<StorageTypeProto> cfList = proto.hasCreationFallbackPolicy() ? proto
        .getCreationFallbackPolicy().getStorageTypesList() : null;
    StorageType[] creationFallbackTypes = cfList == null ? StorageType
        .EMPTY_ARRAY : convertStorageTypes(cfList, cfList.size());
    List<StorageTypeProto> rfList = proto.hasReplicationFallbackPolicy() ?
        proto.getReplicationFallbackPolicy().getStorageTypesList() : null;
    StorageType[] replicationFallbackTypes = rfList == null ? StorageType
        .EMPTY_ARRAY : convertStorageTypes(rfList, rfList.size());
    return new BlockStoragePolicy((byte) proto.getPolicyId(), proto.getName(),
        creationTypes, creationFallbackTypes, replicationFallbackTypes);
  }

  public static HdfsProtos.BlockStoragePolicyProto convert(BlockStoragePolicy policy) {
    HdfsProtos.BlockStoragePolicyProto.Builder builder = HdfsProtos.BlockStoragePolicyProto
        .newBuilder().setPolicyId(policy.getId()).setName(policy.getName());
    // creation storage types
    HdfsProtos.StorageTypesProto creationProto = convert(policy.getStorageTypes());
    Preconditions.checkArgument(creationProto != null);
    builder.setCreationPolicy(creationProto);
    // creation fallback
    HdfsProtos.StorageTypesProto creationFallbackProto = convert(
        policy.getCreationFallbacks());
    if (creationFallbackProto != null) {
      builder.setCreationFallbackPolicy(creationFallbackProto);
    }
    // replication fallback
    HdfsProtos.StorageTypesProto replicationFallbackProto = convert(
        policy.getReplicationFallbacks());
    if (replicationFallbackProto != null) {
      builder.setReplicationFallbackPolicy(replicationFallbackProto);
    }
    return builder.build();
  }

  public static HdfsProtos.StorageTypesProto convert(StorageType[] types) {
    if (types == null || types.length == 0) {
      return null;
    }
    List<StorageTypeProto> list = convertStorageTypes(types);
    return HdfsProtos.StorageTypesProto.newBuilder().addAllStorageTypes(list).build();
  }
}
