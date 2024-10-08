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
import io.hops.metadata.hdfs.entity.EncodingStatus;
import java.util.EnumSet;

import io.hops.metadata.hdfs.entity.MetaStatus;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastUpdatedContentSummary;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AbandonBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddBlockResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.AppendResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CompleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ConcatResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CreateSymlinkResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.DeleteResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.FsyncResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.Builder;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetContentSummaryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetFsStatusRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLinkTargetResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetListingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLocatedFileInfoResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.IsFileClosedResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCachePoolsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MkdirsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.MsyncResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.PingResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RecoverLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RefreshNodesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2RequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.Rename2ResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenameResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RenewLeaseResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.RollingUpgradeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetOwnerResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetPermissionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetQuotaResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetReplicationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetSafeModeResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SetTimesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.TruncateRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.TruncateResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.UpdatePipelineResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CheckAccessResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLastUpdatedContentSummaryRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.GetLastUpdatedContentSummaryResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeIDProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.LocatedBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrResponseProto;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;



/**
 * This class is used on the server side. Calls come across the wire for the
 * for protocol {@link ClientNamenodeProtocolPB}.
 * This class translates the PB data types
 * to the native data types used inside the NN as specified in the generic
 * ClientProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ClientNamenodeProtocolServerSideTranslatorPB
    implements ClientNamenodeProtocolPB {
  final private ClientProtocol server;

  static final ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto
      VOID_SET_STORAGE_POLICY_RESPONSE =
      ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto.newBuilder().build();

  private static final CreateResponseProto VOID_CREATE_RESPONSE =
      CreateResponseProto.newBuilder().build();

  private static final SetPermissionResponseProto VOID_SET_PERM_RESPONSE =
      SetPermissionResponseProto.newBuilder().build();

  private static final SetOwnerResponseProto VOID_SET_OWNER_RESPONSE =
      SetOwnerResponseProto.newBuilder().build();

  private static final AbandonBlockResponseProto VOID_ADD_BLOCK_RESPONSE =
      AbandonBlockResponseProto.newBuilder().build();

  private static final ReportBadBlocksResponseProto
      VOID_REP_BAD_BLOCK_RESPONSE =
      ReportBadBlocksResponseProto.newBuilder().build();

  private static final ConcatResponseProto VOID_CONCAT_RESPONSE =
      ConcatResponseProto.newBuilder().build();

  private static final Rename2ResponseProto VOID_RENAME2_RESPONSE =
      Rename2ResponseProto.newBuilder().build();

  private static final GetListingResponseProto VOID_GETLISTING_RESPONSE =
      GetListingResponseProto.newBuilder().build();

  private static final RenewLeaseResponseProto VOID_RENEWLEASE_RESPONSE =
      RenewLeaseResponseProto.newBuilder().build();

  private static final RefreshNodesResponseProto VOID_REFRESHNODES_RESPONSE =
      RefreshNodesResponseProto.newBuilder().build();

  private static final GetFileInfoResponseProto VOID_GETFILEINFO_RESPONSE =
      GetFileInfoResponseProto.newBuilder().build();

  private static final GetLocatedFileInfoResponseProto
          VOID_GETLOCATEDFILEINFO_RESPONSE =
          GetLocatedFileInfoResponseProto.newBuilder().build();

  private static final GetFileLinkInfoResponseProto
      VOID_GETFILELINKINFO_RESPONSE =
      GetFileLinkInfoResponseProto.newBuilder().build();

  private static final SetQuotaResponseProto VOID_SETQUOTA_RESPONSE =
      SetQuotaResponseProto.newBuilder().build();

  private static final FsyncResponseProto VOID_FSYNC_RESPONSE =
      FsyncResponseProto.newBuilder().build();

  private static final SetTimesResponseProto VOID_SETTIMES_RESPONSE =
      SetTimesResponseProto.newBuilder().build();

  private static final CreateSymlinkResponseProto VOID_CREATESYMLINK_RESPONSE =
      CreateSymlinkResponseProto.newBuilder().build();

  private static final ClientNamenodeProtocolProtos.AddBlockChecksumResponseProto
      VOID_ADDBLOCKCHECKSUM_RESPONSE =
      ClientNamenodeProtocolProtos.AddBlockChecksumResponseProto.newBuilder()
          .build();

  private static final UpdatePipelineResponseProto
      VOID_UPDATEPIPELINE_RESPONSE =
      UpdatePipelineResponseProto.newBuilder().build();

  private static final CancelDelegationTokenResponseProto
      VOID_CANCELDELEGATIONTOKEN_RESPONSE =
      CancelDelegationTokenResponseProto.newBuilder().build();

  private static final SetBalancerBandwidthResponseProto
      VOID_SETBALANCERBANDWIDTH_RESPONSE =
      SetBalancerBandwidthResponseProto.newBuilder().build();

  private static final ClientNamenodeProtocolProtos.ChangeConfResponseProto
      VOID_CHANGECONF_RESPONSE =
      ClientNamenodeProtocolProtos.ChangeConfResponseProto.newBuilder().build();

  private static final ClientNamenodeProtocolProtos.SetMetaStatusResponseProto
      VOID_SET_META_ENABLED_RESPONSE =
      ClientNamenodeProtocolProtos.SetMetaStatusResponseProto.newBuilder()
          .build();

  private static final CheckAccessResponseProto
      VOID_CHECKACCESS_RESPONSE = CheckAccessResponseProto.getDefaultInstance();

  private static final SetAclResponseProto
    VOID_SETACL_RESPONSE = SetAclResponseProto.getDefaultInstance();

  private static final ModifyAclEntriesResponseProto
    VOID_MODIFYACLENTRIES_RESPONSE = ModifyAclEntriesResponseProto
      .getDefaultInstance();

  private static final RemoveAclEntriesResponseProto
    VOID_REMOVEACLENTRIES_RESPONSE = RemoveAclEntriesResponseProto
      .getDefaultInstance();

  private static final RemoveDefaultAclResponseProto
    VOID_REMOVEDEFAULTACL_RESPONSE = RemoveDefaultAclResponseProto
      .getDefaultInstance();

  private static final RemoveAclResponseProto
    VOID_REMOVEACL_RESPONSE = RemoveAclResponseProto.getDefaultInstance();
  
  private static final SetXAttrResponseProto
    VOID_SETXATTR_RESPONSE = SetXAttrResponseProto.getDefaultInstance();
  
  private static final RemoveXAttrResponseProto
    VOID_REMOVEXATTR_RESPONSE = RemoveXAttrResponseProto.getDefaultInstance();

  /**
   * Constructor
   *
   * @param server
   *     - the NN server
   * @throws IOException
   */
  public ClientNamenodeProtocolServerSideTranslatorPB(ClientProtocol server)
      throws IOException {
    this.server = server;
  }

  @Override
  public GetBlockLocationsResponseProto getBlockLocations(
      RpcController controller, GetBlockLocationsRequestProto req)
      throws ServiceException {
    try {
      LocatedBlocks b = server
          .getBlockLocations(req.getSrc(), req.getOffset(), req.getLength());
      Builder builder = GetBlockLocationsResponseProto.newBuilder();
      if (b != null) {
        builder.setLocations(PBHelper.convert(b)).build();
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetMissingBlockLocationsResponseProto getMissingBlockLocations(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetMissingBlockLocationsRequestProto req)
      throws ServiceException {
    try {
      LocatedBlocks b = server.getMissingBlockLocations(req.getFilePath());
      ClientNamenodeProtocolProtos.GetMissingBlockLocationsResponseProto.Builder
          builder =
          ClientNamenodeProtocolProtos.GetMissingBlockLocationsResponseProto
              .newBuilder();
      if (b != null) {
        builder.setLocations(PBHelper.convert(b)).build();
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddBlockChecksumResponseProto addBlockChecksum(
      RpcController controller,
      ClientNamenodeProtocolProtos.AddBlockChecksumRequestProto req)
      throws ServiceException {
    try {
      server.addBlockChecksum(req.getSrc(), req.getBlockIndex(),
          req.getChecksum());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ADDBLOCKCHECKSUM_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetBlockChecksumResponseProto getBlockChecksum(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetBlockChecksumRequestProto req)
      throws ServiceException {
    try {
      long checksum =
          server.getBlockChecksum(req.getSrc(), req.getBlockIndex());
      ClientNamenodeProtocolProtos.GetBlockChecksumResponseProto.Builder
          builder = ClientNamenodeProtocolProtos.GetBlockChecksumResponseProto
          .newBuilder().setChecksum(checksum);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetServerDefaultsResponseProto getServerDefaults(
      RpcController controller, GetServerDefaultsRequestProto req)
      throws ServiceException {
    try {
      FsServerDefaults result = server.getServerDefaults();
      return GetServerDefaultsResponseProto.newBuilder()
          .setServerDefaults(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateResponseProto create(RpcController controller,
      CreateRequestProto req) throws ServiceException {

    try {
      HdfsFileStatus result;
      if (req.hasPolicy()) {
        result = server.create(req.getSrc(), PBHelper.convert(req.getMasked()),
            req.getClientName(), PBHelper.convertCreateFlag(req.getCreateFlag()),
            req.getCreateParent(), (short) req.getReplication(),
            req.getBlockSize(),
            PBHelper.convertCryptoProtocolVersions(
              req.getCryptoProtocolVersionList()),
            PBHelper.convert(req.getPolicy()));
      } else {
        result = server.create(req.getSrc(), PBHelper.convert(req.getMasked()),
            req.getClientName(), PBHelper.convertCreateFlag(req.getCreateFlag()),
            req.getCreateParent(), (short) req.getReplication(),
            req.getBlockSize(),
            PBHelper.convertCryptoProtocolVersions(
              req.getCryptoProtocolVersionList()));
      }

      if (result != null) {
        return CreateResponseProto.newBuilder().setFs(PBHelper.convert(result))
            .build();
      }
      return VOID_CREATE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AppendResponseProto append(RpcController controller,
      AppendRequestProto req) throws ServiceException {
    try {
      EnumSetWritable<CreateFlag> flags = req.hasFlag() ?
          PBHelper.convertCreateFlag(req.getFlag()) :
          new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND));
      LastBlockWithStatus result = server.append(req.getSrc(),
          req.getClientName(), flags);
      AppendResponseProto.Builder builder = AppendResponseProto.newBuilder();
      if (result.getLastBlock() != null) {
        builder.setBlock(PBHelper.convert(result.getLastBlock()));
      }
      if (result.getFileStatus() != null) {
        builder.setStat(PBHelper.convert(result.getFileStatus()));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetReplicationResponseProto setReplication(RpcController controller,
      SetReplicationRequestProto req) throws ServiceException {
    try {
      boolean result =
          server.setReplication(req.getSrc(), (short) req.getReplication());
      return SetReplicationResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto setStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      server.setStoragePolicy(request.getSrc(), request.getPolicyName());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_STORAGE_POLICY_RESPONSE;
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto getStoragePolicy(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto request)
      throws ServiceException {
    try {
      BlockStoragePolicy policy = server.getStoragePolicy((byte)request.getStoragePolicyID());
      ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto.newBuilder();
      if (policy == null) {
        return builder.build();
      }
      builder.setPolicy(PBHelper.convert(policy));
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto getStoragePolicies(
      RpcController controller, ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto request)
      throws ServiceException {
    try {
      BlockStoragePolicy[] policies = server.getStoragePolicies();
      ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto.newBuilder();
      if (policies == null) {
        return builder.build();
      }
      for (BlockStoragePolicy policy : policies) {
        builder.addPolicies(PBHelper.convert(policy));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.SetMetaStatusResponseProto setMetaStatus(
      RpcController controller,
      ClientNamenodeProtocolProtos.SetMetaStatusRequestProto req)
      throws ServiceException {
    try {
      server.setMetaStatus(req.getSrc(), MetaStatus.fromVal((byte)req.getMetaStatus()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_META_ENABLED_RESPONSE;
  }


  @Override
  public SetPermissionResponseProto setPermission(RpcController controller,
      SetPermissionRequestProto req) throws ServiceException {
    try {
      server.setPermission(req.getSrc(), PBHelper.convert(req.getPermission()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_PERM_RESPONSE;
  }

  @Override
  public SetOwnerResponseProto setOwner(RpcController controller,
      SetOwnerRequestProto req) throws ServiceException {
    try {
      server
          .setOwner(req.getSrc(), req.hasUsername() ? req.getUsername() : null,
              req.hasGroupname() ? req.getGroupname() : null);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SET_OWNER_RESPONSE;
  }

  @Override
  public AbandonBlockResponseProto abandonBlock(RpcController controller,
      AbandonBlockRequestProto req) throws ServiceException {
    try {
      server.abandonBlock(PBHelper.convert(req.getB()), req.getFileId(),
          req.getSrc(), req.getHolder());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_ADD_BLOCK_RESPONSE;
  }

  @Override
  public AddBlockResponseProto addBlock(RpcController controller,
      AddBlockRequestProto req) throws ServiceException {

    try {
      List<DatanodeInfoProto> excl = req.getExcludeNodesList();
      List<String> favor = req.getFavoredNodesList();
      LocatedBlock result = server.addBlock(req.getSrc(), req.getClientName(),
          req.hasPrevious() ? PBHelper.convert(req.getPrevious()) : null,
          (excl == null || excl.size() == 0) ? null : PBHelper.convert(excl
              .toArray(new DatanodeInfoProto[excl.size()])), req.getFileId(),
          (favor == null || favor.size() == 0) ? null : favor
              .toArray(new String[favor.size()]));

      return AddBlockResponseProto.newBuilder()
          .setBlock(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetAdditionalDatanodeResponseProto getAdditionalDatanode(
      RpcController controller, GetAdditionalDatanodeRequestProto req)
      throws ServiceException {
    try {
      List<DatanodeInfoProto> existingList = req.getExistingsList();
      List<String> existingStorageIDsList = req.getExistingStorageUuidsList();
      List<DatanodeInfoProto> excludesList = req.getExcludesList();
      LocatedBlock result = server
          .getAdditionalDatanode(req.getSrc(), req.getFileId(), PBHelper.convert(req.getBlk()),
              PBHelper.convert(existingList.toArray(
                  new DatanodeInfoProto[existingList.size()])),
              existingStorageIDsList.toArray(
                  new String[existingStorageIDsList.size()]),
              PBHelper.convert(excludesList.toArray(
                  new DatanodeInfoProto[excludesList.size()])),
              req.getNumAdditionalNodes(), req.getClientName());
      return GetAdditionalDatanodeResponseProto.newBuilder()
          .setBlock(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CompleteResponseProto complete(RpcController controller,
      CompleteRequestProto req) throws ServiceException {
    try {
      boolean result = server.complete(req.getSrc(), req.getClientName(),
          req.hasLast() ? PBHelper.convert(req.getLast()) : null,
          req.hasFileId() ? req.getFileId() : HdfsConstantsClient.GRANDFATHER_INODE_ID,
          req.hasData() ? req.getData().toByteArray() : null);
      return CompleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ReportBadBlocksResponseProto reportBadBlocks(RpcController controller,
      ReportBadBlocksRequestProto req) throws ServiceException {
    try {
      List<LocatedBlockProto> bl = req.getBlocksList();
      server.reportBadBlocks(PBHelper
          .convertLocatedBlock(bl.toArray(new LocatedBlockProto[bl.size()])));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REP_BAD_BLOCK_RESPONSE;
  }

  @Override
  public ConcatResponseProto concat(RpcController controller,
      ConcatRequestProto req) throws ServiceException {
    try {
      List<String> srcs = req.getSrcsList();
      server.concat(req.getTrg(), srcs.toArray(new String[srcs.size()]));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_CONCAT_RESPONSE;
  }

  @Override
  public RenameResponseProto rename(RpcController controller,
      RenameRequestProto req) throws ServiceException {
    try {
      boolean result = server.rename(req.getSrc(), req.getDst());
      return RenameResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public Rename2ResponseProto rename2(RpcController controller,
      Rename2RequestProto req) throws ServiceException {

    try {
      Options.Rename[] options;
      if (req.hasKeepEncodingStatus() && req.getKeepEncodingStatus()) {
        options = new Rename[2];
        options[1] = Rename.KEEP_ENCODING_STATUS;
      } else {
        options = new Rename[1];
      }
      options[0] = req.getOverwriteDest() ? Rename.OVERWRITE : Rename.NONE;
      server.rename2(req.getSrc(), req.getDst(), options);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_RENAME2_RESPONSE;
  }

  @Override
  public TruncateResponseProto truncate(RpcController controller,
      TruncateRequestProto req) throws ServiceException {
    try {
      boolean result = server.truncate(req.getSrc(), req.getNewLength(),
          req.getClientName());
      return TruncateResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteResponseProto delete(RpcController controller,
      DeleteRequestProto req) throws ServiceException {
    try {
      boolean result = server.delete(req.getSrc(), req.getRecursive());
      return DeleteResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public MkdirsResponseProto mkdirs(RpcController controller,
      MkdirsRequestProto req) throws ServiceException {
    try {
      boolean result = server
          .mkdirs(req.getSrc(), PBHelper.convert(req.getMasked()),
              req.getCreateParent());
      return MkdirsResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetListingResponseProto getListing(RpcController controller,
      GetListingRequestProto req) throws ServiceException {
    try {
      DirectoryListing result = server
          .getListing(req.getSrc(), req.getStartAfter().toByteArray(),
              req.getNeedLocation());
      if (result != null) {
        return GetListingResponseProto.newBuilder()
            .setDirList(PBHelper.convert(result)).build();
      } else {
        return VOID_GETLISTING_RESPONSE;
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RenewLeaseResponseProto renewLease(RpcController controller,
      RenewLeaseRequestProto req) throws ServiceException {
    try {
      server.renewLease(req.getClientName());
      return VOID_RENEWLEASE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RecoverLeaseResponseProto recoverLease(RpcController controller,
      RecoverLeaseRequestProto req) throws ServiceException {
    try {
      boolean result = server.recoverLease(req.getSrc(), req.getClientName());
      return RecoverLeaseResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetFsStatsResponseProto getFsStats(RpcController controller,
      GetFsStatusRequestProto req) throws ServiceException {
    try {
      return PBHelper.convert(server.getStats());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDatanodeReportResponseProto getDatanodeReport(
      RpcController controller, GetDatanodeReportRequestProto req)
      throws ServiceException {
    try {
      List<? extends DatanodeInfoProto> result = PBHelper
          .convert(server.getDatanodeReport(PBHelper.convert(req.getType())));
      return GetDatanodeReportResponseProto.newBuilder().addAllDi(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto getDatanodeStorageReport(
      RpcController controller, ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto req)
      throws ServiceException {
    try {
      List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto> reports = PBHelper.convertDatanodeStorageReports(
          server.getDatanodeStorageReport(PBHelper.convert(req.getType())));
      return ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto.newBuilder()
          .addAllDatanodeStorageReports(reports)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetPreferredBlockSizeResponseProto getPreferredBlockSize(
      RpcController controller, GetPreferredBlockSizeRequestProto req)
      throws ServiceException {
    try {
      long result = server.getPreferredBlockSize(req.getFilename());
      return GetPreferredBlockSizeResponseProto.newBuilder().setBsize(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetSafeModeResponseProto setSafeMode(RpcController controller,
      SetSafeModeRequestProto req) throws ServiceException {
    try {
      boolean result = server
          .setSafeMode(PBHelper.convert(req.getAction()), req.getChecked());
      return SetSafeModeResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RefreshNodesResponseProto refreshNodes(RpcController controller,
      RefreshNodesRequestProto req) throws ServiceException {
    try {
      server.refreshNodes();
      return VOID_REFRESHNODES_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }

  }

  @Override
  public RollingUpgradeResponseProto rollingUpgrade(RpcController controller,
      RollingUpgradeRequestProto req) throws ServiceException {
    try {
      final RollingUpgradeInfo info = server.rollingUpgrade(
          PBHelper.convert(req.getAction()));
      final RollingUpgradeResponseProto.Builder b = RollingUpgradeResponseProto.newBuilder();
      if (info != null) {
        b.setRollingUpgradeInfo(PBHelper.convert(info));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
      RpcController controller, ListCorruptFileBlocksRequestProto req)
      throws ServiceException {
    try {
      CorruptFileBlocks result = server.listCorruptFileBlocks(req.getPath(),
          req.hasCookie() ? req.getCookie() : null);
      return ListCorruptFileBlocksResponseProto.newBuilder()
          .setCorrupt(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetFileInfoResponseProto getFileInfo(RpcController controller,
      GetFileInfoRequestProto req) throws ServiceException {
    try {
      HdfsFileStatus result = server.getFileInfo(req.getSrc());

      if (result != null) {
        return GetFileInfoResponseProto.newBuilder()
            .setFs(PBHelper.convert(result)).build();
      }
      return VOID_GETFILEINFO_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLocatedFileInfoResponseProto getLocatedFileInfo(
          RpcController controller, GetLocatedFileInfoRequestProto req)
          throws ServiceException {
    try {
      HdfsFileStatus result = server.getLocatedFileInfo(req.getSrc(),
              req.getNeedBlockToken());
      if (result != null) {
        return GetLocatedFileInfoResponseProto.newBuilder().setFs(
                PBHelper.convert(result)).build();
      }
      return VOID_GETLOCATEDFILEINFO_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetFileLinkInfoResponseProto getFileLinkInfo(RpcController controller,
      GetFileLinkInfoRequestProto req) throws ServiceException {
    try {
      HdfsFileStatus result = server.getFileLinkInfo(req.getSrc());
      if (result != null) {
        System.out.println(
            "got non null result for getFileLinkInfo for " + req.getSrc());
        return GetFileLinkInfoResponseProto.newBuilder()
            .setFs(PBHelper.convert(result)).build();
      } else {
        System.out.println(
            "got  null result for getFileLinkInfo for " + req.getSrc());
        return VOID_GETFILELINKINFO_RESPONSE;
      }

    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetContentSummaryResponseProto getContentSummary(
      RpcController controller, GetContentSummaryRequestProto req)
      throws ServiceException {
    try {
      ContentSummary result = server.getContentSummary(req.getPath());
      return GetContentSummaryResponseProto.newBuilder()
          .setSummary(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetQuotaResponseProto setQuota(RpcController controller,
      SetQuotaRequestProto req) throws ServiceException {
    try {
      server.setQuota(req.getPath(), req.getNamespaceQuota(),
          req.getStoragespaceQuota(),
          req.hasStorageType() ?
          PBHelper.convertStorageType(req.getStorageType()): null);
      return VOID_SETQUOTA_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FsyncResponseProto fsync(RpcController controller,
      FsyncRequestProto req) throws ServiceException {
    try {
      server.fsync(req.getSrc(), req.getFileId(),
          req.getClient(), req.getLastBlockLength());
      return VOID_FSYNC_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetTimesResponseProto setTimes(RpcController controller,
      SetTimesRequestProto req) throws ServiceException {
    try {
      server.setTimes(req.getSrc(), req.getMtime(), req.getAtime());
      return VOID_SETTIMES_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateSymlinkResponseProto createSymlink(RpcController controller,
      CreateSymlinkRequestProto req) throws ServiceException {
    try {
      server.createSymlink(req.getTarget(), req.getLink(),
          PBHelper.convert(req.getDirPerm()), req.getCreateParent());
      return VOID_CREATESYMLINK_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetLinkTargetResponseProto getLinkTarget(RpcController controller,
      GetLinkTargetRequestProto req) throws ServiceException {
    try {
      String result = server.getLinkTarget(req.getPath());
      GetLinkTargetResponseProto.Builder builder =
          GetLinkTargetResponseProto.newBuilder();
      if (result != null) {
        builder.setTargetPath(result);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdateBlockForPipelineResponseProto updateBlockForPipeline(
      RpcController controller, UpdateBlockForPipelineRequestProto req)
      throws ServiceException {
    try {
      LocatedBlockProto result = PBHelper.convert(server
          .updateBlockForPipeline(PBHelper.convert(req.getBlock()),
              req.getClientName()));
      return UpdateBlockForPipelineResponseProto.newBuilder().setBlock(result)
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdatePipelineResponseProto updatePipeline(RpcController controller,
      UpdatePipelineRequestProto req) throws ServiceException {
    try {
      List<DatanodeIDProto> newNodes = req.getNewNodesList();
      List<String> newStorageIDs = req.getStorageIDsList();
      server.updatePipeline(req.getClientName(),
          PBHelper.convert(req.getOldBlock()),
          PBHelper.convert(req.getNewBlock()),
          PBHelper.convert(
              newNodes.toArray(new DatanodeIDProto[newNodes.size()])),
          newStorageIDs.toArray(new String[newStorageIDs.size()]));
      return VOID_UPDATEPIPELINE_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, GetDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      Token<DelegationTokenIdentifier> token =
          server.getDelegationToken(new Text(req.getRenewer()));
      GetDelegationTokenResponseProto.Builder rspBuilder =
          GetDelegationTokenResponseProto.newBuilder();
      if (token != null) {
        rspBuilder.setToken(PBHelper.convert(token));
      }
      return rspBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RenewDelegationTokenResponseProto renewDelegationToken(
      RpcController controller, RenewDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      long result = server.renewDelegationToken(
          PBHelper.convertDelegationToken(req.getToken()));
      return RenewDelegationTokenResponseProto.newBuilder()
          .setNewExpiryTime(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CancelDelegationTokenResponseProto cancelDelegationToken(
      RpcController controller, CancelDelegationTokenRequestProto req)
      throws ServiceException {
    try {
      server.cancelDelegationToken(
          PBHelper.convertDelegationToken(req.getToken()));
      return VOID_CANCELDELEGATIONTOKEN_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SetBalancerBandwidthResponseProto setBalancerBandwidth(
      RpcController controller, SetBalancerBandwidthRequestProto req)
      throws ServiceException {
    try {
      server.setBalancerBandwidth(req.getBandwidth());
      return VOID_SETBALANCERBANDWIDTH_RESPONSE;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDataEncryptionKeyResponseProto getDataEncryptionKey(
      RpcController controller, GetDataEncryptionKeyRequestProto request)
      throws ServiceException {
    try {
      GetDataEncryptionKeyResponseProto.Builder builder =
          GetDataEncryptionKeyResponseProto.newBuilder();
      DataEncryptionKey encryptionKey = server.getDataEncryptionKey();
      if (encryptionKey != null) {
        builder.setDataEncryptionKey(PBHelper.convert(encryptionKey));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public IsFileClosedResponseProto isFileClosed(
      RpcController controller, IsFileClosedRequestProto request)
      throws ServiceException {
    try {
      boolean result = server.isFileClosed(request.getSrc());
      return IsFileClosedResponseProto.newBuilder().setResult(result).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.PingResponseProto ping(
      RpcController controller,
      ClientNamenodeProtocolProtos.PingRequestProto request)
      throws ServiceException {
    try {
      server.ping();
      PingResponseProto.Builder builder = PingResponseProto.newBuilder();
      return builder.build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEncodingStatusResponseProto getEncodingStatus(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetEncodingStatusRequestProto request)
      throws ServiceException {
    try {
      EncodingStatus status = server.getEncodingStatus(request.getPath());
      ClientNamenodeProtocolProtos.GetEncodingStatusResponseProto.Builder
          builder = ClientNamenodeProtocolProtos.GetEncodingStatusResponseProto
          .newBuilder();
      builder.setEncodingStatus(PBHelper.convert(status));
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.EncodeFileResponseProto encodeFile(
      RpcController controller,
      ClientNamenodeProtocolProtos.EncodeFileRequestProto request)
      throws ServiceException {
    try {
      server
          .encodeFile(request.getPath(), PBHelper.convert(request.getPolicy()));
      ClientNamenodeProtocolProtos.EncodeFileResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.EncodeFileResponseProto.newBuilder();
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RevokeEncodingResponseProto revokeEncoding(
      RpcController controller,
      ClientNamenodeProtocolProtos.RevokeEncodingRequestProto request)
      throws ServiceException {
    try {
      server
          .revokeEncoding(request.getPath(), (short) request.getReplication());
      ClientNamenodeProtocolProtos.RevokeEncodingResponseProto.Builder builder =
          ClientNamenodeProtocolProtos.RevokeEncodingResponseProto.newBuilder();
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetRepairedBlockLocationsResponseProto getRepairedBlockLocations(
      RpcController controller,
      ClientNamenodeProtocolProtos.GetRepairedBlockLocationsRequsestProto request)
      throws ServiceException {
    try {
      LocatedBlock b = server.getRepairedBlockLocations(request.getSourcePath(),
          request.getParityPath(), PBHelper.convert(request.getBlock()),
          request.getIsParity());
      ClientNamenodeProtocolProtos.GetRepairedBlockLocationsResponseProto.Builder
          builder =
          ClientNamenodeProtocolProtos.GetRepairedBlockLocationsResponseProto
              .newBuilder();
      if (b != null) {
        builder.setLocatedBlocks(PBHelper.convert(b)).build();
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.ActiveNamenodeListResponseProto getActiveNamenodesForClient(
      RpcController controller,
      ClientNamenodeProtocolProtos.ActiveNamenodeListRequestProto request)
      throws ServiceException {
    try {
      SortedActiveNodeList anl = server.getActiveNamenodesForClient();
      ClientNamenodeProtocolProtos.ActiveNamenodeListResponseProto response =
          convertANListToResponseProto(anl);
      return response;
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private ClientNamenodeProtocolProtos.ActiveNamenodeListResponseProto convertANListToResponseProto(
      SortedActiveNodeList anlWrapper) {
    List<ActiveNode> anl = anlWrapper.getActiveNodes();
    ClientNamenodeProtocolProtos.ActiveNamenodeListResponseProto.Builder
        anlrpb = ClientNamenodeProtocolProtos.ActiveNamenodeListResponseProto
        .newBuilder();
    for (ActiveNode anAnl : anl) {
      ActiveNodeProtos.ActiveNodeProto anp =
          convertANToResponseProto(anAnl);
      anlrpb.addNamenodes(anp);
    }
    return anlrpb.build();
  }

  private ActiveNodeProtos.ActiveNodeProto convertANToResponseProto(
      ActiveNode p) {
    ActiveNodeProtos.ActiveNodeProto.Builder anp =
        ActiveNodeProtos.ActiveNodeProto.newBuilder();
    anp.setId(p.getId());
    anp.setRpcHostname(p.getHostname());
    anp.setRpcIpAddress(p.getRpcServerIpAddress());
    anp.setRpcPort(p.getRpcServerPort());
    anp.setHttpAddress(p.getHttpAddress());
    anp.setServiceIpAddress(p.getServiceRpcIpAddress());
    anp.setServicePort(p.getServiceRpcPort());
    anp.setLocationDomainId(p.getLocationDomainId());
    return anp.build();
  }

  @Override
  public ClientNamenodeProtocolProtos.ChangeConfResponseProto changeConf(
      RpcController controller,
      ClientNamenodeProtocolProtos.ChangeConfProto request)
      throws ServiceException {
    try {
      server.changeConf(request.getPropsList(), request.getNewValsList());
      return VOID_CHANGECONF_RESPONSE;
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }


  @Override
  public CheckAccessResponseProto checkAccess(RpcController controller,
      CheckAccessRequestProto req) throws ServiceException {
    try {
      server.checkAccess(req.getPath(), PBHelper.convert(req.getMode()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_CHECKACCESS_RESPONSE;
  }


  @Override
  public GetLastUpdatedContentSummaryResponseProto getLastUpdatedContentSummary(
      RpcController controller, GetLastUpdatedContentSummaryRequestProto req)
      throws ServiceException {
    try {
      LastUpdatedContentSummary result = server.getLastUpdatedContentSummary(req.getPath());
      return GetLastUpdatedContentSummaryResponseProto.newBuilder()
          .setSummary(PBHelper.convert(result)).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyAclEntriesResponseProto modifyAclEntries(
      RpcController controller, ModifyAclEntriesRequestProto req)
      throws ServiceException {
    try {
      server.modifyAclEntries(req.getSrc(), PBHelper.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_MODIFYACLENTRIES_RESPONSE;
  }

  @Override
  public RemoveAclEntriesResponseProto removeAclEntries(
      RpcController controller, RemoveAclEntriesRequestProto req)
      throws ServiceException {
    try {
      server.removeAclEntries(req.getSrc(),
          PBHelper.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEACLENTRIES_RESPONSE;
  }

  @Override
  public RemoveDefaultAclResponseProto removeDefaultAcl(
      RpcController controller, RemoveDefaultAclRequestProto req)
      throws ServiceException {
    try {
      server.removeDefaultAcl(req.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEDEFAULTACL_RESPONSE;
  }

  @Override
  public RemoveAclResponseProto removeAcl(RpcController controller,
      RemoveAclRequestProto req) throws ServiceException {
    try {
      server.removeAcl(req.getSrc());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEACL_RESPONSE;
  }

  @Override
  public SetAclResponseProto setAcl(RpcController controller,
      SetAclRequestProto req) throws ServiceException {
    try {
      server.setAcl(req.getSrc(), PBHelper.convertAclEntry(req.getAclSpecList()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SETACL_RESPONSE;
  }

  @Override
  public GetAclStatusResponseProto getAclStatus(RpcController controller,
      GetAclStatusRequestProto req) throws ServiceException {
    try {
      return PBHelper.convert(server.getAclStatus(req.getSrc()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public CreateEncryptionZoneResponseProto createEncryptionZone(
    RpcController controller, CreateEncryptionZoneRequestProto req)
    throws ServiceException {
    try {
      server.createEncryptionZone(req.getSrc(), req.getKeyName());
      return CreateEncryptionZoneResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetEZForPathResponseProto getEZForPath(
      RpcController controller, GetEZForPathRequestProto req)
      throws ServiceException {
    try {
      GetEZForPathResponseProto.Builder builder =
          GetEZForPathResponseProto.newBuilder();
      final EncryptionZone ret = server.getEZForPath(req.getSrc());
      if (ret != null) {
        builder.setZone(PBHelper.convert(ret));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.GetEpochMSResponseProto getNNEpochMS(RpcController controller,
      ClientNamenodeProtocolProtos.GetEpochMSRequestProto request)
      throws ServiceException {
    try {
      return ClientNamenodeProtocolProtos.GetEpochMSResponseProto.newBuilder().
              setEpoch(server.getEpochMS()).build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ListEncryptionZonesResponseProto listEncryptionZones(
    RpcController controller, ListEncryptionZonesRequestProto req)
    throws ServiceException {
    try {
      BatchedEntries<EncryptionZone> entries = server
          .listEncryptionZones(req.getId());
      ListEncryptionZonesResponseProto.Builder builder =
          ListEncryptionZonesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i=0; i<entries.size(); i++) {
        builder.addZones(PBHelper.convert(entries.get(i)));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public SetXAttrResponseProto setXAttr(RpcController controller,
      SetXAttrRequestProto req) throws ServiceException {
    try {
      server.setXAttr(req.getSrc(), PBHelper.convertXAttr(req.getXAttr()),
          PBHelper.convert(req.getFlag()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_SETXATTR_RESPONSE;
  }
  
  @Override
  public GetXAttrsResponseProto getXAttrs(RpcController controller,
      GetXAttrsRequestProto req) throws ServiceException {
    try {
      return PBHelper.convertXAttrsResponse(server.getXAttrs(req.getSrc(),
          PBHelper.convertXAttrs(req.getXAttrsList())));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListXAttrsResponseProto listXAttrs(RpcController controller,
    ListXAttrsRequestProto req) throws ServiceException {
    try {
      return PBHelper.convertListXAttrsResponse(server.listXAttrs(req.getSrc()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public RemoveXAttrResponseProto removeXAttr(RpcController controller,
      RemoveXAttrRequestProto req) throws ServiceException {
    try {
      server.removeXAttr(req.getSrc(), PBHelper.convertXAttr(req.getXAttr()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VOID_REMOVEXATTR_RESPONSE;
  }

  @Override
  public MsyncResponseProto msync(RpcController controller,
                                  MsyncRequestProto req) throws ServiceException {
    try {
      server.msync();
      return MsyncResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AddCacheDirectiveResponseProto addCacheDirective(
      RpcController controller, AddCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      long id = server.addCacheDirective(
          PBHelper.convert(request.getInfo()),
          PBHelper.convertCacheFlags(request.getCacheFlags()));
      return AddCacheDirectiveResponseProto.newBuilder().
          setId(id).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyCacheDirectiveResponseProto modifyCacheDirective(
      RpcController controller, ModifyCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      server.modifyCacheDirective(
          PBHelper.convert(request.getInfo()),
          PBHelper.convertCacheFlags(request.getCacheFlags()));
      return ModifyCacheDirectiveResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveCacheDirectiveResponseProto
      removeCacheDirective(RpcController controller,
          RemoveCacheDirectiveRequestProto request)
      throws ServiceException {
    try {
      server.removeCacheDirective(request.getId());
      return RemoveCacheDirectiveResponseProto.
          newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListCacheDirectivesResponseProto listCacheDirectives(
      RpcController controller, ListCacheDirectivesRequestProto request)
      throws ServiceException {
    try {
      CacheDirectiveInfo filter = PBHelper.convert(request.getFilter());
      BatchedEntries<CacheDirectiveEntry> entries = server.listCacheDirectives(request.getPrevId(), filter);
      ListCacheDirectivesResponseProto.Builder builder = ListCacheDirectivesResponseProto.newBuilder();
      builder.setHasMore(entries.hasMore());
      for (int i = 0, n = entries.size(); i < n; i++) {
        builder.addElements(PBHelper.convert(entries.get(i)));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public AddCachePoolResponseProto addCachePool(RpcController controller,
      AddCachePoolRequestProto request) throws ServiceException {
    try {
      server.addCachePool(PBHelper.convert(request.getInfo()));
      return AddCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyCachePoolResponseProto modifyCachePool(RpcController controller,
      ModifyCachePoolRequestProto request) throws ServiceException {
    try {
      server.modifyCachePool(PBHelper.convert(request.getInfo()));
      return ModifyCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public RemoveCachePoolResponseProto removeCachePool(RpcController controller,
      RemoveCachePoolRequestProto request) throws ServiceException {
    try {
      server.removeCachePool(request.getPoolName());
      return RemoveCachePoolResponseProto.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListCachePoolsResponseProto listCachePools(RpcController controller,
      ListCachePoolsRequestProto request) throws ServiceException {
    try {
      BatchedEntries<CachePoolEntry> entries = server.listCachePools(request.getPrevPoolName());
      ListCachePoolsResponseProto.Builder responseBuilder = ListCachePoolsResponseProto.newBuilder();
      responseBuilder.setHasMore(entries.hasMore());
      for (int i = 0, n = entries.size(); i < n; i++) {
        responseBuilder.addEntries(PBHelper.convert(entries.get(i)));
      }
      return responseBuilder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddUserResponseProto addUser(RpcController controller, ClientNamenodeProtocolProtos.AddUserRequestProto request) throws ServiceException {
    try {
      server.addUser(request.hasUserName() ? request.getUserName() : null);
      return ClientNamenodeProtocolProtos.AddUserResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddGroupResponseProto addGroup(RpcController controller, ClientNamenodeProtocolProtos.AddGroupRequestProto request) throws ServiceException {
    try {
      server.addGroup(request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.AddGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.AddUserToGroupResponseProto addUserToGroup(RpcController controller, ClientNamenodeProtocolProtos.AddUserToGroupRequestProto request) throws ServiceException {
    try {
      server.addUserToGroup(request.hasUserName() ? request.getUserName() : null,
              request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.AddUserToGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveUserResponseProto removeUser(RpcController controller, ClientNamenodeProtocolProtos.RemoveUserRequestProto request) throws ServiceException {
    try {
      server.removeUser(request.hasUserName() ? request.getUserName() : null);
      return ClientNamenodeProtocolProtos.RemoveUserResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveGroupResponseProto removeGroup(RpcController controller, ClientNamenodeProtocolProtos.RemoveGroupRequestProto request) throws ServiceException {
    try {
      server.removeGroup(request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.RemoveGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.RemoveUserFromGroupResponseProto removeUserFromGroup(RpcController controller, ClientNamenodeProtocolProtos.RemoveUserFromGroupRequestProto request) throws ServiceException {
    try {
      server.removeUserFromGroup(request.hasUserName() ? request.getUserName() : null,
              request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.RemoveUserFromGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.InvCachesUserRemovedResponseProto invCachesUserRemoved(RpcController controller, ClientNamenodeProtocolProtos.InvCachesUserRemovedRequestProto request) throws ServiceException {
    try {
      server.invCachesUserRemoved(request.hasUserName() ? request.getUserName() : null);
      return ClientNamenodeProtocolProtos.InvCachesUserRemovedResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.InvCachesGroupRemovedResponseProto invCachesGroupRemoved(RpcController controller, ClientNamenodeProtocolProtos.InvCachesGroupRemovedRequestProto request) throws ServiceException {
    try {
      server.invCachesGroupRemoved(request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.InvCachesGroupRemovedResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.InvCachesUserRemovedFromGroupResponseProto invCachesUserRemovedFromGroup(RpcController controller, ClientNamenodeProtocolProtos.InvCachesUserRemovedFromGroupRequestProto request) throws ServiceException {
    try {
      server.invCachesUserRemovedFromGroup(request.hasUserName() ? request.getUserName() : null,
              request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.InvCachesUserRemovedFromGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  @Override
  public ClientNamenodeProtocolProtos.InvCachesUserAddedToGroupResponseProto invCachesUserAddedToGroup(RpcController controller, ClientNamenodeProtocolProtos.InvCachesUserAddedToGroupRequestProto request) throws ServiceException {
    try {
      server.invCachesUserAddedToGroup(request.hasUserName() ? request.getUserName() : null,
              request.hasGroupName() ? request.getGroupName() : null);
      return ClientNamenodeProtocolProtos.InvCachesUserAddedToGroupResponseProto.newBuilder().build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

}
