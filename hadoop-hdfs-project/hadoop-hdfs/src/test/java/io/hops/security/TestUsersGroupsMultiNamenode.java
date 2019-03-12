/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.security;


import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClientAdapter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import static org.junit.Assert.*;

public class TestUsersGroupsMultiNamenode {

  static UsersGroupsCache newUsersGroupsMapping(Configuration conf) throws IOException {
    return new UsersGroupsCache((UserDataAccess)
            HdfsStorageFactory.getDataAccess(UserDataAccess.class),
            (UserGroupDataAccess) HdfsStorageFactory.getDataAccess(
                    UserGroupDataAccess.class), (GroupDataAccess) HdfsStorageFactory.getDataAccess(
            GroupDataAccess.class), conf.getInt(CommonConfigurationKeys
            .HOPS_UG_CACHE_SECS, CommonConfigurationKeys
            .HOPS_UG_CACHE_SECS_DEFAULT), conf.getInt(CommonConfigurationKeys
            .HOPS_UG_CACHE_SIZE, CommonConfigurationKeys
            .HOPS_UG_CACHE_SIZE_DEFUALT));
  }

  private class ClientProtocolMock implements ClientProtocol {

    private final int id;
    private final UsersGroupsCache usersGroupsMapping;

    public ClientProtocolMock(int id, UsersGroupsCache usersGroupsMapping) throws IOException {
      this.id = id;
      this.usersGroupsMapping = usersGroupsMapping;
      UsersGroups.createSyncRow();
    }

    @Override
    public LocatedBlocks getBlockLocations(String src, long offset, long length)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public LocatedBlocks getMissingBlockLocations(String filePath)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public void addBlockChecksum(String src, int blockIndex, long checksum)
        throws IOException {

    }

    @Override
    public long getBlockChecksum(String src, int blockIndex)
        throws IOException {
      return 0;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
      return null;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked,
        String clientName, EnumSetWritable<CreateFlag> flag,
        boolean createParent, short replication, long blockSize,
        EncodingPolicy policy)
        throws AccessControlException, AlreadyBeingCreatedException,
        DSQuotaExceededException, FileAlreadyExistsException,
        FileNotFoundException, NSQuotaExceededException,
        ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
        IOException {
      return null;
    }

    @Override
    public HdfsFileStatus create(String src, FsPermission masked,
        String clientName, EnumSetWritable<CreateFlag> flag,
        boolean createParent, short replication, long blockSize)
        throws AccessControlException, AlreadyBeingCreatedException,
        DSQuotaExceededException, FileAlreadyExistsException,
        FileNotFoundException, NSQuotaExceededException,
        ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
        IOException {
      return null;
    }

    @Override
    public LastBlockWithStatus append(String src, String clientName, EnumSetWritable<CreateFlag> flag)
        throws AccessControlException, DSQuotaExceededException,
        FileNotFoundException, SafeModeException, UnresolvedLinkException,
        IOException {
      return null;
    }

    @Override
    public boolean setReplication(String src, short replication)
        throws AccessControlException, DSQuotaExceededException,
        FileNotFoundException, SafeModeException, UnresolvedLinkException,
        IOException {
      return false;
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(byte storagePolicyID)
        throws IOException {
      return null;
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
      return new BlockStoragePolicy[0];
    }

    @Override
    public void setStoragePolicy(String src, String policyName)
        throws UnresolvedLinkException, FileNotFoundException,
        QuotaExceededException, IOException {

    }

    @Override
    public void setMetaEnabled(String src, boolean metaEnabled)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void setPermission(String src, FsPermission permission)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void setOwner(String src, String username, String groupname)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public LocatedBlock addBlock(String src, String clientName,
        ExtendedBlock previous, DatanodeInfo[] excludeNodes, long fileId,
        String[] favoredNodes)
        throws AccessControlException, FileNotFoundException,
        NotReplicatedYetException, SafeModeException, UnresolvedLinkException,
        IOException {
      return null;
    }

    @Override
    public LocatedBlock getAdditionalDatanode(String src,final long fileId,
        ExtendedBlock blk, DatanodeInfo[] existings,
        String[] existingStorageIDs, DatanodeInfo[] excludes,
        int numAdditionalNodes, String clientName)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public boolean complete(String src, String clientName, ExtendedBlock last,
        long fileId, byte[] data)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {
      return false;
    }

    @Override
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {

    }

    @Override
    public boolean rename(String src, String dst)
        throws UnresolvedLinkException, IOException {
      return false;
    }

    @Override
    public void concat(String trg, String[] srcs)
        throws IOException, UnresolvedLinkException {

    }

    @Override
    public void rename2(String src, String dst, Options.Rename... options)
        throws AccessControlException, DSQuotaExceededException,
        FileAlreadyExistsException, FileNotFoundException,
        NSQuotaExceededException, ParentNotDirectoryException,
        SafeModeException, UnresolvedLinkException, IOException {

    }

    @Override
    public boolean truncate(String src, long newLength, String clientName)
      throws AccessControlException, FileNotFoundException, SafeModeException,
      UnresolvedLinkException, IOException{
      return false;
    }

    @Override
    public boolean delete(String src, boolean recursive)
        throws AccessControlException, FileNotFoundException, SafeModeException,
        UnresolvedLinkException, IOException {
      return false;
    }

    @Override
    public boolean mkdirs(String src, FsPermission masked, boolean createParent)
        throws AccessControlException, FileAlreadyExistsException,
        FileNotFoundException, NSQuotaExceededException,
        ParentNotDirectoryException, SafeModeException, UnresolvedLinkException,
        IOException {
      return false;
    }

    @Override
    public DirectoryListing getListing(String src, byte[] startAfter,
        boolean needLocation)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public void renewLease(String clientName)
        throws AccessControlException, IOException {

    }

    @Override
    public boolean recoverLease(String src, String clientName)
        throws IOException {
      return false;
    }

    @Override
    public long[] getStats() throws IOException {
      return new long[0];
    }

    @Override
    public DatanodeInfo[] getDatanodeReport(
        HdfsConstants.DatanodeReportType type) throws IOException {
      return new DatanodeInfo[0];
    }

    @Override
    public DatanodeStorageReport[] getDatanodeStorageReport(
        HdfsConstants.DatanodeReportType type) throws IOException {
      return new DatanodeStorageReport[0];
    }

    @Override
    public long getPreferredBlockSize(String filename)
        throws IOException, UnresolvedLinkException {
      return 0;
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action,
        boolean isChecked) throws IOException {
      return false;
    }

    @Override
    public void refreshNodes() throws IOException {

    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(
        HdfsConstants.RollingUpgradeAction action) throws IOException {
      return null;
    }

    @Override
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
        throws IOException {
      return null;
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {

    }

    @Override
    public HdfsFileStatus getFileInfo(String src)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public boolean isFileClosed(String src)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return false;
    }

    @Override
    public HdfsFileStatus getFileLinkInfo(String src)
        throws AccessControlException, UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public ContentSummary getContentSummary(String path)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public void setQuota(String path, long namespaceQuota, long diskspaceQuota, StorageType type)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void fsync(String src, long inodeId, String client, long lastBlockLength)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void setTimes(String src, long mtime, long atime)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public void createSymlink(String target, String link, FsPermission dirPerm,
        boolean createParent)
        throws AccessControlException, FileAlreadyExistsException,
        FileNotFoundException, ParentNotDirectoryException, SafeModeException,
        UnresolvedLinkException, IOException {

    }

    @Override
    public String getLinkTarget(String path)
        throws AccessControlException, FileNotFoundException, IOException {
      return null;
    }

    @Override
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block,
        String clientName) throws IOException {
      return null;
    }

    @Override
    public void updatePipeline(String clientName, ExtendedBlock oldBlock,
        ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorages)
        throws IOException {

    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
        throws IOException {
      return null;
    }

    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
        throws IOException {
      return 0;
    }

    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
        throws IOException {

    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
      return null;
    }

    @Override
    public void ping() throws IOException {

    }

    @Override
    public SortedActiveNodeList getActiveNamenodesForClient()
        throws IOException {
      return null;
    }

    @Override
    public void changeConf(List<String> props, List<String> newVals)
        throws IOException {

    }

    @Override
    public EncodingStatus getEncodingStatus(String filePath)
        throws IOException {
      return null;
    }

    @Override
    public void encodeFile(String filePath, EncodingPolicy policy)
        throws IOException {

    }

    @Override
    public void revokeEncoding(String filePath, short replication)
        throws IOException {

    }

    @Override
    public LocatedBlock getRepairedBlockLocations(String sourcePath,
        String parityPath, LocatedBlock block, boolean isParity)
        throws IOException {
      return null;
    }

    @Override
    public void checkAccess(String path, FsAction mode) throws IOException {

    }

    @Override
    public LastUpdatedContentSummary getLastUpdatedContentSummary(String path)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      return null;
    }

    @Override
    public void modifyAclEntries(String src, List<AclEntry> aclSpec)
        throws IOException {

    }

    @Override
    public void removeAclEntries(String src, List<AclEntry> aclSpec)
        throws IOException {

    }

    @Override
    public void removeDefaultAcl(String src) throws IOException {

    }

    @Override
    public void removeAcl(String src) throws IOException {

    }

    @Override
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {

    }

    @Override
    public AclStatus getAclStatus(String src) throws IOException {
      return null;
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo directive,
        EnumSet<CacheFlag> flags) throws IOException {
      return 0;
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo directive,
        EnumSet<CacheFlag> flags) throws IOException {

    }

    @Override
    public void removeCacheDirective(long id) throws IOException {

    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(
        long prevId, CacheDirectiveInfo filter) throws IOException {
      return null;
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {

    }

    @Override
    public void modifyCachePool(CachePoolInfo req) throws IOException {

    }

    @Override
    public void removeCachePool(String pool) throws IOException {

    }

    @Override
    public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(
        String prevPool) throws IOException {
      return null;
    }

    @Override
    public void addUser(String userName) throws IOException {
      usersGroupsMapping.addUser(userName);
    }

    @Override
    public void addGroup(String groupName) throws IOException {
      usersGroupsMapping.addGroup(groupName);
    }

    @Override
    public void addUserToGroup(String userName, String groupName) throws IOException {
      usersGroupsMapping.addUserToGroup(userName, groupName);
    }

    @Override
    public void removeUser(String userName) throws IOException {
      usersGroupsMapping.removeUser(userName);
    }

    @Override
    public void removeGroup(String groupName) throws IOException {
      usersGroupsMapping.removeGroup(groupName);
    }

    @Override
    public void removeUserFromGroup(String userName, String groupName) throws IOException {
      usersGroupsMapping.removeUserFromGroup(userName, groupName);
    }

    @Override
    public void invCachesUserRemoved(String userName) throws IOException {
      usersGroupsMapping.invCacheUserRemoved(userName);
    }

    @Override
    public void invCachesGroupRemoved(String groupName) throws IOException {
      usersGroupsMapping.invCachesGroupRemoved(groupName);
    }

    @Override
    public void invCachesUserRemovedFromGroup(String userName, String groupName) throws IOException {
      usersGroupsMapping.invCachesUserRemovedFromGroup(userName, groupName);
    }

    @Override
    public void invCachesUserAddedToGroup(String userName, String groupName) throws IOException {
      usersGroupsMapping.invCacheUserAddedToGroup(userName, groupName);
    }

    void clearCache(){
      usersGroupsMapping.clear();
    }
  }


  @Test
  public void testMultiNamenode() throws Exception {
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    List<ClientProtocol> namenodes = new ArrayList<>();

    ClientProtocolMock nn1 = new ClientProtocolMock(1, newUsersGroupsMapping(conf));

    namenodes.add(nn1);

    ClientProtocolMock nn2 = new ClientProtocolMock(2, newUsersGroupsMapping(conf));

    namenodes.add(nn2);

    ClientProtocolMock nn3 = new ClientProtocolMock(3, newUsersGroupsMapping(conf));

    namenodes.add(nn3);

    DistributedFileSystem dfs1 =
        DFSClientAdapter
            .newDistributedFileSystem(conf, nn1, namenodes);

    DistributedFileSystem dfs2 =
        DFSClientAdapter.newDistributedFileSystem(conf, nn2,
            namenodes);

    DistributedFileSystem dfs3 =
        DFSClientAdapter.newDistributedFileSystem(conf, nn3,
            namenodes);


    //Adding user
    String user1 = "user1";
    dfs1.addUser(user1);

    Integer user1Id = nn1.usersGroupsMapping.getUserId(user1);
    assertNotSame(0, user1Id);

    assertTrue(nn2.usersGroupsMapping.getUserId(user1) == user1Id);
    assertTrue(nn3.usersGroupsMapping.getUserId(user1) == user1Id);

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    //Adding group

    String group1 = "group1";
    dfs1.addGroup(group1);

    Integer group1Id = nn1.usersGroupsMapping.getGroupId(group1);
    assertNotSame(0, group1Id);

    assertTrue(nn2.usersGroupsMapping.getGroupId(group1) == group1Id);
    assertTrue(nn3.usersGroupsMapping.getGroupId(group1) == group1Id);

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    //Add a user to a group
    dfs2.addUserToGroup(user1, group1);

    List<String> groups = nn2.usersGroupsMapping.getGroups(user1);
    assertNotNull(groups);
    assertEquals(Arrays.asList(group1), groups);

    assertNotNull(nn1.usersGroupsMapping.getGroups(user1));
    assertNotNull(nn3.usersGroupsMapping.getGroups(user1));

    assertEquals(Arrays.asList(group1),
        nn1.usersGroupsMapping.getGroups(user1));
    assertEquals(Arrays.asList(group1),
        nn3.usersGroupsMapping.getGroups(user1));

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    //Add the user to a new group
    String group2 = "group2";
    dfs1.addGroup(group2);
    dfs3.addUserToGroup(user1, group2);
    groups = nn3.usersGroupsMapping.getGroups(user1);
    assertNotNull(groups);
    assertThat(groups, Matchers.containsInAnyOrder(group1, group2));

    assertThat(nn1.usersGroupsMapping.getGroups(user1),
        Matchers.containsInAnyOrder(group1, group2));

    assertThat(nn2.usersGroupsMapping.getGroups(user1),
        Matchers.containsInAnyOrder(group1, group2));

    int group2Id =  nn1.usersGroupsMapping.getGroupId(group2);
    assertEquals(nn2.usersGroupsMapping.getGroupId(group2), (int)group2Id);
    assertEquals(nn3.usersGroupsMapping.getGroupId(group2), (int)group2Id);

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    //remove user-group
    dfs1.removeUserFromGroup(user1, group2);

    groups = nn1.usersGroupsMapping.getGroups(user1);
    assertNotNull(groups);
    assertEquals(Arrays.asList(group1), groups);

    assertEquals(Arrays.asList(group1),
        nn2.usersGroupsMapping.getGroups(user1));

    assertEquals(Arrays.asList(group1),
        nn3.usersGroupsMapping.getGroups(user1));

    //remove group1
    dfs2.removeGroup(group1);
    try {
      assertEquals(0, nn1.usersGroupsMapping.getGroupId(group1));
    } catch (GroupNotFoundException e){
    }

    try {
      assertEquals(0, nn2.usersGroupsMapping.getGroupId(group1));
    } catch (GroupNotFoundException e){
    }

    try {
      assertEquals(0, nn3.usersGroupsMapping.getGroupId(group1));
    } catch (GroupNotFoundException e){
    }

    // user1 has no groups associated
    try {
      assertNull(nn1.usersGroupsMapping.getGroups(user1));
    } catch (GroupsNotFoundForUserException e){
    }

    try {
      assertNull(nn2.usersGroupsMapping.getGroups(user1));
    } catch (GroupsNotFoundForUserException e){
    }

    try {
      assertNull(nn3.usersGroupsMapping.getGroups(user1));
    } catch (GroupsNotFoundForUserException e){
    }

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    // add user1 to group2
    dfs3.addUserToGroup(user1, group2);
    assertEquals(Arrays.asList(group2),
        nn1.usersGroupsMapping.getGroups(user1));

    assertEquals(Arrays.asList(group2),
        nn2.usersGroupsMapping.getGroups(user1));

    assertEquals(Arrays.asList(group2),
        nn3.usersGroupsMapping.getGroups(user1));

    nn1.clearCache();
    nn2.clearCache();
    nn3.clearCache();

    // remove user1
    dfs1.removeUser(user1);

    try{
      nn1.usersGroupsMapping.getUserId(user1);
    } catch(UserNotFoundException e){

    }

    try{
      nn2.usersGroupsMapping.getUserId(user1);
    } catch(UserNotFoundException e){

    }
    try{
      nn3.usersGroupsMapping.getUserId(user1);
    } catch(UserNotFoundException e){

    }
  }
}
