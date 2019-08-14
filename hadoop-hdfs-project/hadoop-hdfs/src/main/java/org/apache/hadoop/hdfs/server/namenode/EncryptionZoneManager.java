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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.EncryptionZoneDataAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.fs.BatchedRemoteIterator.BatchedListEntries;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants
    .CRYPTO_XATTR_ENCRYPTION_ZONE;

/**
 * Manages the list of encryption zones in the filesystem.
 * <p/>
 * The EncryptionZoneManager has its own lock, but relies on the FSDirectory
 * lock being held for many operations. The FSDirectory lock should not be
 * taken if the manager lock is already held.
 */
public class EncryptionZoneManager {

  public static Logger LOG = LoggerFactory.getLogger(EncryptionZoneManager
      .class);

  /**
   * EncryptionZoneInt is the internal representation of an encryption zone. The
   * external representation of an EZ is embodied in an EncryptionZone and
   * contains the EZ's pathname.
   */
  private static class EncryptionZoneInt {
    private final long inodeId;
    private final CipherSuite suite;
    private final CryptoProtocolVersion version;
    private final String keyName;

    EncryptionZoneInt(long inodeId, CipherSuite suite,
        CryptoProtocolVersion version, String keyName) {
      Preconditions.checkArgument(suite != CipherSuite.UNKNOWN);
      Preconditions.checkArgument(version != CryptoProtocolVersion.UNKNOWN);
      this.inodeId = inodeId;
      this.suite = suite;
      this.version = version;
      this.keyName = keyName;
    }

    long getINodeId() {
      return inodeId;
    }

    CipherSuite getSuite() {
      return suite;
    }

    CryptoProtocolVersion getVersion() { return version; }

    String getKeyName() {
      return keyName;
    }
  }

  private final FSDirectory dir;
  private final int maxListEncryptionZonesResponses;

  /**
   * Construct a new EncryptionZoneManager.
   *
   * @param dir Enclosing FSDirectory
   */
  public EncryptionZoneManager(FSDirectory dir, Configuration conf) {
    this.dir = dir;
    maxListEncryptionZonesResponses = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES_DEFAULT
    );
    Preconditions.checkArgument(maxListEncryptionZonesResponses >= 0,
        DFSConfigKeys.DFS_NAMENODE_LIST_ENCRYPTION_ZONES_NUM_RESPONSES + " " +
            "must be a positive integer."
    );
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param inodeId of the encryption zone
   * @param keyName encryption zone key name
   */
  void addEncryptionZone(Long inodeId, CipherSuite suite,
      CryptoProtocolVersion version, String keyName) throws TransactionContextException, StorageException {
    unprotectedAddEncryptionZone(inodeId, suite, version, keyName);
  }

  /**
   * Add a new encryption zone.
   * <p/>
   * Does not assume that the FSDirectory lock is held.
   *
   * @param inodeId of the encryption zone
   * @param keyName encryption zone key name
   */
  void unprotectedAddEncryptionZone(Long inodeId,
      CipherSuite suite, CryptoProtocolVersion version, String keyName) 
      throws TransactionContextException, StorageException {
    final HdfsProtos.ZoneEncryptionInfoProto proto = PBHelper.convert(suite, version, keyName);
    EntityManager.add(new io.hops.metadata.hdfs.entity.EncryptionZone(inodeId, proto.toByteArray()));
  }

  /**
   * Remove an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  void removeEncryptionZone(Long inodeId) throws StorageException, TransactionContextException {
    io.hops.metadata.hdfs.entity.EncryptionZone ez = EntityManager.find(
        io.hops.metadata.hdfs.entity.EncryptionZone.Finder.ByPrimaryKeyInContext, inodeId);
    if (ez != null) {
      EntityManager.remove(ez);
    }
  }

  /**
   * Returns true if an IIP is within an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  boolean isInAnEZ(INodesInPath iip)
      throws UnresolvedLinkException, TransactionContextException, StorageException, InvalidProtocolBufferException {
    return (getEncryptionZoneForPath(iip) != null);
  }

  /**
   * Returns the path of the EncryptionZoneInt.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  private String getFullPathName(EncryptionZoneInt ezi) throws IOException {
    return dir.getInode(ezi.getINodeId()).getFullPathName();
  }

  /**
   * Get the key name for an encryption zone. Returns null if <tt>iip</tt> is
   * not within an encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  String getKeyName(final INodesInPath iip) throws TransactionContextException, StorageException,
      InvalidProtocolBufferException {
    EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return null;
    }
    return ezi.getKeyName();
  }

  /**
   * Looks up the EncryptionZoneInt for a path within an encryption zone.
   * Returns null if path is not within an EZ.
   * <p/>
   * Must be called while holding the manager lock.
   */
  private EncryptionZoneInt getEncryptionZoneForPath(INodesInPath iip) throws TransactionContextException, StorageException, InvalidProtocolBufferException {
    Preconditions.checkNotNull(iip);
    List<INode> inodes = iip.getReadOnlyINodes();
    for (int i = inodes.size() - 1; i >= 0; i--) {
      final INode inode = inodes.get(i);
      if (inode != null) {
        io.hops.metadata.hdfs.entity.EncryptionZone ez = EntityManager.find(
            io.hops.metadata.hdfs.entity.EncryptionZone.Finder.ByPrimaryKeyInContext, inode.getId());
        if (ez != null && ez.getZoneInfo() != null) {
          final HdfsProtos.ZoneEncryptionInfoProto proto = HdfsProtos.ZoneEncryptionInfoProto.
              parseFrom(ez.getZoneInfo());
          return new EncryptionZoneInt(inode.getId(), PBHelper.convert(proto.getSuite()), PBHelper.convert(
              proto.getCryptoProtocolVersion()), proto.getKeyName());
        }
      }
    }
    return null;
  }

  /**
   * Returns an EncryptionZone representing the ez for a given path.
   * Returns an empty marker EncryptionZone if path is not in an ez.
   *
   * @param iip The INodesInPath of the path to check
   * @return the EncryptionZone representing the ez for the path.
   */
  EncryptionZone getEZINodeForPath(INodesInPath iip) throws IOException {
    final EncryptionZoneInt ezi = getEncryptionZoneForPath(iip);
    if (ezi == null) {
      return null;
    } else {
      return new EncryptionZone(ezi.getINodeId(), getFullPathName(ezi),
          ezi.getSuite(), ezi.getVersion(), ezi.getKeyName());
    }
  }

  /**
   * Throws an exception if the provided path cannot be renamed into the
   * destination because of differing encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   *
   * @param srcIIP source IIP
   * @param dstIIP destination IIP
   * @param src    source path, used for debugging
   * @throws IOException if the src cannot be renamed to the dst
   */
  void checkMoveValidity(INodesInPath srcIIP, INodesInPath dstIIP, String src)
      throws IOException {
    final EncryptionZoneInt srcEZI = getEncryptionZoneForPath(srcIIP);
    final EncryptionZoneInt dstEZI = getEncryptionZoneForPath(dstIIP);
    final boolean srcInEZ = (srcEZI != null);
    final boolean dstInEZ = (dstEZI != null);
    if (srcInEZ) {
      if (!dstInEZ) {
        if (srcEZI.getINodeId() == srcIIP.getLastINode().getId()) {
          // src is ez root and dest is not in an ez. Allow the rename.
          return;
        }
        throw new IOException(
            src + " can't be moved from an encryption zone.");
      }
    } else {
      if (dstInEZ) {
        throw new IOException(
            src + " can't be moved into an encryption zone.");
      }
    }

    if (srcInEZ || dstInEZ) {
      Preconditions.checkState(srcEZI != null, "couldn't find src EZ?");
      Preconditions.checkState(dstEZI != null, "couldn't find dst EZ?");
      if (srcEZI.getINodeId() != dstEZI.getINodeId()) {
        final String srcEZPath = getFullPathName(srcEZI);
        final String dstEZPath = getFullPathName(dstEZI);
        final StringBuilder sb = new StringBuilder(src);
        sb.append(" can't be moved from encryption zone ");
        sb.append(srcEZPath);
        sb.append(" to encryption zone ");
        sb.append(dstEZPath);
        sb.append(".");
        throw new IOException(sb.toString());
      }
    }
  }

  /**
   * Create a new encryption zone.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  XAttr createEncryptionZone(String src, CipherSuite suite,
      CryptoProtocolVersion version, String keyName)
      throws IOException {
    final INodesInPath srcIIP = dir.getINodesInPath4Write(src, false);
    if (dir.isNonEmptyDirectory(srcIIP)) {
      throw new IOException(
          "Attempt to create an encryption zone for a non-empty directory.");
    }

    if (srcIIP != null &&
        srcIIP.getLastINode() != null &&
        !srcIIP.getLastINode().isDirectory()) {
      throw new IOException("Attempt to create an encryption zone for a file.");
    }
    EncryptionZoneInt ezi = getEncryptionZoneForPath(srcIIP);
    if (ezi != null) {
      throw new IOException("Directory " + src + " is already in an " +
          "encryption zone. (" + getFullPathName(ezi) + ")");
    }

    final HdfsProtos.ZoneEncryptionInfoProto proto =
        PBHelper.convert(suite, version, keyName);
    final XAttr ezXAttr = XAttrHelper
        .buildXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, proto.toByteArray());

    final List<XAttr> xattrs = Lists.newArrayListWithCapacity(1);
    xattrs.add(ezXAttr);
    // updating the xattr will call addEncryptionZone,
    // done this way to handle edit log loading
    FSDirXAttrOp.unprotectedSetXAttrs(dir, src, xattrs, EnumSet.of(XAttrSetFlag.CREATE));
    return ezXAttr;
  }

  /**
   * Cursor-based listing of encryption zones.
   * <p/>
   * Called while holding the FSDirectory lock.
   */
  BatchedListEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    List<io.hops.metadata.hdfs.entity.EncryptionZone> list
        = (List<io.hops.metadata.hdfs.entity.EncryptionZone>) new LightWeightRequestHandler(HDFSOperationType.GET_ALL_EZ) {
          @Override
          public Object performTask() throws IOException {
            EncryptionZoneDataAccess da = (EncryptionZoneDataAccess) HdfsStorageFactory.getDataAccess(
                EncryptionZoneDataAccess.class);
            return da.getAll();
          }
        }.handle();
    Collections.sort(list);
    final int numResponses = Math.min(maxListEncryptionZonesResponses,
        list.size());
    final List<EncryptionZone> zones =
        Lists.newArrayListWithExpectedSize(numResponses);

    int count = 0;
    int passed = 0;
    for (final io.hops.metadata.hdfs.entity.EncryptionZone ez : list) {
      if (ez.getInodeId() <= prevId) {
        passed++;
        continue;
      }
      boolean added = (boolean) new HopsTransactionalRequestHandler(HDFSOperationType.LIST_EZ) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ,
              TransactionLockTypes.INodeResolveType.PATH, ez.getInodeId());
          locks.add(il);
          locks.add(lf.getEZLock());
        }

        @Override
        public Object performTask() throws IOException {
          /*
           * Skip EZs that are only present in snapshots. Re-resolve the path to
           * see if the path's current inode ID matches EZ map's INode ID.
           *
           * INode#getFullPathName simply calls getParent recursively, so will return
           * the INode's parents at the time it was snapshotted. It will not
           * contain a reference INode.
           */
          final String pathName = dir.getInode(ez.getInodeId()).getFullPathName();
          INodesInPath iip = dir.getINodesInPath(pathName, false);
          INode lastINode = iip.getLastINode();
          if (lastINode == null || lastINode.getId() != ez.getInodeId()) {
            return false;
          }
          final HdfsProtos.ZoneEncryptionInfoProto proto = HdfsProtos.ZoneEncryptionInfoProto.
              parseFrom(ez.getZoneInfo());
          // Add the EZ to the result list
          zones.add(new EncryptionZone(ez.getInodeId(), pathName, PBHelper.convert(proto.getSuite()), PBHelper.convert(
              proto.getCryptoProtocolVersion()), proto.getKeyName()));
          return true;
        }
      }.handle();
      if(!added){
        continue;
      }
      count++;
      if (count >= numResponses) {
        break;
      }
    }
    final boolean hasMore = (numResponses < list.size() - passed);
    return new BatchedListEntries<EncryptionZone>(zones, hasMore);
  }
}
