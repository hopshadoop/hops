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

import com.google.common.base.Preconditions;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.LockFactory.BLK;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCacheDistributed;
import org.apache.hadoop.ipc.Server;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

/**
 * Restrictions for a concat operation:
 * <pre>
 * 1. the src file and the target file are in the same dir
 * 2. all the source files are not in snapshot
 * 3. any source file cannot be the same with the target file
 * 4. source files cannot be under construction or empty
 * 5. source file's preferred block size cannot be greater than the target file
 * </pre>
 */
class FSDirConcatOp {

  static HdfsFileStatus concat(final FSDirectory fsd, final String target, final String[] srcs)
      throws IOException {
    Preconditions.checkArgument(!target.isEmpty(), "Target file name is empty");
    Preconditions.checkArgument(srcs != null && srcs.length > 0,
      "No sources given");
    assert srcs != null;
    if (FSDirectory.LOG.isDebugEnabled()) {
      FSDirectory.LOG.debug("concat {} to {}", Arrays.toString(srcs), target);
    }

    final String[] paths = new String[srcs.length + 1];
    System.arraycopy(srcs, 0, paths, 0, srcs.length);
    paths[srcs.length] = target;
    
    return (HdfsFileStatus) new HopsTransactionalRequestHandler(HDFSOperationType.CONCAT) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = getInstance();
        INodeLock il = lf.getINodeLock(INodeLockType.WRITE_ON_TARGET_AND_PARENT, INodeResolveType.PATH.PATH, paths)
            .setNameNodeID(fsd.getFSNamesystem().getNameNode().getId())
            .setActiveNameNodes(fsd.getFSNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getBlockLock()).add(
            lf.getBlockRelated(BLK.RE.RE, BLK.CR, BLK.ER, BLK.PE, BLK.UC, BLK.IV));
        if (fsd.getFSNamesystem().isRetryCacheEnabled()) {
          locks.add(lf.getRetryCacheEntryLock(Server.getClientId(),
              Server.getCallId()));
        }
        if (fsd.getFSNamesystem().isErasureCodingEnabled()) {
          locks.add(lf.getEncodingStatusLock(LockType.WRITE.WRITE, srcs));
        }
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        final CacheEntry cacheEntry = RetryCacheDistributed.waitForCompletion(fsd.getFSNamesystem().getRetryCache());
        if (cacheEntry != null && cacheEntry.isSuccess()) {
          return null; // Return previous response
        }
        boolean success = false;
        try {
          final INodesInPath targetIIP = fsd.getINodesInPath4Write(target);
          // write permission for the target
          FSPermissionChecker pc = null;
          if (fsd.isPermissionEnabled()) {
            pc = fsd.getPermissionChecker();
            fsd.checkPathAccess(pc, targetIIP, FsAction.WRITE);
          } 

          // check the target
          verifyTargetFile(fsd, target, targetIIP);
          // check the srcs
          INodeFile[] srcFiles = verifySrcFiles(fsd, srcs, targetIIP, pc);

          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " + Arrays.toString(srcs) + " to " + target);
          }

          long timestamp = now();
          unprotectedConcat(fsd, targetIIP, srcFiles, timestamp);
          success = true;
          return fsd.getAuditFileInfo(targetIIP);
          
        } finally {
          RetryCacheDistributed.setState(cacheEntry, success);
        }
      }
    }.handle();
  }

  private static void verifyTargetFile(FSDirectory fsd, final String target,
      final INodesInPath targetIIP) throws IOException {
    // check the target
    final INodeFile targetINode = INodeFile.valueOf(targetIIP.getLastINode(),
        target);

    if (targetINode.isFileStoredInDB()) {
      throw new IOException(
          "The target file is stored in the database. Can not concat to a a file stored in the database");
    }
    if (targetINode.isUnderConstruction()) {
      throw new HadoopIllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
  }
  
  private static INodeFile[] verifySrcFiles(FSDirectory fsd, String[] srcs,
      INodesInPath targetIIP, FSPermissionChecker pc) throws IOException {
    // to make sure no two files are the same
    Set<INodeFile> si = new LinkedHashSet<>();
    final INodeFile targetINode = targetIIP.getLastINode().asFile();
    final INodeDirectory targetParent = targetINode.getParent();
    // now check the srcs
    for (String src : srcs) {
      final INodesInPath iip = fsd.getINodesInPath4Write(src);
      // permission check for srcs
      if (pc != null) {
        fsd.checkPathAccess(pc, iip, FsAction.READ); // read the file
        fsd.checkParentAccess(pc, iip, FsAction.WRITE); // for delete
      }

      final INode srcINode = iip.getLastINode();
      final INodeFile srcINodeFile = INodeFile.valueOf(srcINode, src);
      // make sure the src file and the target file are in the same dir
      if (srcINodeFile.getParent() != targetParent) {
        throw new HadoopIllegalArgumentException("Source file " + src
            + " is not in the same directory with the target "
            + targetIIP.getPath());
      }
      // source file cannot be the same with the target file
      if (srcINode == targetINode) {
        throw new HadoopIllegalArgumentException("concat: the src file " + src
            + " is the same with the target file " + targetIIP.getPath());
      }
      // source file cannot be under construction or empty
      if(srcINodeFile.isUnderConstruction() || srcINodeFile.numBlocks() == 0) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }
      // source file's preferred block size cannot be greater than the target
      // file
      if (srcINodeFile.getPreferredBlockSize() >
          targetINode.getPreferredBlockSize()) {
        throw new HadoopIllegalArgumentException("concat: source file " + src
            + " has preferred block size " + srcINodeFile.getPreferredBlockSize()
            + " which is greater than the target file's preferred block size "
            + targetINode.getPreferredBlockSize());
      }
      si.add(srcINodeFile);
    }
    
    // make sure no two files are the same
    if (si.size() < srcs.length) {
      // it means at least two files are the same
      throw new HadoopIllegalArgumentException(
          "concat: at least two of the source files are the same");
    }
    return si.toArray(new INodeFile[si.size()]);
  }

  private static QuotaCounts computeQuotaDeltas(FSDirectory fsd,
      INodeFile target, INodeFile[] srcList) throws StorageException, TransactionContextException {
    QuotaCounts deltas = new QuotaCounts.Builder().build();
    final short targetRepl = target.getBlockReplication();
    for (INodeFile src : srcList) {
      short srcRepl = src.getBlockReplication();
      long fileSize = src.computeFileSize();
      if (targetRepl != srcRepl) {
        deltas.addStorageSpace(fileSize * (targetRepl - srcRepl));
        BlockStoragePolicy bsp =
            fsd.getBlockStoragePolicySuite().getPolicy(src.getStoragePolicyID());
        if (bsp != null) {
          List<StorageType> srcTypeChosen = bsp.chooseStorageTypes(srcRepl);
          for (StorageType t : srcTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, -fileSize);
            }
          }
          List<StorageType> targetTypeChosen = bsp.chooseStorageTypes(targetRepl);
          for (StorageType t : targetTypeChosen) {
            if (t.supportTypeQuota()) {
              deltas.addTypeSpace(t, fileSize);
            }
          }
        }
      }
    }
    return deltas;
  }

  private static void verifyQuota(FSDirectory fsd, INodesInPath targetIIP,
      QuotaCounts deltas) throws QuotaExceededException, StorageException, TransactionContextException {
    if (!fsd.getFSNamesystem().isImageLoaded()) {
      // Do not check quota if editlog is still being processed
      return;
    }
    FSDirectory.verifyQuota(targetIIP, targetIIP.length() - 1, deltas, null);
  }
  
  /**
   * Concat all the blocks from srcs to trg and delete the srcs files
   * @param fsd FSDirectory
   */
  static void unprotectedConcat(FSDirectory fsd, INodesInPath targetIIP,
      INodeFile[] srcList, long timestamp) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "
          + targetIIP.getPath());
    }

    final INodeFile trgInode = targetIIP.getLastINode().asFile();
    QuotaCounts deltas = computeQuotaDeltas(fsd, trgInode, srcList);
    verifyQuota(fsd, targetIIP, deltas);

    INodeDirectory trgParent = targetIIP.getINode(-2).asDirectory();
    List<BlockInfoContiguous> oldBlks = trgInode.concatBlocks(srcList);

    //params for updating the EntityManager.snapshots
    INodeCandidatePrimaryKey trg_param = new INodeCandidatePrimaryKey(trgInode.getId());
    List<INodeCandidatePrimaryKey> srcs_param = new ArrayList<>();
    
    for (INodeFile allSrcInode : srcList) {
      srcs_param.add(new INodeCandidatePrimaryKey(allSrcInode.getId()));
    }
    // since we are in the same dir - we can use same parent to remove files
    int count = 0;
    for (INodeFile nodeToRemove : srcList) {
      if(nodeToRemove != null) {
        
        nodeToRemove.getParent().removeChild(nodeToRemove);
        count++;
      }
    }

    trgInode.setModificationTime(timestamp);
    trgParent.updateModificationTime(timestamp);
    // update quota on the parent directory ('count' files removed, 0 space)
    fsd.unprotectedUpdateCount(targetIIP, targetIIP.length() - 1, deltas);
    EntityManager
        .snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.Concat,
            trg_param, srcs_param, oldBlks);
  }
}
