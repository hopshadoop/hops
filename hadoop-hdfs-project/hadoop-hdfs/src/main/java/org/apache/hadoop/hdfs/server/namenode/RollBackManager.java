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
import java.util.concurrent.ExecutionException;

import io.hops.metadata.HdfsVariables;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.Variables;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.metadata.rollBack.dal.RollBackAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLockTypes.INodeResolveType;
import io.hops.transaction.lock.TransactionLocks;

import static io.hops.transaction.lock.LockFactory.getInstance;

/**Takes care of roll-back after snapshot on root was taken.
 * @author Pushparaj
 */
public class RollBackManager {

    static final Log LOG = LogFactory.getLog(RollBackManager.class);
  private final FSNamesystem namesystem;
  private boolean isRollBackInProgress=false;
  private RollBackAccess rollBackImpl;
  private int rootId=2;
  
  
  public RollBackManager(Namesystem namesystem) {
    this.namesystem = (FSNamesystem) namesystem;
     rollBackImpl = (RollBackAccess)HdfsStorageFactory.getDataAccess(RollBackAccess.class);
  
  }
  
    private enum status {
         NOT_STARTED,
         STARTED,
        ROOT_LOCKED,
        FILETREE_READ,
        WAIT_COMPLETE,
        
        INODES_PHASE1_COMPLETE, //Delete all rows with status=2 or status=3
       
        INODES_PHASE2_COMPLETE,//Update all rows with id>0 and isDeleted=1 to isDeleted=0
       
        INODES_PHASE3_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] and [new row's parentid]=-[back-up row's parentid]
               
        INODE_ATTRIBS_PHASE1_COMPLETE,//Delete all rows with status=2 or status=3
       
        INODE_ATTRIBS_PHASE2_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] 
      
        BLOCKS_PHASE1_COMPLETE,//Delete all rows with status=2 or status=3
       
        BLOCKS_PHASE2_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] 
                
        ROOT_UNLOCKED,
        COMPLETE;
    };

  /*
   * Leader when starts checks whether the roll-back process stopped in half-way on the prveious dead leader,if it was, then this leader resumes it.
   */
    public void checkAndStartRollBack() throws IOException {
        
        if(!namesystem.isLeader()){
            return;
        }
        
        String status = HdfsVariables.getRollBackStatus2();
        if (status.equalsIgnoreCase("NOT_STARTED")) {
            //Since roll-back process was not started on previous dead leader, nothing we need to do. We just return.
            return;
          } else {
               //RollBack was started on another leader namenode which was dead and now I am the leader,
            //hence this namenode starts roll-back process from where the previous dead leader stopped.
            startRollBack(true);
        }

    }
  
    
    @SuppressWarnings("empty-statement")
    private boolean startRollBack(boolean isPreviousLeaderDead) throws IOException {
        isRollBackInProgress = true;
        String stsString = HdfsVariables.getRollBackStatus2();

        status sts = status.valueOf(stsString);

            switch (sts) {
                case NOT_STARTED:
                    HdfsVariables.setRollBackStatus2("STARTED");
                    
                case STARTED:
                    if(takeSubTreeLockOnRoot(isPreviousLeaderDead)){
                         HdfsVariables.setRollBackStatus2("ROOT_LOCKED");
                    }
                   
                    else{
                        throw new IOException("Taking lock on root is failed");
                    }
                    
                case ROOT_LOCKED:
                    //We may need to change the subTreeLock Owner in case this namenode is not the one, but with-out that also it should work.
                    HdfsVariables.setRollBackStatus2("FILETREE_READ");
                    
                case FILETREE_READ:
                    long presentTime = System.currentTimeMillis();
                    presentTime += 60*1000;
                    
                    while(System.currentTimeMillis()<presentTime)
                        ;//Wait for 1 minute so that no opearations goes-on
                    
                    HdfsVariables.setRollBackStatus2("WAIT_COMPLETE");
                    
                /*Process Inodes*/
                case WAIT_COMPLETE:
                   
                    if(!rollBackImpl.processInodesPhase1())
                        throw new IOException("Exception while processing case:WAIT_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("INODES_PHASE1_COMPLETE");

                case INODES_PHASE1_COMPLETE:
                   
                    if(!rollBackImpl.processInodesPhase2())
                        throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("INODES_PHASE2_COMPLETE");

                case INODES_PHASE2_COMPLETE:
                   
                    if(!rollBackImpl.processInodesPhase3())
                        throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("INODES_PHASE3_COMPLETE");
                    
                    
                case INODES_PHASE3_COMPLETE:
                   
                    if(!rollBackImpl.processInodeAttributesPhase1())
                        throw new IOException("Exception while processing case:INODES_PHASE3_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("INODE_ATTRIBUTES_PHASE1_COMPLETE");
                    
                    
                    
                case INODE_ATTRIBS_PHASE1_COMPLETE:
                   
                    if(!rollBackImpl.processInodeAttributesPhase2())
                        throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("INODE_ATTRIBS_PHASE2_COMPLETE");

                case INODE_ATTRIBS_PHASE2_COMPLETE:
                   
                    if(!rollBackImpl.processBlocksPhase1())
                        throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("BLOCKS_PHASE1_COMPLETE");
                                                 
                case BLOCKS_PHASE1_COMPLETE:
                   
                    if(!rollBackImpl.processInodesPhase2())
                        throw new IOException("Exception while processing case:BLOCKS_PHASE1_COMPLETE");
                    
                    HdfsVariables.setRollBackStatus2("BLOCKS_PHASE2_COMPLETE");

                case BLOCKS_PHASE2_COMPLETE:
                    
                    if (releaseLocksOnRoot()) {
                        HdfsVariables.setRollBackStatus2("ROOT_UNLOCKED");
                    }else{
                        throw new IOException("Unable to remove locks on root");
                    }
                              
                case ROOT_UNLOCKED://Laste state. Release locks on root    
                   HdfsVariables.setRollBackStatus2("COMPLETE");
                   isRollBackInProgress = false;
                    return true;
                
                case COMPLETE:
                    isRollBackInProgress = false;
                    return true;

                default://A wrong status message
                   
                    isRollBackInProgress = false;
                    throw new IOException("Wrong Status Message");

            }

    }
    
   /**
    * 
    * If a leader namenode took lock on root and dead, and another leader comes-up then we do not need to take the lock//process again.
    * */
    private Boolean takeSubTreeLockOnRoot(final boolean isPreviousLeaderDead) throws IOException {

        return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.LOCK_ROOT) {
            @Override
            public void acquireLock(TransactionLocks locks) throws  IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getINodeLock(namesystem.getNameNode(),
                        INodeLockType.WRITE,
                        INodeResolveType.PATH, false, true,
                        new String[]{"/"} ));
            }

            @Override
            public Object performTask() throws  IOException {

                INode currentRoot = namesystem.dir.getRootDir();

                if (currentRoot != null && currentRoot.isRoot()&&currentRoot.getId()==INodeDirectory.ROOT_ID) {
                    if (namesystem.dir.getRootDir().isSubtreeLocked()) {
                        if (isPreviousLeaderDead) {
                            //SubTreeLock on root already taken. Nothing to do. If the current leader is not the same as the one which took subtreeLock on root, we may need to change that.
                            //That code is commented below.
            /* 
                             new HopsTransactionalRequestHandler(HDFSOperationType.SET_SUBTREE_LOCK) {
                             @Override
                             public TransactionLocks acquireLock() throws PersistanceException, IOException, ExecutionException {
                             HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
                             tla.getLocks()
                             .addINode(
                             INodeResolveType.PATH,
                             INodeLockType.WRITE, false, new String[]{"/"});
                             return tla.acquire();
                             }

                             @Override
                             public Object performTask() throws PersistanceException, IOException {

                             INode inode = namesystem.dir.getRootDir();
                             if (inode != null && inode.isRoot()) {
                             inode.setSubtreeLockOwner(namesystem.getNamenodeId());
                             EntityManager.update(inode);
                             } else {
                             throw new IOException("Root INode is Not Found");
                             }
                             return inode;
                             }
                             }.handle(namesystem);
                             */
                            if (currentRoot.getStatus() == SnapShotConstants.Modified) {
                                INodeDirectoryWithQuota rootBackUpRow = (INodeDirectoryWithQuota) new LightWeightRequestHandler(HDFSOperationType.GET_ROOT_BACKUP) {
                                    @Override
                                    public Object performTask() throws  IOException {
                                        INodeDataAccess<INode> dataAccess = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
                                        return dataAccess.indexScanfindInodeById(-INodeDirectory.ROOT_ID);
                                    }
                                }.handle();

                                INodeDirectoryWithQuota newRoot = new INodeDirectoryWithQuota(((INodeDirectoryWithQuota) rootBackUpRow).getNsQuota(), ((INodeDirectoryWithQuota) rootBackUpRow).getDsQuota(), (INodeDirectory) rootBackUpRow);
                                newRoot.setSubtreeLocked(true);
                                newRoot.setIdNoPersistance(Integer.MAX_VALUE);
                                EntityManager.add(newRoot);
                                EntityManager.remove(rootBackUpRow);
                                EntityManager.remove(currentRoot);
                            } else {
                                INodeDirectoryWithQuota newRoot = new INodeDirectoryWithQuota(((INodeDirectoryWithQuota) currentRoot).getNsQuota(), ((INodeDirectoryWithQuota) currentRoot).getDsQuota(), (INodeDirectory) currentRoot);
                                newRoot.setIdNoPersistance(Integer.MAX_VALUE);
                                newRoot.setSubtreeLocked(true);
                                EntityManager.add(newRoot);
                                EntityManager.remove(currentRoot);
                            }

                            return true;
                        } else {
                            //Previous Leader not dead, but some-other opeation took subTree Lock on root. So throw an exception or wait fot it to release the lock.
                            throw new IOException("Unable to take SubTree lock on root. Try after Sometime.");
                        }


                    } else {
                        
                        if (currentRoot.getStatus() == SnapShotConstants.Modified) {
                            INodeDirectoryWithQuota rootBackUpRow = (INodeDirectoryWithQuota) new LightWeightRequestHandler(HDFSOperationType.GET_ROOT_BACKUP) {
                                @Override
                                public Object performTask() throws  IOException {
                                    INodeDataAccess<INode> dataAccess = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
                                    return dataAccess.indexScanfindInodeById(-INodeDirectory.ROOT_ID);
                                }
                            }.handle();

                            INodeDirectoryWithQuota newRoot = new INodeDirectoryWithQuota(((INodeDirectoryWithQuota) rootBackUpRow).getNsQuota(), ((INodeDirectoryWithQuota) rootBackUpRow).getDsQuota(), (INodeDirectory) rootBackUpRow);
                            newRoot.setSubtreeLocked(true);
                            newRoot.setIdNoPersistance(Integer.MAX_VALUE);
                            newRoot.setSubtreeLockOwner(namesystem.getNamenodeId());
                            EntityManager.add(newRoot);
                            EntityManager.remove(rootBackUpRow);
                            EntityManager.remove(currentRoot);
                        } else {
                            INodeDirectoryWithQuota newRoot = new INodeDirectoryWithQuota(((INodeDirectoryWithQuota) currentRoot).getNsQuota(), ((INodeDirectoryWithQuota) currentRoot).getDsQuota(), (INodeDirectory) currentRoot);
                            newRoot.setIdNoPersistance(Integer.MAX_VALUE);
                            newRoot.setSubtreeLocked(true);
                            newRoot.setSubtreeLockOwner(namesystem.getNamenodeId());
                            EntityManager.add(newRoot);
                            EntityManager.remove(currentRoot);
                        }
                        return true;
                    }
                    
                } else {
                    throw new IOException("Unable to take Read Root to take SubTreeLock on it..");
                }

            }
        }.handle(namesystem);
    }

    
    private Boolean releaseLocksOnRoot() throws IOException {

        return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROOT_BACKUP) {
             
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {

            }
             
            @Override
            public Object performTask() throws  IOException {
                INodeDataAccess<INode> dataAccess = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
                INode currentRoot = dataAccess.indexScanfindInodeById(Integer.MAX_VALUE);
                INodeDirectoryWithQuota newRoot = new INodeDirectoryWithQuota(((INodeDirectoryWithQuota) currentRoot).getNsQuota(), ((INodeDirectoryWithQuota) currentRoot).getDsQuota(), (INodeDirectory) currentRoot);
                newRoot.setIdNoPersistance(INodeDirectory.ROOT_ID);
                newRoot.setSubtreeLocked(false);
                newRoot.setStatusNoPersistance(SnapShotConstants.Original);
                EntityManager.add(newRoot);
                EntityManager.remove(currentRoot);
                return true;
            }
        }.handle();
    }
        
  
    @SuppressWarnings("empty-statement")
  public boolean processRollBack() throws IOException{
      if(isRollBackInProgress){
          //Wait for the roll-back to finish.
          while(isRollBackInProgress)
              ;//Wait until RollBack finishes
          return isRollBackInProgress;
      }
      else{
        return  startRollBack(false);
      }
      
  }
  
  public boolean isRollBackInProgress(){
      return isRollBackInProgress;
  }

    private void processINodesPhase1() {
       
        
    }
  
  
}
