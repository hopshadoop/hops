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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.sun.xml.bind.v2.TODO;
import io.hops.common.GlobalThreadPool;
import io.hops.exception.StorageCallPreventedException;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
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

/**Takes care of rolling-back after snapshot on root was taken.
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
                    // What if takeSubTreeLockOnRoot succeds and leader fails? The next leader should check that inode with INTEGER.MAX_ID exists
                    //and then set status to ROOT_LOCKED.
                    Boolean  root_Id_Integer_Max_value_exists = (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROOT_BACKUP) {
                        @Override
                        public void acquireLock(TransactionLocks locks) throws IOException {
                            LockFactory lf = getInstance();
                            locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(Integer.MAX_VALUE), false));
                        }

                        @Override
                        public Object performTask() throws IOException {
                            try{
                                INode root = EntityManager.find(INode.Finder.ByINodeId, Integer.MAX_VALUE);
                                if(root == null){
                                    return false;
                                }
                            }catch (StorageCallPreventedException e ){
                                return false;
                            }

                            return true;
                        }


                    }.handle();

                    if(root_Id_Integer_Max_value_exists){
                        //Subtree lock was taken, root Id changed to Integer.max_value, committed but leader crashed while setting rootBack Status to ROOT_LOCKED.
                        HdfsVariables.setRollBackStatus2("ROOT_LOCKED");
                    }else{
                        outWhile: while(true){
                            try {
                               if( takeSubTreeLockOnRoot()) {
                                    break outWhile;
                               }
                            }catch (Exception ex) {
                                //try until you take sub-tree lock on root.
                            }

                        }
                        HdfsVariables.setRollBackStatus2("ROOT_LOCKED");
                    }

                case ROOT_LOCKED:
                    //We may need to change the subTreeLock Owner in case this namenode is not the one, but with-out that also it should work.
                    HdfsVariables.setRollBackStatus2("FILETREE_READ");
                    
                case FILETREE_READ:
                    long presentTime = System.currentTimeMillis();
                    presentTime += 1*1000;
                    
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
                   
                    if(!rollBackImpl.processBlocksPhase2())
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

    private Boolean takeSubTreeLockOnRoot() throws IOException {

        return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.LOCK_ROOT) {
            @Override
            public void acquireLock(TransactionLocks locks) throws  IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(INodeDirectory.ROOT_ID), false,true))
                .add(lf.getIndividualINodeAttributesLock(TransactionLockTypes.LockType.WRITE, new INodeIdentifier(-INodeDirectory.ROOT_ID)));
            }

            @Override
            public Object performTask() throws  IOException {

                INode currentRoot = namesystem.dir.getRootDir();
                INodeAttributes currentRootAttributes = EntityManager.find(INodeAttributes.Finder.ByINodeId, INodeDirectory.ROOT_ID);

                if (currentRoot != null && currentRoot.isRoot()&&currentRoot.getId()==INodeDirectory.ROOT_ID) {
                    if (namesystem.dir.getRootDir().isSubtreeLocked()) {
                       throw new IOException("SubTree Lock on root with id="+INodeDirectory.ROOT_ID+"exists.Unable to take subTreeLock on root.");
                    }else {
                             //At the end there will be row in INodes and INodeAttribute tables with id Integer.MAX_VAUE;
                        if (currentRoot.getStatus() == SnapShotConstants.Modified) {
                            INodeDirectoryWithQuota rootBackUpRow=(INodeDirectoryWithQuota) EntityManager.find(INode.Finder.ByINodeId,-INodeDirectory.ROOT_ID);
                            INodeAttributes rootBackUpRowAttributes;
                            try{
                                  rootBackUpRowAttributes = EntityManager.find(INodeAttributes.Finder.ByINodeId, -INodeDirectory.ROOT_ID);
                            }catch(StorageCallPreventedException excep){
                                rootBackUpRowAttributes = null;
                            }

                            rootBackUpRow.setIdNoPersistance(Integer.MAX_VALUE);
                             INodeDirectoryWithQuota newRoot=null;
                            if(rootBackUpRowAttributes!=null){
                                newRoot = new INodeDirectoryWithQuota(rootBackUpRowAttributes.getNsQuota(), rootBackUpRowAttributes.getDsQuota(), rootBackUpRowAttributes.getNsCount(),
                                         rootBackUpRowAttributes.getDiskspace(),SnapShotConstants.Original,  (INodeDirectory) rootBackUpRow);
                            } else{
                                  newRoot = new INodeDirectoryWithQuota(currentRootAttributes.getNsQuota(), currentRootAttributes.getDsQuota(), currentRootAttributes.getNsCount(),
                                           currentRootAttributes.getDiskspace(),SnapShotConstants.Original,  (INodeDirectory) rootBackUpRow);
                            }
                            newRoot.setSubtreeLocked(true);
                            newRoot.setSubtreeLockOwner(namesystem.getNamenodeId());
                            newRoot.setStatusNoPersistance(SnapShotConstants.Original);
                            newRoot.setParentIdNoPersistance(-rootBackUpRow.getParentId());
                            EntityManager.add(newRoot);

                            EntityManager.remove(currentRoot);
                            EntityManager.remove(currentRootAttributes);

                            rootBackUpRow.setIdNoPersistance(-INodeDirectory.ROOT_ID);
                            EntityManager.remove(rootBackUpRow);
                            if(rootBackUpRowAttributes!=null){
                                EntityManager.remove(rootBackUpRowAttributes);
                            }
                            return true;
                        } else {

                            // The case where rootRow is not modified but its attributes row may be modified.
                            INodeAttributes rootBackUpRowAttributes;
                            INodeDirectoryWithQuota newRoot=null;
                            try{
                                rootBackUpRowAttributes = EntityManager.find(INodeAttributes.Finder.ByINodeId, -INodeDirectory.ROOT_ID);
                            }catch(StorageCallPreventedException excep){
                                rootBackUpRowAttributes = null;
                            }
                            currentRoot.setIdNoPersistance(Integer.MAX_VALUE);

                            if(rootBackUpRowAttributes!=null){
                                newRoot = new INodeDirectoryWithQuota(rootBackUpRowAttributes.getNsQuota(), rootBackUpRowAttributes.getDsQuota(), rootBackUpRowAttributes.getNsCount(),
                                        rootBackUpRowAttributes.getDiskspace(),SnapShotConstants.Original,  (INodeDirectory) currentRoot);
                            }else{
                                newRoot = new INodeDirectoryWithQuota(currentRootAttributes.getNsQuota(), currentRootAttributes.getDsQuota(), currentRootAttributes.getNsCount(),
                                        currentRootAttributes.getDiskspace(),SnapShotConstants.Original,  (INodeDirectory) currentRoot);
                            }
                            currentRoot.setIdNoPersistance(INodeDirectory.ROOT_ID);//Since we changed it two lines above to create newRoot in line above.
                            newRoot.setStatusNoPersistance(SnapShotConstants.Original);
                            newRoot.setSubtreeLocked(true);
                            newRoot.setSubtreeLockOwner(namesystem.getNamenodeId());
                            EntityManager.add(newRoot);
                            EntityManager.remove(currentRoot);
                            EntityManager.remove(currentRootAttributes);
                            if(rootBackUpRowAttributes!=null){
                                EntityManager.remove(rootBackUpRowAttributes);
                            }
                            return true;
                        }

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
                LockFactory lf = getInstance();
                locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(Integer.MAX_VALUE), false));
            }
             
            @Override
            public Object performTask() throws  IOException {

                INodeDirectoryWithQuota currentRoot=(INodeDirectoryWithQuota) EntityManager.find(INode.Finder.ByINodeId,Integer.MAX_VALUE);
                INodeAttributes rootBackUpRowAttributes = EntityManager.find(INodeAttributes.Finder.ByINodeId, Integer.MAX_VALUE);
                currentRoot.setIdNoPersistance(INodeDirectory.ROOT_ID);
                INodeDirectoryWithQuota newRoot =  new INodeDirectoryWithQuota(rootBackUpRowAttributes.getNsQuota(), rootBackUpRowAttributes.getDsQuota(), rootBackUpRowAttributes.getNsCount(),
                                                    rootBackUpRowAttributes.getDiskspace(),rootBackUpRowAttributes.getStatus(),  (INodeDirectory) currentRoot);
                newRoot.setSubtreeLocked(false);
                newRoot.getINodeAttributes().setStatus(SnapShotConstants.Original);
                EntityManager.add(newRoot);
                currentRoot.setIdNoPersistance(Integer.MAX_VALUE); //Because it was changed above to INodeDirectory.ROOT_ID;
                EntityManager.remove(currentRoot);
                EntityManager.remove(rootBackUpRowAttributes);
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
  
}
