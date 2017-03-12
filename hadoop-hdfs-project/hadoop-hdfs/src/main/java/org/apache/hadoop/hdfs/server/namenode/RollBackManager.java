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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.hops.metadata.HdfsVariables;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.lock.LockFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.metadata.rollBack.dal.RollBackAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.lock.TransactionLockTypes.INodeLockType;
import io.hops.transaction.lock.TransactionLocks;

import static io.hops.transaction.lock.LockFactory.getInstance;

/**Takes care of rolling-back after snapshot on root was taken.
 * @author Pushparaj
 */
public class RollBackManager {

    static final Log LOG = LogFactory.getLog(RollBackManager.class);
    private final FSNamesystem namesystem;
    private ScheduledExecutorService executorService;
    private boolean isRollBackInProgress = false;
    private RollBackAccess rollBackImpl;
    private int rootId = 2;


    public RollBackManager(Namesystem namesystem) {
        this.namesystem = (FSNamesystem) namesystem;
        rollBackImpl = (RollBackAccess) HdfsStorageFactory.getDataAccess(RollBackAccess.class);
        this.executorService = Executors.newScheduledThreadPool(2);

    }

    public enum RollBackStatus {
        NOT_STARTED,
        STARTED,
        ROOT_SUBTREE_LOCKED,
        WAIT_FOR_SUBTREE_OPERATIONS,
        WAIT_FOR_QUOTA_UPDATES,

        INODES_PHASE1_COMPLETE, //Delete all rows with status=2 or status=3

        INODES_PHASE2_COMPLETE,//Update all rows with id>0 and isDeleted=1 to isDeleted=0

        INODES_PHASE3_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] and [new row's parentid]=-[back-up row's parentid]

        INODE_ATTRIBS_PHASE1_COMPLETE,//Delete all rows with status=2 or status=3

        INODE_ATTRIBS_PHASE2_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] 

        BLOCKS_PHASE1_COMPLETE,//Delete all rows with status=2 or status=3

        BLOCKS_PHASE2_COMPLETE,//Insert new row for each backup row with [new row's id]=-[back-up row's id] 

        ROOT_SUBTREE_UNLOCKED,
        COMPLETE;
    }

    ;

    public enum RollBackRequestStatus {
        REQUESTED,
        NOT_REQUESTED
    }

    private boolean rollBackInProgress = false;

    /*
     * Leader when starts checks whether the roll-back process stopped in half-way on the prveious dead leader,if it was, then this leader resumes it.
     */
    public void checkAndStartRollBack() throws IOException {

        class RollBackRequestStatusChecker implements Runnable {
            @Override
            public void run() {
                if (namesystem.isLeader()) {
                    try {
                        String rollBackRequestStatus = HdfsVariables.getRollBackRequestStatus();
                        RollBackRequestStatus reqSts = RollBackRequestStatus.valueOf(rollBackRequestStatus);
                        switch (reqSts) {
                            case REQUESTED:
                                if (!rollBackInProgress) {
                                    rollBackInProgress = true;
                                    startRollBack();
                                }
                                break;
                            case NOT_REQUESTED:
                                break;
                            default:
                                throw new IOException("Unknown rollBackRequestStatus is received");
                        }

                    } catch (IOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }
        executorService.scheduleWithFixedDelay(new RollBackRequestStatusChecker(), 2, 2, TimeUnit.SECONDS);
    }


    @SuppressWarnings("empty-statement")
    private boolean startRollBack() throws IOException {
        isRollBackInProgress = true;
        String stsString = HdfsVariables.getRollBackStatus2();

        RollBackStatus sts = RollBackStatus.valueOf(stsString);

        switch (sts) {
            case NOT_STARTED:
                HdfsVariables.setRollBackStatus2(RollBackStatus.STARTED);

            case STARTED:
                // What if takeSubTreeLockOnRoot succeds and leader fails? The next leader should check that inode with INTEGER.MAX_ID exists
                //and then set status to ROOT_SUBTREE_LOCKED.
                long startTime = System.currentTimeMillis();
                boolean subTreeLocked = false;
                while (System.currentTimeMillis() - startTime < 100 * 1000 && !subTreeLocked) {
                    try {
                        if (takeSubTreeLockOnRoot(namesystem)) {
                            subTreeLocked = true;
                        }
                    } catch (Exception ex) {
                        //try until you take sub-tree lock on root.
                    }
                    if (subTreeLocked) {
                        HdfsVariables.setRollBackStatus2(RollBackStatus.ROOT_SUBTREE_LOCKED);
                    } else {
                        throw new IOException("Unable to take subtree Lock on root even after 100 seconds");
                    }
                }

            case ROOT_SUBTREE_LOCKED:
                if (rollBackImpl.waitForSubTreeOperations()) {
                    HdfsVariables.setRollBackStatus2(RollBackStatus.WAIT_FOR_SUBTREE_OPERATIONS);
                } else {
                    throw new IOException("Ongoing subtree operations were not finished.Hence rollBack Failed");
                }


            case WAIT_FOR_SUBTREE_OPERATIONS:
                if (rollBackImpl.waitForQuotaUpdates()) {
                    HdfsVariables.setRollBackStatus2(RollBackStatus.WAIT_FOR_QUOTA_UPDATES);
                } else {
                    throw new IOException("Waiting for quota updates to complete were not finished.Hence rollBack Failed");
                }

                /*Process Inodes*/
            case WAIT_FOR_QUOTA_UPDATES:

                if (!rollBackImpl.processInodesPhase1())
                    throw new IOException("Exception while processing case:WAIT_FOR_QUOTA_UPDATES");

                HdfsVariables.setRollBackStatus2(RollBackStatus.INODES_PHASE1_COMPLETE);

            case INODES_PHASE1_COMPLETE:

                if (!rollBackImpl.processInodesPhase2())
                    throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.INODES_PHASE2_COMPLETE);

            case INODES_PHASE2_COMPLETE:

                if (!rollBackImpl.processInodesPhase3())
                    throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.INODES_PHASE3_COMPLETE);


            case INODES_PHASE3_COMPLETE:

                if (!rollBackImpl.processInodeAttributesPhase1())
                    throw new IOException("Exception while processing case:INODES_PHASE3_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.INODE_ATTRIBS_PHASE1_COMPLETE);


            case INODE_ATTRIBS_PHASE1_COMPLETE:

                if (!rollBackImpl.processInodeAttributesPhase2())
                    throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.INODE_ATTRIBS_PHASE2_COMPLETE);

            case INODE_ATTRIBS_PHASE2_COMPLETE:

                if (!rollBackImpl.processBlocksPhase1())
                    throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.BLOCKS_PHASE1_COMPLETE);

            case BLOCKS_PHASE1_COMPLETE:

                if (!rollBackImpl.processBlocksPhase2())
                    throw new IOException("Exception while processing case:BLOCKS_PHASE1_COMPLETE");

                HdfsVariables.setRollBackStatus2(RollBackStatus.BLOCKS_PHASE2_COMPLETE);

            case BLOCKS_PHASE2_COMPLETE:

                if (releaseLocksOnRoot(namesystem)) {
                    HdfsVariables.setRollBackStatus2(RollBackStatus.ROOT_SUBTREE_UNLOCKED);
                } else {
                    throw new IOException("Unable to remove locks on root");
                }

            case ROOT_SUBTREE_UNLOCKED:
                HdfsVariables.setRollBackStatus2(RollBackStatus.COMPLETE);

            case COMPLETE:
                isRollBackInProgress = false;
                return true;

            default://A wrong status message

                isRollBackInProgress = false;
                throw new IOException("Wrong Status Message");

        }

    }

    protected static Boolean takeSubTreeLockOnRoot(final FSNamesystem namesystem) throws IOException {

        return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.ROOT_SUBTREE_LOCK) {
            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(INodeDirectory.ROOT_ID), false))
                        .add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(-INodeDirectory.ROOT_ID), false));
            }

            @Override
            public Object performTask() throws IOException {

                INode currentRoot = namesystem.dir.getRootDir();

                if (currentRoot != null && currentRoot.isRoot() && currentRoot.getId() == INodeDirectory.ROOT_ID) {
                    if (namesystem.dir.getRootDir().isSubtreeLocked()) {//Assuming no operation takes subTreeLock on root.
                        if (currentRoot.getSubtreeLockOwner() == namesystem.getNamenodeId()) {
                            // case 1: single namenode died without commiting that subtree lock has been taken but died and then came back alive
                            // Case 2: Namenode x took subtree lock but died without commiting the status of rollBack and all others also died and then this NameNode came alive.
                            // Case 1 is particular case of Case 2 when total NameNodes are 1.
                            return true;
                        } else {
                            //The namenode which took subtree lock died before updating the rollBack status and a new leader was elected.
                            currentRoot.setSubtreeLockOwner(namesystem.getNamenodeId());
                        }
                    } else {
                        currentRoot.setSubtreeLocked(true);
                        currentRoot.setSubtreeLockOwner(namesystem.getNamenodeId());

                    }

                    if (currentRoot.getStatus() == SnapShotConstants.Modified) {
                        INode rootBackUp = EntityManager.find(INode.Finder.ByINodeId, -INodeDirectory.ROOT_ID);
                        if (rootBackUp != null) {
                            rootBackUp.setSubtreeLocked(true);
                            rootBackUp.setSubtreeLockOwner(namesystem.getNamenodeId());
                        } else {
                            throw new IOException("Failed to take subtree lock on the Root.Root's BackupRow doesn't exist.");
                        }
                    }
                    return true;
                } else {
                    throw new IOException("Unable to read Root to take SubTreeLock on it..");
                }
            }
        }.handle(namesystem);
    }


    protected static Boolean releaseLocksOnRoot(final FSNamesystem namesystem) throws IOException {

        return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROOT_BACKUP) {

            @Override
            public void acquireLock(TransactionLocks locks) throws IOException {
                LockFactory lf = getInstance();
                locks.add(lf.getIndividualINodeLock(INodeLockType.WRITE, new INodeIdentifier(INodeDirectory.ROOT_ID), false));
            }

            @Override
            public Object performTask() throws IOException {
                INode currentRoot = namesystem.dir.getRootDir();

                if (currentRoot != null && currentRoot.isRoot() && currentRoot.getId() == INodeDirectory.ROOT_ID) {
                    if (namesystem.dir.getRootDir().isSubtreeLocked()) {
                        currentRoot.setSubtreeLocked(false);
                        currentRoot.setSubtreeLockOwner(-1);
                        return true;
                    } else {
                        throw new IOException("Unable to release SubTreeLock on the root since it is already unlocked.");
                    }
                } else {
                    throw new IOException("Unable to release SubTreeLock on the root since after rollBack Procedure unable to find Root INode.");
                }

            }
        }.handle();
    }

    public boolean processRollBack() throws IOException {
        String rollBackRequestStatus = HdfsVariables.getRollBackRequestStatus();
        RollBackRequestStatus reqSts = RollBackRequestStatus.valueOf(rollBackRequestStatus);
        switch (reqSts) {
            case REQUESTED:
                String rollBackStatusFromNDB;
                rollBackStatusFromNDB = HdfsVariables.getRollBackStatus();
                RollBackStatus sts = RollBackStatus.valueOf(rollBackStatusFromNDB);
                switch (sts) {
                    case NOT_STARTED:
                        throw new IOException("RollBack is already requested and is yet to start");
                    default:
                        throw new IOException("RollBack is already requested and it is in progress");
                }

            case NOT_REQUESTED:
                HdfsVariables.setRollBackRequestStatus(RollBackRequestStatus.REQUESTED);
                return true;
            default:
                throw new IOException("Unknown snapshotRequestStatus is received");
        }
    }
}
