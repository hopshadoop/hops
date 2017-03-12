package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.rollBack.dal.RemoveSnapshotAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Copyright (c) 01/08/16 DigitalRoute
 * All rights reserved.
 * Usage of this program and the accompanying materials is subject to license terms
 *
 * @author pushparaj.motamari
 */
public class RemoveSnapshotManager {
    static final Log LOG = LogFactory.getLog(RemoveSnapshotManager.class);
    private final FSNamesystem namesystem;
    private ScheduledExecutorService executorService;
    private RemoveSnapshotAccess removeSnapshotImpl;
    private int rootId = 2;


    public RemoveSnapshotManager(Namesystem namesystem) {
        this.namesystem = (FSNamesystem) namesystem;
        removeSnapshotImpl = (RemoveSnapshotAccess) HdfsStorageFactory.getDataAccess(RemoveSnapshotAccess.class);
        this.executorService = Executors.newScheduledThreadPool(2);
    }

    public enum ExecutionStatus {
        NOT_STARTED,
        STARTED,
        ROOT_SUBTREE_LOCKED,
        WAIT_FOR_SUBTREE_OPERATIONS,
        WAIT_FOR_QUOTA_UPDATES,

        INODES_PHASE1_COMPLETE,

        INODES_PHASE2_COMPLETE,

        INODE_ATTRIBS_PHASE1_COMPLETE,

        INODE_ATTRIBS_PHASE2_COMPLETE,

        BLOCKS_PHASE1_COMPLETE,

        BLOCKS_PHASE2_COMPLETE,

        ROOT_SUBTREE_UNLOCKED,
        COMPLETE;
    }

    public boolean process() throws  IOException{
        String stsString = HdfsVariables.getRemoveSnapshotStatus();

        ExecutionStatus sts = ExecutionStatus.valueOf(stsString);

        switch (sts) {
            case NOT_STARTED:
                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.STARTED);

            case STARTED:
                // What if takeSubTreeLockOnRoot succeds and leader fails? The next leader should check that inode with INTEGER.MAX_ID exists
                //and then set status to ROOT_SUBTREE_LOCKED.
                long startTime = System.currentTimeMillis();
                boolean subTreeLocked = false;
                while (System.currentTimeMillis() - startTime < 100 * 1000 && !subTreeLocked) {
                    try {
                        if (RollBackManager.takeSubTreeLockOnRoot(namesystem)) {
                            subTreeLocked = true;
                        }
                    } catch (Exception ex) {
                        //try until you take sub-tree lock on root.
                    }
                    if (subTreeLocked) {
                        HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.ROOT_SUBTREE_LOCKED);
                    } else {
                        throw new IOException("Unable to take subtree Lock on root even after 100 seconds");
                    }
                }

            case ROOT_SUBTREE_LOCKED:
                if (removeSnapshotImpl.waitForSubTreeOperations()) {
                    HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.WAIT_FOR_SUBTREE_OPERATIONS);
                } else {
                    throw new IOException("Ongoing subtree operations were not finished.Hence rollBack Failed");
                }


            case WAIT_FOR_SUBTREE_OPERATIONS:
                if (removeSnapshotImpl.waitForQuotaUpdates()) {
                    HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.WAIT_FOR_QUOTA_UPDATES);
                } else {
                    throw new IOException("Waiting for quota updates to complete were not finished.Hence rollBack Failed");
                }

                /*Process Inodes*/
            case WAIT_FOR_QUOTA_UPDATES:

                if (!removeSnapshotImpl.processInodesPhase1())
                    throw new IOException("Exception while processing case:WAIT_FOR_QUOTA_UPDATES");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.INODES_PHASE1_COMPLETE);

            case INODES_PHASE1_COMPLETE:

                if (!removeSnapshotImpl.processInodesPhase2())
                    throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.INODES_PHASE2_COMPLETE);

            case INODES_PHASE2_COMPLETE:

                if (!removeSnapshotImpl.processInodeAttributesPhase1())
                    throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.INODE_ATTRIBS_PHASE1_COMPLETE);

            case INODE_ATTRIBS_PHASE1_COMPLETE:

                if (!removeSnapshotImpl.processInodeAttributesPhase2())
                    throw new IOException("Exception while processing case:INODES_PHASE1_COMPLETE");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.INODE_ATTRIBS_PHASE2_COMPLETE);

            case INODE_ATTRIBS_PHASE2_COMPLETE:

                if (!removeSnapshotImpl.processBlocksPhase1())
                    throw new IOException("Exception while processing case:INODES_PHASE2_COMPLETE");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.BLOCKS_PHASE1_COMPLETE);

            case BLOCKS_PHASE1_COMPLETE:

                if (!removeSnapshotImpl.processBlocksPhase2())
                    throw new IOException("Exception while processing case:BLOCKS_PHASE1_COMPLETE");

                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.BLOCKS_PHASE2_COMPLETE);

            case BLOCKS_PHASE2_COMPLETE:

                if (RollBackManager.releaseLocksOnRoot(namesystem)) {
                    HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.ROOT_SUBTREE_UNLOCKED);
                } else {
                    throw new IOException("Unable to remove locks on root");
                }

            case ROOT_SUBTREE_UNLOCKED:
                HdfsVariables.setRemoveSnapshotStatus(ExecutionStatus.COMPLETE);

            case COMPLETE:
                return true;

            default://A wrong status message

                throw new IOException("Wrong Status Message");

        }

    }
}
