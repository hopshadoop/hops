package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.rollBack.dal.RollBackAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Copyright (c) 04/07/16 DigitalRoute
 * All rights reserved.
 * Usage of this program and the accompanying materials is subject to license terms
 *
 * @author pushparaj.motamari
 */
public class SnapShotManager {
    private final FSNamesystem namesystem;
    private ScheduledExecutorService executorService;
    public static final Log LOG = LogFactory.getLog(SnapShotManager.class);
    private RollBackAccess rollBackImpl;
    private boolean snapShotTakingInProgress = false;

    public SnapShotManager(FSNamesystem fsNamesystem) {
        this.namesystem = fsNamesystem;
        this.executorService = Executors.newScheduledThreadPool(2);
        rollBackImpl = (RollBackAccess) HdfsStorageFactory.getDataAccess(RollBackAccess.class);
    }

    public enum SnapshotStatus {
        NO_SNAPSHOT,
        STARTED,
        ROOT_SUBTREE_LOCKED,
        WAIT_FOR_SUBTREE_OPERATIONS,
        WAIT_FOR_QUOTA_UPDATES,
        SNAPSHOT_TAKEN,
        ROOT_SUBTREE_UNLOCKED
    }

    ;

    public enum SnapshotRequestStatus {
        REQUESTED,
        NOT_REQUESTED
    }

    public void start() {

        class SnapshotRequestStatusChecker implements Runnable {
            @Override
            public void run() {
                if (namesystem.isLeader()) {
                    try {
                        String snapShotRequestStatus = HdfsVariables.getSnapShotRequestStatus();
                        SnapshotRequestStatus reqSts = SnapshotRequestStatus.valueOf(snapShotRequestStatus);
                        switch (reqSts) {
                            case REQUESTED:
                                if (!snapShotTakingInProgress) {
                                    snapShotTakingInProgress = true;
                                    handleSnapshotOnRoot();
                                }
                                break;
                            case NOT_REQUESTED:
                                break;
                            default:
                                throw new IOException("Unknown snapshotRequestStatus is received");
                        }

                    } catch (IOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }
        class SnapshotStatusChecker implements Runnable {
            @Override
            public void run() {
                String snapShotTakenStatusFromNDB;
                try {
                    snapShotTakenStatusFromNDB = HdfsVariables.getSnapShotStatus();
                    boolean snapShotTakenStatusOnLocal = namesystem.isSnapshotAtRootTaken();
                    boolean sts = false;

                    if (snapShotTakenStatusFromNDB.equals(SnapshotStatus.SNAPSHOT_TAKEN)) {
                        sts = true;
                    } else {
                        sts = false;
                    }

                    if (snapShotTakenStatusOnLocal != sts) {
                        namesystem.setSnapShotAtRootTaken(!snapShotTakenStatusOnLocal);
                    }
                } catch (IOException e) {
                    LOG.error("Exception while retrieving the Snapshot status from NDB in SnapShotManager", e);
                }
            }
        }

        executorService.scheduleWithFixedDelay(new SnapshotRequestStatusChecker(), 2, 2, TimeUnit.SECONDS);
        executorService.scheduleWithFixedDelay(new SnapshotStatusChecker(), 2, 2, TimeUnit.SECONDS);
    }

    public boolean takeSnapshotOnRoot() throws IOException {
            String snapShotRequestStatus = HdfsVariables.getSnapShotRequestStatus();
            SnapshotRequestStatus reqSts = SnapshotRequestStatus.valueOf(snapShotRequestStatus);
            switch (reqSts) {
                case REQUESTED:
                    String snapShotTakenStatusFromNDB;
                    snapShotTakenStatusFromNDB = HdfsVariables.getSnapShotStatus();
                    SnapshotStatus sts = SnapshotStatus.valueOf(snapShotTakenStatusFromNDB);
                    switch (sts) {
                        case SNAPSHOT_TAKEN:
                            throw new IOException("Snapshot already taken");
                        case NO_SNAPSHOT:
                            throw new IOException("Snapshot is already requested and is yet to start");
                        default:
                            throw new IOException("Already a snapshot is requested by another user and taking is in progress");
                    }

                case NOT_REQUESTED:
                    HdfsVariables.setSnapShotRequestStatus(SnapshotRequestStatus.REQUESTED);
                    return true;
                default:
                    throw new IOException("Unknown snapshotRequestStatus is received");
            }
    }

    public void handleSnapshotOnRoot() throws IOException {
        String snapShotTakenStatusFromNDB;
        snapShotTakenStatusFromNDB = HdfsVariables.getSnapShotStatus();
        SnapshotStatus sts = SnapshotStatus.valueOf(snapShotTakenStatusFromNDB);
        switch (sts) {
            case NO_SNAPSHOT:
                HdfsVariables.setSnapShotStatus(SnapshotStatus.STARTED);

            case STARTED:
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
                        HdfsVariables.setSnapShotStatus(SnapshotStatus.ROOT_SUBTREE_LOCKED);
                    } else {
                        throw new IOException("Unable to take subtree Lock on root even after 100 seconds");
                    }
                }

            case ROOT_SUBTREE_LOCKED:
                if (rollBackImpl.waitForSubTreeOperations()) {
                    HdfsVariables.setSnapShotStatus(SnapshotStatus.WAIT_FOR_SUBTREE_OPERATIONS);
                } else {
                    throw new IOException("Ongoing subtree operations were not finished.Hence rollBack Failed");
                }


            case WAIT_FOR_SUBTREE_OPERATIONS:
                if (rollBackImpl.waitForQuotaUpdates()) {
                    HdfsVariables.setSnapShotStatus(SnapshotStatus.WAIT_FOR_QUOTA_UPDATES);
                } else {
                    throw new IOException("Waiting for quota updates to complete were not finished.Hence rollBack Failed");
                }


            case WAIT_FOR_QUOTA_UPDATES:
                if (RollBackManager.releaseLocksOnRoot(namesystem)) {
                    HdfsVariables.setSnapShotStatus(SnapshotStatus.ROOT_SUBTREE_UNLOCKED);
                } else {
                    throw new IOException("Unable to remove locks on root");
                }

            case ROOT_SUBTREE_UNLOCKED:
                HdfsVariables.setSnapShotStatus(SnapshotStatus.SNAPSHOT_TAKEN);
                namesystem.setSnapShotAtRootTaken(true);

        }
    }
}
