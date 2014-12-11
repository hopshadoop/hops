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
package io.hops.erasure_coding;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.EncodingStatusOperationType;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Daemon that manages erasure-coded files and their status. It scans for
 * requested encodings or repairs and schedules them if resources are available.
 * It checks the status of encodings and repairs and adjusts the file states
 * accordingly.
 */
public class ErasureCodingManager extends Configured {

  static final Log LOG = LogFactory.getLog(ErasureCodingManager.class);

  private final FSNamesystem namesystem;
  private final Daemon erasureCodingMonitorThread = new Daemon(
      new ErasureCodingMonitor());
  private EncodingManager encodingManager;
  private BlockRepairManager blockRepairManager;
  private String parityFolder;
  private final long recheckInterval;
  private final int activeEncodingLimit;
  private int activeEncodings = 0;
  private final int activeRepairLimit;
  private final int activeParityRepairLimit;
  private int activeRepairs = 0;
  private int activeParityRepairs = 0;
  private final int repairDelay;
  private final int parityRepairDelay;
  private final int deletionLimit;

  private static boolean enabled = false;

  public ErasureCodingManager(FSNamesystem namesystem, Configuration conf) {
    super(conf);
    this.namesystem = namesystem;
    this.parityFolder = conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER);
    this.recheckInterval = conf.getInt(DFSConfigKeys.RECHECK_INTERVAL_KEY,
        DFSConfigKeys.DEFAULT_RECHECK_INTERVAL);
    this.activeEncodingLimit =
        conf.getInt(DFSConfigKeys.ACTIVE_ENCODING_LIMIT_KEY,
            DFSConfigKeys.DEFAULT_ACTIVE_ENCODING_LIMIT);
    this.activeRepairLimit = conf.getInt(DFSConfigKeys.ACTIVE_REPAIR_LIMIT_KEY,
        DFSConfigKeys.DEFAULT_ACTIVE_REPAIR_LIMIT);
    this.activeParityRepairLimit =
        conf.getInt(DFSConfigKeys.ACTIVE_PARITY_REPAIR_LIMIT_KEY,
            DFSConfigKeys.DEFAULT_ACTIVE_PARITY_REPAIR_LIMIT);
    this.repairDelay = conf.getInt(DFSConfigKeys.REPAIR_DELAY_KEY,
        DFSConfigKeys.DEFAULT_REPAIR_DELAY_KEY);
    this.parityRepairDelay = conf.getInt(DFSConfigKeys.PARITY_REPAIR_DELAY_KEY,
        DFSConfigKeys.DEFAULT_PARITY_REPAIR_DELAY);
    this.deletionLimit = conf.getInt(DFSConfigKeys.DELETION_LIMIT_KEY,
        DFSConfigKeys.DEFAULT_DELETION_LIMIT);
    enabled = conf.getBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY,
        DFSConfigKeys.DEFAULT_ERASURE_CODING_ENABLED_KEY);
  }

  private boolean loadRaidNodeClasses() {
    try {
      Class<?> encodingManagerClass = getConf().getClass(
          DFSConfigKeys.ENCODING_MANAGER_CLASSNAME_KEY, null);
      if (encodingManagerClass == null) {
        encodingManagerClass = Class.forName(
            DFSConfigKeys.DEFAULT_ENCODING_MANAGER_CLASSNAME);
      }
      if (!EncodingManager.class.isAssignableFrom(encodingManagerClass)) {
        throw new ClassNotFoundException(
            encodingManagerClass + " is not an implementation of " +
                EncodingManager.class.getCanonicalName());
      }
      Constructor<?> encodingManagerConstructor = encodingManagerClass
          .getConstructor(Configuration.class);
      encodingManager = (EncodingManager) encodingManagerConstructor
          .newInstance(getConf());

      Class<?> blockRepairManagerClass = getConf().getClass(
          DFSConfigKeys.BLOCK_REPAIR_MANAGER_CLASSNAME_KEY, null);
      if (blockRepairManagerClass == null) {
        blockRepairManagerClass = Class.forName(
            DFSConfigKeys.DEFAULT_BLOCK_REPAIR_MANAGER_CLASSNAME);
      }
      if (!BlockRepairManager.class.isAssignableFrom(blockRepairManagerClass)) {
        throw new ClassNotFoundException(
            blockRepairManagerClass + " is not an implementation of " +
                BlockRepairManager.class.getCanonicalName());
      }
      Constructor<?> blockRepairManagerConstructor = blockRepairManagerClass
          .getConstructor(Configuration.class);
      blockRepairManager = (BlockRepairManager) blockRepairManagerConstructor
          .newInstance(getConf());
    } catch (Exception e) {
      LOG.error("Could not load erasure coding classes", e);
      return false;
    }

    return true;
  }

  public void activate() {
    if (!loadRaidNodeClasses()) {
      LOG.error("ErasureCodingMonitor not started. An error occurred during" +
          " the loading of the encoding library.");
      return;
    }

    erasureCodingMonitorThread.start();
    LOG.info("ErasureCodingMonitor started");
  }

  public void close() {
    try {
      if (erasureCodingMonitorThread != null) {
        erasureCodingMonitorThread.interrupt();
        erasureCodingMonitorThread.join(3000);
      }
    } catch (InterruptedException ie) {
    }
    LOG.info("ErasureCodingMonitor stopped");
  }

  public static boolean isErasureCodingEnabled(Configuration conf) {
    return conf.getBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY,
        DFSConfigKeys.DEFAULT_ERASURE_CODING_ENABLED_KEY);
  }

  private class ErasureCodingMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          try {
            if (namesystem.isInSafeMode()) {
              Thread.sleep(recheckInterval);
              continue;
            }
          } catch (IOException e) {
            LOG.info("In safe mode skipping this round");
          }
          if (namesystem.isLeader()) {
            checkActiveEncodings();
            scheduleEncodings();
            checkActiveRepairs();
            scheduleSourceRepairs();
            scheduleParityRepairs();
            garbageCollect();
            checkRevoked();
          }
          try {
            Thread.sleep(recheckInterval);
          } catch (InterruptedException ie) {
            LOG.warn("ErasureCodingMonitor thread received " +
                    "InterruptedException.", ie);
            break;
          }
        } catch (Throwable e) {
          LOG.error(e);
        }
      }
    }
  }

  private void checkActiveEncodings() throws IOException {
    LOG.info("Checking active encoding.");
    List<Report> reports = encodingManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          LOG.info("Encoding finished for " + report.getFilePath());
          finalizeEncoding(report.getFilePath());
          activeEncodings--;
          break;
        case FAILED:
          LOG.info("Encoding failed for " + report.getFilePath());
          updateEncodingStatus(report.getFilePath(),
              EncodingStatus.Status.ENCODING_FAILED,
              EncodingStatus.ParityStatus.REPAIR_FAILED);
          activeEncodings--;
          break;
        case CANCELED:
          LOG.info("Encoding canceled for " + report.getFilePath());
          updateEncodingStatus(report.getFilePath(),
              EncodingStatus.Status.ENCODING_CANCELED);
          activeEncodings--;
          break;
      }
    }
  }

  private void finalizeEncoding(final String path) {
    LOG.info("Finilizing encoding for " + path);
    try {
      new HopsTransactionalRequestHandler(HDFSOperationType.GET_INODE) {
        private String parityPath;

        @Override
        public void setUp() throws StorageException, IOException {
          super.setUp();
          EncodingStatus status = namesystem.getEncodingStatus(path);
          parityPath = parityFolder + "/" + status.getParityFileName();
        }

        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
                  TransactionLockTypes.INodeResolveType.PATH, path, parityPath)
                  .setNameNodeID(namesystem.getNameNode().getId())
                  .setActiveNameNodes(namesystem.getNameNode().getActiveNameNodes().getActiveNodes());
          locks.add(il).add(lf.getEncodingStatusLock(TransactionLockTypes.LockType.WRITE, path));
        }

        @Override
        public Object performTask() throws StorageException, IOException {
          INode sourceInode = namesystem.getINode(path);
          INode parityInode = namesystem.getINode(parityPath);

          if (sourceInode == null) {
            return null;
          }

          EncodingStatus encodingStatus = EntityManager
              .find(EncodingStatus.Finder.ByInodeId, sourceInode.getId());

          // Might get reported a second time after recovery
          if (encodingStatus.getStatus()
              != EncodingStatus.Status.ENCODING_ACTIVE) {
            return null;
          }

          if (parityInode == null) {
            encodingStatus.setStatus(EncodingStatus.Status.ENCODING_FAILED);
            encodingStatus.setStatusModificationTime(
                System.currentTimeMillis());
          } else {
            encodingStatus.setStatus(EncodingStatus.Status.ENCODED);
            encodingStatus.setStatusModificationTime(
                System.currentTimeMillis());
            encodingStatus.setParityInodeId(parityInode.getId());
            encodingStatus.setParityStatus(EncodingStatus.ParityStatus.HEALTHY);
            encodingStatus.setParityStatusModificationTime(
                System.currentTimeMillis());
          }

          EntityManager.update(encodingStatus);
          return null;
        }
      }.handle(this);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void updateEncodingStatus(String filePath,
      EncodingStatus.Status status, EncodingStatus.ParityStatus parityStatus) {
    try {
      namesystem.updateEncodingStatus(filePath, status, parityStatus, null);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void updateEncodingStatus(String filePath,
      EncodingStatus.Status status) {
    updateEncodingStatus(filePath, status, null);
  }

  private void updateEncodingStatus(String filePath,
      EncodingStatus.ParityStatus status) {
    updateEncodingStatus(filePath, null, status);
  }

  private void scheduleEncodings() throws IOException {
    LOG.info("Schedule encodings.");
    final int limit = activeEncodingLimit - activeEncodings;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_ENCODINGS) {
      @Override
      public Object performTask() throws StorageException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedEncodings(limit);
      }
    };
    Collection<EncodingStatus> requestedEncodings =
        (Collection<EncodingStatus>) findHandler.handle();
    for (EncodingStatus encodingStatus : requestedEncodings) {
      try {
        LOG.info("Trying to schedule encoding for " + encodingStatus);
        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        if (iNode == null) {
          LOG.error("findInode returned null for id " + encodingStatus.
              getInodeId());
          continue;
        }
        if (iNode.isUnderConstruction()) {
          // It might still be written to the file
          LOG.info("Still under construction. Encoding not scheduled for " +
              iNode.getId());
          continue;
        }

        String path = namesystem.getPath(iNode.getId(), iNode.isInTree());
        if (iNode == null) {
          continue;
        }

        LOG.info("Schedule encoding for " + path);
        UUID parityFileName = UUID.randomUUID();
        encodingManager.encodeFile(
            encodingStatus.getEncodingPolicy(),
            new Path(path),
            new Path(parityFolder + "/" + parityFileName.toString()),
            encodingStatus.getStatus() ==
                EncodingStatus.Status.COPY_ENCODING_REQUESTED ? true : false);
        namesystem.updateEncodingStatus(path,
            EncodingStatus.Status.ENCODING_ACTIVE, parityFileName.toString());
        activeEncodings++;
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  private void checkActiveRepairs() throws IOException {
    LOG.info("Checking active repairs.");
    List<Report> reports = blockRepairManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          LOG.info("Repair finished for " + report.getFilePath());
          if (isParityFile(report.getFilePath())) {
            checkFixedParity(report.getFilePath());
            activeParityRepairs--;
          } else {
            checkFixedSource(report.getFilePath());
            activeRepairs--;
          }
          break;
        case FAILED:
          LOG.info("Repair failed for " + report.getFilePath());
          if (isParityFile(report.getFilePath())) {
            updateEncodingStatus(report.getFilePath(),
                EncodingStatus.ParityStatus.REPAIR_FAILED);
            activeParityRepairs--;
          } else {
            updateEncodingStatus(report.getFilePath(),
                EncodingStatus.Status.REPAIR_FAILED);
            activeRepairs--;
          }
          break;
        case CANCELED:
          LOG.info("Repair canceled for " + report.getFilePath());
          if (isParityFile(report.getFilePath())) {
            updateEncodingStatus(report.getFilePath(),
                EncodingStatus.ParityStatus.REPAIR_CANCELED);
            activeParityRepairs--;
          } else {
            updateEncodingStatus(report.getFilePath(),
                EncodingStatus.Status.REPAIR_CANCELED);
            activeRepairs--;
          }
          break;
      }
    }
  }

  private void checkFixedSource(final String path) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_FIXED_SOURCE) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
                TransactionLockTypes.INodeResolveType.PATH, path).setNameNodeID(namesystem.getNameNode().getId())
                .setActiveNameNodes(namesystem.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getEncodingStatusLock(TransactionLockTypes.LockType.WRITE, path));
      }

      @Override
      public Object performTask() throws IOException {
        INode targetNode = namesystem.getINode(path);
        EncodingStatus status = EntityManager
            .find(EncodingStatus.Finder.ByInodeId, targetNode.getId());
        if (status.getLostBlocks() == 0) {
          status.setStatus(EncodingStatus.Status.ENCODED);
        } else {
          status.setStatus(EncodingStatus.Status.REPAIR_REQUESTED);
        }
        status.setStatusModificationTime(System.currentTimeMillis());
        EntityManager.update(status);
        return null;
      }
    }.handle();
  }

  private void checkFixedParity(final String path) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CHECK_FIXED_PARITY) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE,
                TransactionLockTypes.INodeResolveType.PATH, path).setNameNodeID(namesystem.getNameNode().getId())
                .setActiveNameNodes(namesystem.getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il).add(lf.getEncodingStatusLock(TransactionLockTypes.LockType.WRITE, path));
      }

      @Override
      public Object performTask() throws IOException {
        INode targetNode = namesystem.getINode(path);
        EncodingStatus status = EntityManager.find(
            EncodingStatus.Finder.ByParityInodeId, targetNode.getId());
        if (status.getLostParityBlocks() == 0) {
          status.setParityStatus(EncodingStatus.ParityStatus.HEALTHY);
        } else {
          status.setParityStatus(EncodingStatus.ParityStatus.REPAIR_REQUESTED);
        }
        status.setParityStatusModificationTime(System.currentTimeMillis());
        EntityManager.update(status);
        return null;
      }
    }.handle();
  }

  private void scheduleSourceRepairs() throws IOException {
    LOG.info("Scheduling repairs");
    final int limit = activeRepairLimit - activeRepairs;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_REPAIRS) {
      @Override
      public Object performTask() throws IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedRepairs(limit);
      }
    };

    Collection<EncodingStatus> requestedRepairs =
        (Collection<EncodingStatus>) findHandler.handle();
    for (EncodingStatus encodingStatus : requestedRepairs) {
      try {
        LOG.info("Scheduling source repair  for " + encodingStatus);
        if (System.currentTimeMillis()
            - encodingStatus.getStatusModificationTime() < repairDelay) {
          LOG.info("Skipping source repair. Delay not reached: " + repairDelay);
          continue;
        }

        if (encodingStatus.isParityRepairActive()) {
          LOG.info("Skipping source repair. Parity repair is active");
          continue;
        }

        String path = namesystem.getPath(encodingStatus.getInodeId(), encodingStatus.isInTree());
        // Set status before doing something. In case the file is recovered inbetween we don't have an invalid status.
        // If starting repair fails somehow then this should be detected by a timeout later.
        namesystem.updateEncodingStatus(path,
            EncodingStatus.Status.REPAIR_ACTIVE);
        LOG.info("Status set to source repair active " + encodingStatus);
        blockRepairManager.repairSourceBlocks(
            encodingStatus.getEncodingPolicy().getCodec(),
            new Path(path),
            new Path(parityFolder + "/" + encodingStatus.getParityFileName()));
        LOG.info("Scheduled job for source repair " + encodingStatus);
        activeRepairs++;
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  private void scheduleParityRepairs() {
    LOG.info("Scheduling parity repairs");
    final int limit = activeParityRepairLimit - activeParityRepairs;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_PARITY_REPAIRS) {
      @Override
      public Object performTask() throws IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedParityRepairs(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedRepairs =
          (Collection<EncodingStatus>) findHandler.handle();
      for (EncodingStatus encodingStatus : requestedRepairs) {
        LOG.info("Scheduling parity repair for " + encodingStatus);
        if (System.currentTimeMillis() -
            encodingStatus.getParityStatusModificationTime() <
            parityRepairDelay) {
          LOG.info("Skipping  parity repair. Delay not reached: " +
              parityRepairDelay);
          continue;
        }

        if (encodingStatus.getStatus().equals(EncodingStatus.Status.ENCODED) ==
            false) {
          // Only repair parity for non-broken source files. Otherwise repair source file first.
          LOG.info("Skipping parity repair. Source file not healthy.");
          continue;
        }

        String path = namesystem.getPath(encodingStatus.getInodeId(), encodingStatus.isInTree());
        // Set status before doing something. In case the file is recovered inbetween we don't have an invalid status.
        // If starting repair fails somehow then this should be detected by a timeout later.
        namesystem.updateEncodingStatus(path,
            EncodingStatus.ParityStatus.REPAIR_ACTIVE);
        LOG.info("Status set to parity repair active " + encodingStatus);
        blockRepairManager
            .repairParityBlocks(encodingStatus.getEncodingPolicy().getCodec(),
                new Path(path), new Path(
                parityFolder + "/" + encodingStatus.getParityFileName()));
        LOG.info("Scheduled job for parity repair " + encodingStatus);
        activeRepairs++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void garbageCollect() throws IOException {
    LOG.info("Starting garbage collection");
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_DELETED) {
      @Override
      public Object performTask() throws IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findDeleted(deletionLimit);
      }
    };
    Collection<EncodingStatus> markedAsDeleted =
        (Collection<EncodingStatus>) findHandler.handle();
    for (EncodingStatus status : markedAsDeleted) {
      LOG.info("Trying to collect " + status);
      try {
        namesystem.deleteWithTransaction(
            parityFolder + "/" + status.getParityFileName(), false);
        namesystem.removeEncodingStatus(status);
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  private void checkRevoked() throws IOException {
    LOG.info("Checking replication for revocations");
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REVOKED) {
      @Override
      public Object performTask() throws IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRevoked();
      }
    };
    Collection<EncodingStatus> markedAsRevoked =
        (Collection<EncodingStatus>) findHandler.handle();
    for (EncodingStatus status : markedAsRevoked) {
      LOG.info("Checking replication for revoked status: " + status);
      String path = namesystem.getPath(status.getInodeId(), status.isInTree());
      int replication = namesystem.getFileInfo(path, true).getReplication();
      LocatedBlocks blocks = namesystem.getBlockLocations(path, 0,
          Long.MAX_VALUE, true, true).blocks;
      if (checkReplication(blocks, replication)) {
        LOG.info("Revocation successful for " + status);
        namesystem.deleteWithTransaction(
            parityFolder + "/" + status.getParityFileName(), false);
        namesystem.removeEncodingStatus(path, status);
      }
    }
  }

  private boolean checkReplication(LocatedBlocks blocks, int replication) {
    for (LocatedBlock locatedBlock : blocks.getLocatedBlocks()) {
      if (locatedBlock.getLocations().length != replication) {
        return false;
      }
    }
    return true;
  }

  public boolean isParityFile(String path) {
    Pattern pattern = Pattern.compile(parityFolder + ".*");
    Matcher matcher = pattern.matcher(path);
    if (matcher.matches()) {
      return true;
    }
    return false;
  }

  public static boolean isEnabled() {
    return enabled;
  }
}
