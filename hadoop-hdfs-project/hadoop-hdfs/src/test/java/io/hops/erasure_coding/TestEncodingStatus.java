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

import io.hops.DalDriver;
import io.hops.DalStorageFactory;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.metadata.hdfs.entity.EncodingStatus;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.EncodingStatusOperationType;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class TestEncodingStatus extends TestCase {

  static {
    try {
      final DalStorageFactory sf = DalDriver.load("io.hops.metadata.ndb.NdbStorageFactory");
      Properties conf = new Properties();
      conf.load(ClassLoader.getSystemResourceAsStream("ndb-config.properties"));
      sf.setConfiguration(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      HdfsStorageFactory.setConfiguration(new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testAddAndFindEncodingStatus() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final EncodingStatus statusToAdd =
        new EncodingStatus(1L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L);

    HopsTransactionalRequestHandler addReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.ADD_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            EntityManager.add(statusToAdd);
            return null;
          }
        };
    addReq.handle();

    HopsTransactionalRequestHandler findReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.FIND_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Long id = (Long) getParams()[0];
            locks.add(lf.getIndivdualEncodingStatusLock(
                TransactionLockTypes.LockType.READ_COMMITTED, id));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            Long id = (Long) getParams()[0];
            return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
          }
        };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(statusToAdd.getInodeId(), foundStatus.getInodeId());
    assertEquals(statusToAdd.getStatus(), foundStatus.getStatus());
    assertEquals(statusToAdd.getEncodingPolicy(),
        foundStatus.getEncodingPolicy());
    assertEquals(statusToAdd.getStatusModificationTime(),
        foundStatus.getStatusModificationTime());

    // Cleanup
    HopsTransactionalRequestHandler delReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.DELETE_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndivdualEncodingStatusLock(
                TransactionLockTypes.LockType.WRITE, statusToAdd.getInodeId()));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            EntityManager.remove(statusToAdd);
            return null;
          }
        };
    delReq.handle();

    findReq.setParams(statusToAdd.getInodeId());
    assertNull(findReq.handle());
  }

  @Test
  public void testUpdateEncodingStatus() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final EncodingStatus statusToAdd =
        new EncodingStatus(1L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L);

    HopsTransactionalRequestHandler addReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.ADD_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            EntityManager.add(statusToAdd);
            return null;
          }
        };
    addReq.handle();

    final EncodingPolicy policy1 = new EncodingPolicy("codec2", (short) 2);
    final EncodingStatus updatedStatus =
        new EncodingStatus(1L, EncodingStatus.Status.ENCODING_ACTIVE, policy1,
            2L);

    HopsTransactionalRequestHandler updateReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.UPDATE_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Long id = (Long) getParams()[0];
            locks.add(lf.getIndivdualEncodingStatusLock(
                TransactionLockTypes.LockType.WRITE, id));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            Long id = (Long) getParams()[0];
            EntityManager.update(updatedStatus);
            return null;
          }
        };
    updateReq.setParams(updatedStatus.getInodeId());
    updateReq.handle();

    HopsTransactionalRequestHandler findReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.FIND_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            Long id = (Long) getParams()[0];
            locks.add(lf.getIndivdualEncodingStatusLock(
                TransactionLockTypes.LockType.READ_COMMITTED, id));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            Long id = (Long) getParams()[0];
            return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
          }
        };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(updatedStatus.getInodeId(), foundStatus.getInodeId());
    assertEquals(updatedStatus.getStatus(), foundStatus.getStatus());
    assertEquals(updatedStatus.getEncodingPolicy(),
        foundStatus.getEncodingPolicy());
    assertEquals(updatedStatus.getStatusModificationTime(),
        foundStatus.getStatusModificationTime());

    // Cleanup
    HopsTransactionalRequestHandler delReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.DELETE_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(lf.getIndivdualEncodingStatusLock(
                TransactionLockTypes.LockType.WRITE, statusToAdd.getInodeId()));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            EntityManager.remove(statusToAdd);
            return null;
          }
        };
    delReq.handle();

    findReq.setParams(statusToAdd.getInodeId());
    assertNull(findReq.handle());
  }

  @Test
  public void testCountEncodingRequested() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final ArrayList<EncodingStatus> statusToAdd =
        new ArrayList<>();
    statusToAdd.add(
        new EncodingStatus(1L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L));
    statusToAdd
        .add(new EncodingStatus(2L, EncodingStatus.Status.ENCODED, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(3L, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(4L, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(5L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L));

    HopsTransactionalRequestHandler addReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.ADD_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            for (EncodingStatus status : statusToAdd) {
              EntityManager.add(status);
            }
            return null;
          }
        };
    addReq.handle();

    HopsTransactionalRequestHandler countReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.COUNT_REQUESTED_ENCODINGS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            return EntityManager
                .count(EncodingStatus.Counter.RequestedEncodings);
          }
        };
    assertEquals(count(statusToAdd, EncodingStatus.Status.ENCODING_REQUESTED),
        (int) (Integer) countReq.handle());

    // Cleanup
    HopsTransactionalRequestHandler delReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.DELETE_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            for (EncodingStatus status : statusToAdd) {
              EntityManager.remove(status);
            }
            return null;
          }
        };
    delReq.handle();
  }

  @Test
  public void testFindEncodingRequested() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final ArrayList<EncodingStatus> statusToAdd =
        new ArrayList<>();
    statusToAdd.add(
        new EncodingStatus(1L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L));
    statusToAdd
        .add(new EncodingStatus(2L, EncodingStatus.Status.ENCODED, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(3L, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(4L, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(
        new EncodingStatus(5L, EncodingStatus.Status.ENCODING_REQUESTED, policy,
            1L));

    HopsTransactionalRequestHandler addReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.ADD_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            for (EncodingStatus status : statusToAdd) {
              EntityManager.add(status);
            }
            return null;
          }
        };
    addReq.handle();

    LightWeightRequestHandler findReq = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_BY_INODE_ID) {

      @Override
      public Object performTask() throws StorageException, IOException {
        EncodingStatusDataAccess dataAccess =
            (EncodingStatusDataAccess) HdfsStorageFactory
                .getDataAccess(EncodingStatusDataAccess.class);
        Integer limit = (Integer) getParams()[0];
        return dataAccess.findRequestedEncodings(limit);
      }
    };
    findReq.setParams(100);
    Collection<EncodingStatus> foundStatus =
        (Collection<EncodingStatus>) findReq.handle();
    assertEquals(count(statusToAdd, EncodingStatus.Status.ENCODING_REQUESTED),
        count(foundStatus, EncodingStatus.Status.ENCODING_REQUESTED));

    int limit = 1;
    findReq.setParams(limit);
    foundStatus = (Collection<EncodingStatus>) findReq.handle();
    assertEquals(count(foundStatus, EncodingStatus.Status.ENCODING_REQUESTED),
        limit);

    // Cleanup
    HopsTransactionalRequestHandler delReq =
        new HopsTransactionalRequestHandler(
            HDFSOperationType.DELETE_ENCODING_STATUS) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {

          }

          @Override
          public Object performTask() throws StorageException, IOException {
            for (EncodingStatus status : statusToAdd) {
              EntityManager.remove(status);
            }
            return null;
          }
        };
    delReq.handle();
  }

  private int count(Collection<EncodingStatus> collection,
      EncodingStatus.Status status) {
    int count = 0;
    for (EncodingStatus encodingStatus : collection) {
      if (encodingStatus.getStatus().equals(status)) {
        count++;
      }
    }
    return count;
  }
}
