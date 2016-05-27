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
package io.hops.metadata;

import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.entity.StorageId;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StorageIdMap {

  private Map<String, Integer> storageIdtoSId = Collections.synchronizedMap(new HashMap<String, Integer>());
  private Map<Integer, String> sIdtoStorageId = Collections.synchronizedMap(new HashMap<Integer, String>());;
  
  public StorageIdMap() throws IOException {
    this(true);
  }

  public StorageIdMap(boolean loadFromDB) throws IOException {
    if(loadFromDB) {
      initialize();
    }
  }

  public void update(DatanodeStorageInfo s) throws IOException {
    String storageUuid = s.getStorageID();
    getSetSId(storageUuid);
    s.setSid(storageIdtoSId.get(storageUuid));
  }

  public int getSId(String storageId) {
    return storageIdtoSId.get(storageId);
  }

  public String getStorageId(int sid) {
    return sIdtoStorageId.get(sid);
  }

  private void initialize() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.INITIALIZE_SID_MAP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        StorageIdMapDataAccess<StorageId> da =
            (StorageIdMapDataAccess) HdfsStorageFactory.getDataAccess(StorageIdMapDataAccess.class);
        Collection<StorageId> sids = da.findAll();

        if(sids == null) {
          return null;
        }

        for (StorageId h : sids) {
          storageIdtoSId.put(h.getStorageId(), h.getsId());
          sIdtoStorageId.put(h.getsId(), h.getStorageId());
        }
        return null;
      }
    }.handle();
  }

  private void getSetSId(final String storageUuid) throws IOException {
    // If we've already seen this storageUuid, ignore this call.
    if(this.storageIdtoSId.containsKey(storageUuid)) {
      return;
    }

    new HopsTransactionalRequestHandler(HDFSOperationType.GET_SET_SID) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.SIdCounter, LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        int currSIdCount = HdfsVariables.getSIdCounter();
        StorageIdMapDataAccess<StorageId> da =
            (StorageIdMapDataAccess) HdfsStorageFactory
                .getDataAccess(StorageIdMapDataAccess.class);
        StorageId h = da.findByPk(storageUuid);
        if (h == null) {
          h = new StorageId(storageUuid, currSIdCount);
          da.add(h);
          currSIdCount++;
          HdfsVariables.setSIdCounter(currSIdCount);
        }

        storageIdtoSId.put(storageUuid, h.getsId());
        sIdtoStorageId.put(h.getsId(), storageUuid);

        return null;
      }
    }.handle();
  }
}
