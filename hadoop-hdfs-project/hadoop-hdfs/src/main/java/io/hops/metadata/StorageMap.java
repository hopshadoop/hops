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
import io.hops.metadata.hdfs.dal.StorageDataAccess;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO merge with StorageIdMap
public class StorageMap {
  private static final Log LOG = LogFactory.getLog(StorageMap.class);

  /**
   * Stores storageID->sid and sid->storageID mapping.
   * Works effectively like a cache to avoid hitting the DAL.
   */
  private StorageIdMap storageIdMap;

  private Map<Integer, DatanodeStorageInfo> storageInfoMap =
      Collections.synchronizedMap(new HashMap<Integer, DatanodeStorageInfo>());

  /**
   * Maps DatanodeUuids to the sids of the storages on the datanode.
   * Loads the data from the database, so it might contain datanodes/storages
   * that haven't heartbeated to this namenode yet.
   */
  private Map<String, ArrayList<Integer>> datanodeUuidToSids =
      Collections.synchronizedMap(new HashMap<String, ArrayList<Integer>>());;

  public StorageMap() {
    this(true);
  }

  // TODO should this method throw the IOExceptions?
  public StorageMap(boolean loadFromDB) {
    try {
      // Initialize the StorageId (String) <--> sid (int) maps
      this.storageIdMap = new StorageIdMap(loadFromDB);

      if(loadFromDB) {
        // Initialize the datanodeUuid (String) --> sids (int[]) map
        this.initializeDatanodeUuidToSidsMap();
      }
    } catch (IOException e) {
      LOG.error("Failed to load storageIdMap from database.", e);
    }
  }

  /**
   * Load the storage entries from the database to map DNUuids to sids
   */
  private void initializeDatanodeUuidToSidsMap() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.INITIALIZE_SID_MAP) {
      @Override
      public Object performTask() throws StorageException, IOException {
        StorageDataAccess<Storage> da =
            (StorageDataAccess) HdfsStorageFactory
                .getDataAccess(StorageDataAccess.class);
        Collection<Storage> entries = da.findAll();
        for (Storage sid : entries) {
          // If we already have an entry for this DN, add this sid to it,
          // otherwise create a new entry with only this sid.
          ArrayList<Integer> sids = datanodeUuidToSids.get(sid.getHostID());
          if(sids == null) {
            sids = new ArrayList<Integer>();
          }
          sids.add(sid.getStorageID());
          datanodeUuidToSids.put(sid.getHostID(), sids);
        }
        return null;
      }
    }.handle();
  }

  /**
   * Adds or replaces storageinfo for the given sid
   */
  public void updateStorage(final DatanodeStorageInfo storageInfo) {
    try {
      // Allow lookup of storageId (String) <--> sid (int)
      storageIdMap.update(storageInfo);

      // Also write to the storages table (mapping DN-Sid-storagetype)
      final int sid = storageInfo.getSid();
      final String datanodeUuid = storageInfo.getDatanodeDescriptor().getDatanodeUuid();
      final int storageType = storageInfo.getStorageType().ordinal();

      // Get the list of storages we know to be on this DN
      ArrayList<Integer> sids = this.datanodeUuidToSids.get(datanodeUuid);

      if(sids == null) { // First time we see this DN
        sids = new ArrayList<Integer>();
        this.datanodeUuidToSids.put(datanodeUuid, sids);
      }

      if(! sids.contains(sid)) { // We haven't seen this sid on this DN yet
        // Add in hashmap
        sids.add(sid);

        // Persist to database
        new HopsTransactionalRequestHandler(HDFSOperationType.UPDATE_SID_MAP) {
          @Override
          public void acquireLock(TransactionLocks locks) throws IOException {
            LockFactory lf = LockFactory.getInstance();
            locks.add(
                lf.getVariableLock(Variable.Finder.StorageMap,
                    LockType.READ_COMMITTED));
          }

          @Override
          public Object performTask() throws StorageException, IOException {
            StorageDataAccess<Storage> da =
                (StorageDataAccess) HdfsStorageFactory
                    .getDataAccess(StorageDataAccess.class);
            Storage h = da.findByPk(sid);
            if (h == null) {
              h = new Storage(sid, datanodeUuid, storageType);
              da.add(h);
            }
            return null;
          }
        }.handle();
      }
    } catch (IOException e) {
      // TODO throw some stuff?
      e.printStackTrace();
    }

    // Allow lookup of sid (int) -> DatanodeStorageInfo
    storageInfoMap.put(storageInfo.getSid(), storageInfo);
  }

  /**
   * StorageUuid (String) --> sid (int)
   */
  public int getSid(String storageUuid) {
    return this.storageIdMap.getSId(storageUuid);
  }

  /**
   * sid (int) --> storageUuid (String)
   */
  public String getStorageUuid(int sid) {
    return this.storageIdMap.getStorageId(sid);
  }

  /**
   * sid (int) --> DatanodeStorageInfo
   * (or null if the DatanodeStorageInfo isn't known on this NN yet)
   */
  public DatanodeStorageInfo getStorage(int sid) {
    return storageInfoMap.get(sid);
  }

  public List<Integer> getSidsForDatanodeUuid(String datanodeUuid) {
    return datanodeUuidToSids.get(datanodeUuid);
  }
}
