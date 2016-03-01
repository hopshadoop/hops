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
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.entity.StorageId;
import io.hops.transaction.handler.HDFSOperationType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StorageMap {
  /**
   * Stores storageID->sid and sid->storageID mapping.
   * Works effectively like a cache to avoid hitting the DAL.
   */
  private StorageIdMap storageIdMap;

  private Map<Integer, DatanodeStorageInfo> storageInfoMap =
      Collections.synchronizedMap(new HashMap<Integer, DatanodeStorageInfo>());

  public StorageMap() {
    // TODO throw some stuff?
    try {
      this.storageIdMap = new StorageIdMap();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Adds or replaces storageinfo for the given sid
   */
  public void updateStorage(DatanodeStorageInfo storageInfo) {
    // Allow lookup of storageId (String) -> sid (int)
    try {
      storageIdMap.update(storageInfo);
    } catch (IOException e) {
      // TODO throw some stuff?
      e.printStackTrace();
    }

    // Allow lookup of sid (int) ->
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
}
