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

import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StorageMap {
  // TODO is this the best place for this?
  // TODO check if there ever is concurrent access to this map (now
  // everything is in synchronized blocks, but is that necessary?
  private Map<Integer, DatanodeStorageInfo> storageInfoMap =
      Collections.synchronizedMap(new HashMap<Integer, DatanodeStorageInfo>());

  /**
   * Adds or replaces storageinfo for the given sid
   */
  public void updateStorage(DatanodeStorageInfo storageInfo) {
    storageInfoMap.put(storageInfo.getSid(), storageInfo);
  }

  public DatanodeStorageInfo getStorage(int sid) {
    return storageInfoMap.get(sid);
  }
}
