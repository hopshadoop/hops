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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.*;

public class BlockStoragePolicySuite {
  private static final Log LOG = LogFactory.getLog(BlockStoragePolicySuite.class);

  public final static byte ID_UNSPECIFIED = 0;

  /**
   * Store all replica's on DISK.
   */
  private final static BlockStoragePolicy WARM = new BlockStoragePolicy(
      WARM_STORAGE_POLICY_ID,
      WARM_STORAGE_POLICY_NAME,
      new StorageType[]{StorageType.DISK},
      new StorageType[]{},
      new StorageType[]{});

  /**
   * Store one replica on SSD, the rest on DISK.
   */
  private final static BlockStoragePolicy ONE_SSD = new BlockStoragePolicy(
      ONESSD_STORAGE_POLICY_ID,
      ONESSD_STORAGE_POLICY_NAME,
      new StorageType[]{StorageType.SSD, StorageType.DISK},
      new StorageType[]{},
      new StorageType[]{});

  /**
   * Attempt to store all replica's on ssd, but fallback to DISK if no space
   * available.
   */
  private final static BlockStoragePolicy ALL_SSD = new BlockStoragePolicy(
      ALLSSD_STORAGE_POLICY_ID,
      ALLSSD_STORAGE_POLICY_NAME,
      new StorageType[]{StorageType.SSD},
      new StorageType[]{StorageType.DISK},
      new StorageType[]{StorageType.DISK});

  public final static BlockStoragePolicy DEFAULT = WARM;

  /**
   * @return the corresponding policy, or {@link BlockStoragePolicySuite#DEFAULT}
   * if no blockStoragePolicy is specified ({@link BlockStoragePolicySuite#ID_UNSPECIFIED}).
   */
  public static BlockStoragePolicy getPolicy(byte id) {
    LOG.debug("called getPolicy(" + id + ")");
    switch(id) {
      case ID_UNSPECIFIED:
        return DEFAULT;
      case WARM_STORAGE_POLICY_ID:
        return WARM;
      case ONESSD_STORAGE_POLICY_ID:
        return ONE_SSD;
      case ALLSSD_STORAGE_POLICY_ID:
        return ALL_SSD;
      default:
        LOG.debug("getPolicy() called with unknown/unspecified storagePolicyID " + id);
        return DEFAULT;
    }
  }

  public static BlockStoragePolicy getPolicy(String name) {
    LOG.debug("called getPolicy(\"" + name + "\")");

    if(WARM_STORAGE_POLICY_NAME.equals(name)) {
      return WARM;
    } else if(ONESSD_STORAGE_POLICY_NAME.equals(name)) {
      return ONE_SSD;
    } else if(ALLSSD_STORAGE_POLICY_NAME.equals(name)) {
      return ALL_SSD;
    } else {
       return null;
    }
  }

  public static BlockStoragePolicy getDefaultPolicy() {
    return DEFAULT;
  }

  public static BlockStoragePolicy[] getAllStoragePolicies() {
    return new BlockStoragePolicy[]{WARM, ONE_SSD, ALL_SSD};
  }
}
