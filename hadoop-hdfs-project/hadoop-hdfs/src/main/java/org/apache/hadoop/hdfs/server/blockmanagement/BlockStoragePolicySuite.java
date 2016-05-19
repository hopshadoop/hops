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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.*;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ALLSSD_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ALLSSD_STORAGE_POLICY_NAME;

public class BlockStoragePolicySuite {
  private static final Log LOG = LogFactory.getLog(BlockStoragePolicySuite.class);

  /**
   * Storage type unspecified: inherits parent storage type, or DEFAULT (for
   * root folder)
   */
  public final static byte ID_UNSPECIFIED = 0;

  /**
   * Array where the ID of each policy maps to the actual policy.
   * Length = max(id's + 1)
   */
  private static BlockStoragePolicy[] policies = new BlockStoragePolicy[13];
  static {
    /*
    In accordance with HDFS:

    Policy ID	Policy Name	  Block Placement         Fallback storages   Fallback storages
                             (n  replicas)	         for creation	       for replication
    (15	      Lasy_Persist	RAM_DISK: 1, DISK: n-1	   DISK	              DISK)   <-- not implemented in Hops
    12	      All_SSD	      SSD: n	                   DISK	              DISK
    10	      One_SSD	      SSD: 1,DISK: n-1           SSD, DISK	        SSD, DISK
    7	        Hot (default)	DISK: n	                   <none>	            ARCHIVE
    5	        Warm	        DISK: 1, ARCHIVE: n-1    	 ARCHIVE, DISK	    ARCHIVE, DISK
    2	        Cold	        ARCHIVE: n	               <none>	            <none>

    New type:
    4         RAID5         RAID5                     DISK                DISK
  */

    /** Attempt to store all replica's on SSD. */
    final BlockStoragePolicy ALL_SSD = new BlockStoragePolicy(
        ALLSSD_STORAGE_POLICY_ID, ALLSSD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.SSD},
        new StorageType[]{StorageType.DISK},
        new StorageType[]{StorageType.DISK});

    /** Store one replica on SSD, the rest on DISK. */
    final BlockStoragePolicy ONE_SSD = new BlockStoragePolicy(
        ONESSD_STORAGE_POLICY_ID, ONESSD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK},
        new StorageType[]{StorageType.SSD, StorageType.DISK});

    /** Store all replica's on DISK. */
    final BlockStoragePolicy HOT = new BlockStoragePolicy(
        HOT_STORAGE_POLICY_ID, HOT_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK},
        new StorageType[]{},
        new StorageType[]{StorageType.ARCHIVE});

    /** Store 1 replica on DISK, rest on ARCHIVE. */
    final BlockStoragePolicy WARM = new BlockStoragePolicy(
        WARM_STORAGE_POLICY_ID, WARM_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.DISK, StorageType.ARCHIVE},
        new StorageType[]{StorageType.ARCHIVE, StorageType.DISK},
        new StorageType[]{StorageType.ARCHIVE, StorageType.DISK});

    /** Store all replica's on ARCHIVE. */
    final BlockStoragePolicy COLD = new BlockStoragePolicy(
        COLD_STORAGE_POLICY_ID, COLD_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.ARCHIVE},
        new StorageType[]{},
        new StorageType[]{});

    /** Store all replica's on RAID (nice for combining with Erasure Coding). */
    final BlockStoragePolicy RAID5 = new BlockStoragePolicy(
        RAID5_STORAGE_POLICY_ID,
        RAID5_STORAGE_POLICY_NAME,
        new StorageType[]{StorageType.RAID5},
        new StorageType[]{},
        new StorageType[]{});

    policies[ALLSSD_STORAGE_POLICY_ID] = ALL_SSD; // 12
    policies[ONESSD_STORAGE_POLICY_ID] = ONE_SSD; // 10
    policies[HOT_STORAGE_POLICY_ID] = HOT; // 7
    policies[WARM_STORAGE_POLICY_ID] = WARM; // 5
    policies[RAID5_STORAGE_POLICY_ID] = RAID5; // 3
    policies[COLD_STORAGE_POLICY_ID] = COLD; // 2
    policies[ID_UNSPECIFIED] = HOT; // 0
  }

  public final static BlockStoragePolicy DEFAULT = policies[0];

  /**
   * @return the corresponding policy, or {@link BlockStoragePolicySuite#DEFAULT}
   * if no blockStoragePolicy is specified ({@link BlockStoragePolicySuite#ID_UNSPECIFIED}).
   */
  public static BlockStoragePolicy getPolicy(byte id) {
    LOG.debug("called getPolicy(" + id + ")");

    if(0 <= id && id < policies.length && policies[id] != null) {
      return policies[id];
    }

    LOG.debug("getPolicy() called with unknown/unspecified storagePolicyID " + id);
    return null;
  }

  public static BlockStoragePolicy getPolicy(String name) {
    LOG.debug("called getPolicy(\"" + name + "\")");
    Preconditions.checkNotNull(name);

    if (policies != null) {
      for (BlockStoragePolicy policy : policies) {
        if (policy != null && policy.getName().equalsIgnoreCase(name)) {
          return policy;
        }
      }
    }
    return null;
  }

  public static BlockStoragePolicy getDefaultPolicy() {
    return DEFAULT;
  }

  public static BlockStoragePolicy[] getAllStoragePolicies() {
    return policies;
  }
}
