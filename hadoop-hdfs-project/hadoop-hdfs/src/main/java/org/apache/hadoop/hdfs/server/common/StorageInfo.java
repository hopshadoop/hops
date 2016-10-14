/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common;

import com.google.common.base.Joiner;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.common.entity.Variable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Common class for storage information.
 * <p/>
 * TODO namespaceID should be long and computed as hash(address + port)
 */
@InterfaceAudience.Private
public class StorageInfo {

  public static final Log LOG = LogFactory.getLog(StorageInfo.class);
  public static final int DEFAULT_ROW_ID = 0;
      // StorageInfo is stored as one row in the database.
  protected String blockpoolID = "";
      // id of the block pool. moved it from NNStorage.java to here. This is where it should have been
  private static StorageInfo storageInfo = null;

  public int layoutVersion;   // layout version of the storage data
  public int namespaceID;     // id of the file system
  public String clusterID;      // id of the cluster
  public long cTime;           // creation time of the file system state

  public StorageInfo() {
    this(0, 0, "", 0L, "");
  }

  public StorageInfo(int layoutV, int nsID, String cid, long cT, String bpid) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;

    blockpoolID = bpid;

  }
  
  public StorageInfo(StorageInfo from) {
    setStorageInfo(from);
  }

  /**
   * Layout version of the storage data.
   */
  public int getLayoutVersion() {
    return layoutVersion;
  }

  /**
   * Namespace id of the file system.<p>
   * Assigned to the file system at formatting and never changes after that.
   * Shared by all file system components.
   */
  public int getNamespaceID() {
    return namespaceID;
  }

  /**
   * cluster id of the file system.<p>
   */
  public String getClusterID() {
    return clusterID;
  }
  
  /**
   * Creation time of the file system state.<p>
   * Modified during upgrades.
   */
  public long getCTime() {
    return cTime;
  }
  
  public void setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    clusterID = from.clusterID;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  public boolean versionSupportsFederation() {
    return LayoutVersion.supports(Feature.FEDERATION, layoutVersion);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterID)
        .append(";nsid=").append(namespaceID).append(";c=").append(cTime);
    return sb.toString();
  }
  
  public String toColonSeparatedString() {
    return Joiner.on(":").join(layoutVersion, namespaceID, cTime, clusterID);
  }
  

  public static StorageInfo getStorageInfoFromDB() throws IOException {
    if (storageInfo == null) {
      storageInfo = (StorageInfo) new HopsTransactionalRequestHandler(
          HDFSOperationType.GET_STORAGE_INFO) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = LockFactory.getInstance();
          locks.add(lf.getVariableLock(Variable.Finder.StorageInfo,
              TransactionLockTypes.LockType.READ));
        }

        @Override
        public Object performTask() throws StorageException, IOException {
          return HdfsVariables.getStorageInfo();
        }
      }.handle();
    }
    return storageInfo;
  }

  public static void storeStorageInfoToDB(final String clusterId) throws
      IOException { // should only be called by the format function once during the life time of the cluster.
    // Solution. call format on only one namenode or every one puts the same values.
    
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_STORAGE_INFO) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.StorageInfo,
            TransactionLockTypes.LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Configuration conf = new Configuration();
        String bpid = newBlockPoolID();
        storageInfo = new StorageInfo(HdfsConstants.LAYOUT_VERSION,
            conf.getInt(DFSConfigKeys.DFS_NAME_SPACE_ID_KEY,
                DFSConfigKeys.DFS_NAME_SPACE_ID_DEFAULT), clusterId, 0L, bpid);
        HdfsVariables.setStorageInfo(storageInfo);
        LOG.info("Added new entry to storage info. nsid:" +
            DFSConfigKeys.DFS_NAME_SPACE_ID_KEY + " CID:" + clusterId +
            " pbid:" + bpid);
        return null;
      }
    }.handle();
  }
  
  public String getBlockPoolId() {
    return blockpoolID;
  }
  
  static String newBlockPoolID() throws UnknownHostException {
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException e) {
      System.out.println("Could not find ip address of \"default\" inteface.");
      throw e;
    }

    int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
    String bpid = "BP-" + rand + "-" + ip + "-" + Time.now();
    return bpid;
  }
  
  /**
   * Generate new clusterID.
   * <p/>
   * clusterID is a persistent attribute of the cluster.
   * It is generated when the cluster is created and remains the same
   * during the life cycle of the cluster.  When a new name node is formated,
   * if
   * this is a new cluster, a new clusterID is geneated and stored.  Subsequent
   * name node must be given the same ClusterID during its format to be in the
   * same cluster.
   * When a datanode register it receive the clusterID and stick with it.
   * If at any point, name node or data node tries to join another cluster, it
   * will be rejected.
   *
   * @return new clusterID
   */
  public static String newClusterID() {
    return "CID-" + UUID.randomUUID().toString();
  }
  
  public int getDefaultRowId() {
    return this.DEFAULT_ROW_ID;
  }

}
