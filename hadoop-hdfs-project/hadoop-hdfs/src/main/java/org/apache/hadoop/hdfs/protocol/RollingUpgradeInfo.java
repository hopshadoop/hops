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
package org.apache.hadoop.hdfs.protocol;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.common.entity.Variable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Rolling upgrade information
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RollingUpgradeInfo extends RollingUpgradeStatus {
  private final long startTime;
  private final long finalizeTime;
  
  public RollingUpgradeInfo(String blockPoolId,
      long startTime, long finalizeTime) {
    super(blockPoolId);
    this.startTime = startTime;
    this.finalizeTime = finalizeTime;
  }
  
  public boolean isStarted() {
    return startTime != 0;
  }
  
  /** @return The rolling upgrade starting time. */
  public long getStartTime() {
    return startTime;
  }
  
  public boolean isFinalized() {
    return finalizeTime != 0;
  }

  public long getFinalizeTime() {
    return finalizeTime;
  }

  @Override
  public int hashCode() {
    //only use lower 32 bits
    return super.hashCode() ^ (int)startTime ^ (int)finalizeTime;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof RollingUpgradeInfo)) {
      return false;
    }
    final RollingUpgradeInfo that = (RollingUpgradeInfo)obj;
    return super.equals(that)
        && this.startTime == that.startTime
        && this.finalizeTime == that.finalizeTime;
  }

  @Override
  public String toString() {
    return super.toString()
      +  "\n     Start Time: " + (startTime == 0? "<NOT STARTED>": timestamp2String(startTime))
      +  "\n  Finalize Time: " + (finalizeTime == 0? "<NOT FINALIZED>": timestamp2String(finalizeTime));
  }
  
  private static String timestamp2String(long timestamp) {
    return new Date(timestamp) + " (=" + timestamp + ")";
  }

  public static class Bean {
    private final String blockPoolId;
    private final long startTime;
    private final long finalizeTime;

    public Bean(RollingUpgradeInfo f) {
      this.blockPoolId = f.getBlockPoolId();
      this.startTime = f.startTime;
      this.finalizeTime = f.finalizeTime;
    }

    public String getBlockPoolId() {
      return blockPoolId;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getFinalizeTime() {
      return finalizeTime;
    }

  }
  
//  public static RollingUpgradeInfo getRollingUpgradeInfoFromDB() throws IOException {
//    return (RollingUpgradeInfo) new HopsTransactionalRequestHandler(HDFSOperationType.GET_ROLLING_UPGRADE_INFO) {
//      @Override
//      public void acquireLock(TransactionLocks locks) throws IOException {
//        LockFactory lf = LockFactory.getInstance();
//        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo, TransactionLockTypes.LockType.READ));
//      }
//
//      @Override
//      public Object performTask() throws StorageException, IOException {
//        return HdfsVariables.getStorageInfo();
//      }
//    }.handle();
//  }
//
//  public static void storeRollingUpgradeToDB(final RollingUpgradeInfo rollingUpgradeInfo) throws IOException { 
//    
//    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ROLLING_UPGRADE_INFO) {
//      @Override
//      public void acquireLock(TransactionLocks locks) throws IOException {
//        LockFactory lf = LockFactory.getInstance();
//        locks.add(lf.getVariableLock(Variable.Finder.RollingUpgradeInfo,TransactionLockTypes.LockType.WRITE));
//      }
//
//      @Override
//      public Object performTask() throws StorageException, IOException {
//        HdfsVariables.setRollingUpgradeInfo(rollingUpgradeInfo);
//        return null;
//      }
//    }.handle();
//  }
}
