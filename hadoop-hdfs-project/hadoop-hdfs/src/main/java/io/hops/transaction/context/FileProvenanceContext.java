/*
 * Copyright (C) 2019 hops.io.
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
package io.hops.transaction.context;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.FileProvenanceEntry;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Objects;
import io.hops.metadata.hdfs.dal.FileProvenanceDataAccess;

public class FileProvenanceContext extends BaseEntityContext<FileProvenanceContext.DBKey, FileProvenanceEntry> {
  
  private final FileProvenanceDataAccess<FileProvenanceEntry> dataAccess;

  class DBKey {

    private final long inodeId;
    private final int userId;
    private final String appId;
    private final long timestamp;
    private final long logicalTime;
    private final String operation;
    public DBKey(long inodeId, int userId, String appId, long timestamp, long logicalTime, String operation) {
      this.inodeId = inodeId;
      this.userId = userId;
      this.appId = appId;
      this.timestamp = timestamp;
      this.logicalTime = logicalTime;
      this.operation = operation;
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 17 * hash + (int) (this.inodeId ^ (this.inodeId >>> 32));
      hash = 17 * hash + this.userId;
      hash = 17 * hash + Objects.hashCode(this.appId);
      hash = 17 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DBKey other = (DBKey) obj;
      if (this.inodeId != other.inodeId) {
        return false;
      }
      if (this.userId != other.userId) {
        return false;
      }
      if (this.timestamp != other.timestamp) {
        return false;
      }
      if (!Objects.equals(this.appId, other.appId)) {
        return false;
      }
      return true;
    }
  }

  public FileProvenanceContext(FileProvenanceDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(FileProvenanceEntry logEntry)
    throws TransactionContextException {
    DBKey dbKey = getKey(logEntry);
    if (get(dbKey) != null) {
      throw new RuntimeException("Conflicting logical time in the "
        + "ProvenanceLogEntry");
    }
    
    super.add(logEntry);
    log("provenance-log-added", "inodeId", logEntry.getInodeId(), "userId",
      logEntry.getUserId(), "appId", logEntry.getAppId(), "operation",
      logEntry.getOperationEnumVal());
  }

  @Override
  DBKey getKey(FileProvenanceEntry logEntry) {
    return new DBKey(logEntry.getInodeId(), logEntry.getUserId(),
      logEntry.getAppId(), logEntry.getTimestamp(), logEntry.getLogicalTime(), logEntry.getOperation());
  }

  @Override
  public void prepare(TransactionLocks tlm)
    throws TransactionContextException, StorageException {
    dataAccess.addAll(getAdded());
  }

}