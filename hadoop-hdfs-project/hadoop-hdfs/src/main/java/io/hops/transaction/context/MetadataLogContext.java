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
package io.hops.transaction.context;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.lock.TransactionLocks;

public class MetadataLogContext
    extends BaseEntityContext<MetadataLogContext.Key, MetadataLogEntry> {
  private final MetadataLogDataAccess<MetadataLogEntry> dataAccess;

  class Key {
    private long datasetId;
    private long inodeId;
    private long timestamp;

    public Key(long datasetId, long inodeId, long timestamp) {
      this.datasetId = datasetId;
      this.inodeId = inodeId;
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Key key = (Key) o;

      if (datasetId != key.datasetId) {
        return false;
      }
      if (inodeId != key.inodeId) {
        return false;
      }
      return timestamp == key.timestamp;
    }

    @Override
    public int hashCode() {
      int result = Long.hashCode(datasetId);
      result = 31 * result + Long.hashCode(inodeId);
      result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
      return result;
    }
  }

  public MetadataLogContext(
      MetadataLogDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(MetadataLogEntry logEntry)
      throws TransactionContextException {
    Key key = getKey(logEntry);
    if(get(key) != null){
      throw new RuntimeException("Conflicting logical time in the " +
          "MetadataLogEntry");
    }
    super.add(logEntry);
    log("metadata-log-added","logEntry", logEntry);
  }

  @Override
  Key getKey(MetadataLogEntry metadataLogEntry) {
    return new Key(metadataLogEntry.getDatasetId(),
        metadataLogEntry.getInodeId(), metadataLogEntry.getLogicalTime());
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.addAll(getAdded());
  }

}
