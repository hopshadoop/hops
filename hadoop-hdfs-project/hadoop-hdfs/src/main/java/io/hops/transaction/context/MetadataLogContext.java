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
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class MetadataLogContext
    extends BaseEntityContext<MetadataLogContext.Key, MetadataLogEntry> {
  private final MetadataLogDataAccess<MetadataLogEntry> dataAccess;
  private Collection<MetadataLogEntry> existing = Collections.emptyList();

  class Key {
    private int datasetId;
    private int inodeId;
    private long timestamp;

    public Key(int datasetId, int inodeId, long timestamp) {
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
      int result = datasetId;
      result = 31 * result + inodeId;
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
    while (get(key) != null) {
      key.timestamp = logEntry.updateTimestamp();
    }
    super.add(logEntry);
    log("metadata-log-added","baseDirId", logEntry.getDatasetId(), "inodeId",logEntry.getInodeId(),"name", logEntry.getInodeName(), "pid", logEntry.getInodeParentId(), "Operation", logEntry.getOperation());
          for(int i = 0;i < Thread.currentThread().getStackTrace().length;i++){
        System.out.println((Thread.currentThread().getStackTrace()[i]));
      }
  }

  @Override
  Key getKey(MetadataLogEntry metadataLogEntry) {
    return new Key(metadataLogEntry.getDatasetId(),
        metadataLogEntry.getInodeId(), metadataLogEntry.getTimestamp());
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    for (MetadataLogEntry existingEntry : existing) {
      MetadataLogEntry cached = get(getKey(existingEntry));
      cached.updateTimestamp();
    }
    dataAccess.addAll(getAdded());
  }

  @Override
  public Collection<MetadataLogEntry> findList(
      FinderType<MetadataLogEntry> finder, Object... params)
      throws TransactionContextException, StorageException {
    MetadataLogEntry.Finder mFinder = (MetadataLogEntry.Finder) finder;
    switch (mFinder) {
      case ALL_CACHED:
        return new ArrayList<MetadataLogEntry>(getAll());
      case FETCH_EXISTING:
        return fetchExisting((Collection<MetadataLogEntry>) params[0]);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  private Collection<MetadataLogEntry> fetchExisting(
      Collection<MetadataLogEntry> toRead) throws StorageException {
    existing = dataAccess.readExisting(toRead);
    return existing;
  }
}
