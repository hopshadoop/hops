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
    private int logicTime;

    public Key(int datasetId, int inodeId, int logicTime) {
      this.datasetId = datasetId;
      this.inodeId = inodeId;
      this.logicTime = logicTime;
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
      return logicTime == key.logicTime;
    }

    @Override
    public int hashCode() {
      int result = datasetId;
      result = 31 * result + inodeId;
      result = 31 * result + logicTime;
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
      key.logicTime++;
    }
    logEntry.setLogicalTime(key.logicTime);
    super.add(logEntry);
  }

  @Override
  Key getKey(MetadataLogEntry metadataLogEntry) {
    return new Key(metadataLogEntry.getDatasetId(),
        metadataLogEntry.getInodeId(), metadataLogEntry.getLogicalTime());
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    for (MetadataLogEntry existingEntry : existing) {
      MetadataLogEntry cached = get(getKey(existingEntry));
      cached.incrementLogicalTime();
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
