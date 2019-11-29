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
import io.hops.metadata.hdfs.dal.FileProvXAttrBufferDataAccess;
import io.hops.transaction.lock.TransactionLocks;
import java.util.Objects;
import io.hops.metadata.hdfs.entity.FileProvXAttrBufferEntry;

public class FileProvXAttrBufferContext extends BaseEntityContext<FileProvXAttrBufferContext.Key, FileProvXAttrBufferEntry> {

  private final FileProvXAttrBufferDataAccess<FileProvXAttrBufferEntry> dataAccess;

  /*
   * Our entity uses inode id and user as a composite key.
   * Hence, we need to implement a composite key class.
   */
  class Key {

    long inodeId;
    byte namespace;
    String name;
    long inodeLogicalTime;

    public Key(long inodeId, byte namespace, String name, long inodeLogicalTime) {
      this.inodeId = inodeId;
      this.namespace = namespace;
      this.name = name;
      this.inodeLogicalTime = this.inodeLogicalTime;
    }

    @Override
    public int hashCode() {
      int hash = 5;
      hash = 89 * hash + (int) (this.inodeId ^ (this.inodeId >>> 32));
      hash = 89 * hash + this.namespace;
      hash = 89 * hash + Objects.hashCode(this.name);
      hash = 89 * hash + (int) (this.inodeLogicalTime ^ (this.inodeLogicalTime >>> 32));
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
      final Key other = (Key) obj;
      if (this.inodeId != other.inodeId) {
        return false;
      }
      if (this.namespace != other.namespace) {
        return false;
      }
      if (this.inodeLogicalTime != other.inodeLogicalTime) {
        return false;
      }
      if (!Objects.equals(this.name, other.name)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "Key{" + "inodeId=" + inodeId + ", namespace=" + namespace + ", name=" + name + ", inodeLogicalTime="
        + inodeLogicalTime + '}';
    }
  }

  public FileProvXAttrBufferContext(FileProvXAttrBufferDataAccess<FileProvXAttrBufferEntry> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(FileProvXAttrBufferEntry entry)
    throws TransactionContextException {
    super.add(entry);
    log("provenance-log-added", "inodeId", entry.getInodeId(), "namespace",
      entry.getNamespace(), "name", entry.getName(), "inodeLogicalTime", entry.getINodeLogicalTime());
  }

  @Override
  Key getKey(FileProvXAttrBufferEntry entry) {
    return new Key(entry.getInodeId(), entry.getNamespace(), entry.getName(), entry.getINodeLogicalTime());
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    dataAccess.addAll(getAdded());
  }
}
