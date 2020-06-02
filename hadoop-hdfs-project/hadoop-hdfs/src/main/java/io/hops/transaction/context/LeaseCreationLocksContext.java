/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2020 Logical Clocks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.CounterType;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.LeaseCreationLocksDataAccess;
import io.hops.metadata.hdfs.entity.LeaseCreationLock;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Collection;

public class LeaseCreationLocksContext extends BaseEntityContext<Integer, LeaseCreationLock> {

  private final LeaseCreationLocksDataAccess<LeaseCreationLock> dataAccess;

  public LeaseCreationLocksContext(LeaseCreationLocksDataAccess<LeaseCreationLock> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(LeaseCreationLock lease) throws TransactionContextException {
    throw new TransactionContextException("Updating lock rows is not supported");
  }

  @Override
  public LeaseCreationLock find(FinderType<LeaseCreationLock> finder, Object... params)
          throws TransactionContextException, StorageException {

    LeaseCreationLock.Finder lFinder = (LeaseCreationLock.Finder) finder;
    switch (lFinder) {
      case ByRowID:
        return findByPK(finder, (Integer) params[0]);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void remove(LeaseCreationLock lease) throws TransactionContextException {
    throw new TransactionContextException("Counting lock rows is not supported");
  }

  @Override
  public void prepare(TransactionLocks tlm)
          throws TransactionContextException, StorageException {
    assert getRemoved().size() == 0 && getAdded().size() == 0 && getModified().size() == 0;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
  }

  @Override
  Integer getKey(LeaseCreationLock lock) {
    return lock.getLock();
  }

  private LeaseCreationLock findByPK(FinderType<LeaseCreationLock> finder, Integer rowID)
          throws StorageCallPreventedException, StorageException {
    aboutToAccessStorage(finder, rowID);
    LeaseCreationLock lock = dataAccess.lock(rowID);
    miss(finder, lock);
    return lock;
  }

}
