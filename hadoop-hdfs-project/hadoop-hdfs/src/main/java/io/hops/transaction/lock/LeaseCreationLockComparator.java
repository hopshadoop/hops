package io.hops.transaction.lock;

import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.util.Comparator;

public class LeaseCreationLockComparator implements Comparator<String> {

  final int LEASE_CREATION_LOCK_ROWS;
  public LeaseCreationLockComparator(int LEASE_CREATION_LOCK_ROWS ){
    this.LEASE_CREATION_LOCK_ROWS = LEASE_CREATION_LOCK_ROWS;
  }

  @Override
  public int compare(String h0, String h1) {
    int lockRow0 = Math.abs(Lease.getHolderId(h0)) % LEASE_CREATION_LOCK_ROWS;
    int lockRow1 = Math.abs(Lease.getHolderId(h1)) % LEASE_CREATION_LOCK_ROWS;
    return Integer.compare(lockRow0, lockRow1);
  }
}
