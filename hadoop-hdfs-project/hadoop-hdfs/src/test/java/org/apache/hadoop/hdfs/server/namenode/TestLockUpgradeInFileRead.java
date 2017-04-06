package org.apache.hadoop.hdfs.server.namenode;

/**
 * Created by salman on 3/29/17.
 */

import io.hops.exception.LockUpgradeException;
import io.hops.transaction.lock.TransactionLockTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.junit.Assert.fail;


public class TestLockUpgradeInFileRead {

  //
  // writes specified bytes to file.
  //
  public static void writeFile(FSDataOutputStream stm, int size)
          throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(0, size);
    stm.write(buffer, 0, size);
  }
  @Test
  public void TestFileReadLockUpgrade() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLOCK_SIZE = 1024;
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte
      //update the access time every time
      conf.setLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 100); // update the access time after every 100 ms
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      Path file = new Path("/file.txt");
      FSDataOutputStream out = dfs.create(file, (short)3);
      writeFile(out, 1024);
      out.close();

      FSNamesystem ns = cluster.getNamesystem();
      try{
        Thread.sleep(1000); // One second has passed and the access time should be updated
                                  // Should get a LockUpgradeException while trying to update the timestamp
                                  // while holding the read lock
        ns.getBlockLocationsWithLock("client", file.toString(), 0, 1024, TransactionLockTypes.INodeLockType.READ);
        fail("The operation was expected to fail");
      }catch(LockUpgradeException e){
      }catch (InterruptedException e){
      }

      try{
        // should succeed while holding the appropriate write lock
        ns.getBlockLocationsWithLock("client", file.toString(), 0, 1024, TransactionLockTypes.INodeLockType.WRITE);
      }catch(Exception e){
        fail("No exception was expected. Got "+e);
        e.printStackTrace();
      }


      try{
        // Now reading the file again with read lock should work
        ns.getBlockLocationsWithLock("client", file.toString(), 0, 1024, TransactionLockTypes.INodeLockType.READ);
      }catch(Exception e){
        fail("No exception was expected. Got "+e);
        e.printStackTrace();
      }

      try{
        Thread.sleep(1000);
        FSDataInputStream in = dfs.open(file);
        in.read();
        in.close();
      }catch(Exception e){
        fail("No exception was expected. Got "+e);
        e.printStackTrace();
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
