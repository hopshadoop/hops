package org.apache.hadoop.hdfs;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by salman on 2016-03-22.
 */

public class TestSmallFilesCreation {

  static final String hdfsClinetEmulationForSF = "hdfsClientEmulationForSF";

  static void writeFile(DistributedFileSystem dfs, String name, int size, boolean overwrite) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) dfs.create(new Path(name), overwrite);
    writeData(os, 0, size);
    os.close();
  }

  static void writeFile(FileSystem fs, String name, int size) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) fs.create(new Path(name), (short)1);
    writeData(os, 0, size);
    os.close();
  }

  static void writeData(FSDataOutputStream os, int existingSize, int size) throws IOException {
    byte[] data = new byte[size];
    for (int i = 0;  i < size; i++, existingSize++) {
      byte number = (byte) (existingSize % 128);
      data[i] = number;
    }
    os.write(data);
  }

  /**
   * This method reads the file using different read methods.
   */
  static void verifyFile(FileSystem dfs, String file, int size) throws IOException {
    //reading one byte at a time.
    FSDataInputStream is = dfs.open(new Path(file));
    byte[] buffer = new byte[size];
    IOUtils.readFully(is, buffer,0, size );
    is.close();
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted. Expecting: " + (i%128) + " got: " + buffer[i] +
                " index: " +
                "" + i);
      }
    }
  }

  public static int countDBFiles() throws IOException {
    return countInMemoryDBFiles() + countAllOnDiskDBFiles();
  }

  public static int countInMemoryDBFiles() throws IOException {
    LightWeightRequestHandler countDBFiles = new LightWeightRequestHandler(HDFSOperationType.TEST_DB_FILES) {
      @Override
      public Object performTask() throws StorageException, IOException {
        InMemoryInodeDataAccess fida =
                (InMemoryInodeDataAccess) HdfsStorageFactory.getDataAccess(InMemoryInodeDataAccess.class);
        return fida.count();
      }
    };
    return (Integer) countDBFiles.handle();
  }

  public static int countAllOnDiskDBFiles() throws IOException {
    return  countOnDiskLargeDBFiles()+countOnDiskMediumDBFiles()+countOnDiskSmallDBFiles();
  }


  public static int countOnDiskSmallDBFiles() throws IOException {
    LightWeightRequestHandler countDBFiles = new LightWeightRequestHandler(HDFSOperationType.TEST_DB_FILES) {
      @Override
      public Object performTask() throws StorageException, IOException {
        int count = 0;
        DBFileDataAccess fida;
        fida = (SmallOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(SmallOnDiskInodeDataAccess.class);
        count += fida.count();
        return count;
      }
    };
    return (Integer) countDBFiles.handle();
  }

  public static int countOnDiskMediumDBFiles() throws IOException {
    LightWeightRequestHandler countDBFiles = new LightWeightRequestHandler(HDFSOperationType.TEST_DB_FILES) {
      @Override
      public Object performTask() throws StorageException, IOException {
        int count = 0;
        DBFileDataAccess fida;
        fida = (MediumOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(MediumOnDiskInodeDataAccess.class);
        count += fida.count();
        return count;
      }
    };
    return (Integer) countDBFiles.handle();
  }

  public static int countOnDiskLargeDBFiles() throws IOException {
    LightWeightRequestHandler countDBFiles = new LightWeightRequestHandler(HDFSOperationType.TEST_DB_FILES) {
      @Override
      public Object performTask() throws StorageException, IOException {
        int count = 0;
        LargeOnDiskInodeDataAccess fida;
        fida = (LargeOnDiskInodeDataAccess) HdfsStorageFactory.getDataAccess(LargeOnDiskInodeDataAccess.class);
        count += fida.countUniqueFiles();
        return count;
      }
    };
    return (Integer) countDBFiles.handle();
  }

  /**
   * Simple read and write test
   *
   * @throws IOException
   */
  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";
      final String FILE_NAME3 = "/TEST-FLIE3";
      final String FILE_NAME4 = "/TEST-FLIE4";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();

      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);

      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME4, ONDISK_LARGE_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME4, ONDISK_LARGE_FILE_MAX_SIZE);

      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Write large file and make sure that it is stored on the datanodes
   *
   * @throws IOException
   */
  @Test
  public void TestWriteLargeFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final int FILE_SIZE = ONDISK_LARGE_FILE_MAX_SIZE + 1;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      writeFile(dfs, FILE_NAME, FILE_SIZE);

      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * if the file is small but the client calls flush method before the
   * close operation. The file will be flushed to datanodes rather than the database. This is because the
   * final size of the file is not known before the file is closed.
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHflush() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME = "/TEST-FLIE";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeData(out, 0, INMEMORY_SMALL_FILE_MAX_SIZE);
      out.flush();
      out.hflush();
      out.close();


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * if the file is small but the client calls sync ethod before the
   * close operation the save the file on the datanodes. This is because the
   * final size of the file is not known before the file is closed
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHsync() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME = "/TEST-FLIE";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeData(out, 0, INMEMORY_SMALL_FILE_MAX_SIZE);
      out.hsync();
      out.close();

      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks = dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database", lblks.hasPhantomBlock());
      assertTrue("Expecting 0 DB files. Got:" + countDBFiles(), countDBFiles() == 0);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * delete file stored in the database
   *
   * @throws IOException
   */
  @Test
  public void TestDeleteSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";
      final String FILE_NAME3 = "/TEST-FLIE3";
      final String FILE_NAME4 = "/TEST-FLIE4";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME4, ONDISK_LARGE_FILE_MAX_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      dfs.delete(new Path(FILE_NAME1));
      dfs.delete(new Path(FILE_NAME2));
      dfs.delete(new Path(FILE_NAME3));
      dfs.delete(new Path(FILE_NAME4));

      assertTrue("Expecting 0 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  test mv smallfile smallfile_new
   */
  @Test
  public void TestRenameSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";
      final String FILE_NAME3 = "/TEST-FLIE3";
      final String FILE_NAME4 = "/TEST-FLIE4";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME4, ONDISK_LARGE_FILE_MAX_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME1 + "1"));
      dfs.rename(new Path(FILE_NAME2), new Path(FILE_NAME2 + "1"));
      dfs.rename(new Path(FILE_NAME3), new Path(FILE_NAME3 + "1"));
      dfs.rename(new Path(FILE_NAME4), new Path(FILE_NAME4 + "1"));

      verifyFile(dfs, FILE_NAME1 + "1", INMEMORY_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2 + "1", ONDISK_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME3 + "1", ONDISK_MEDIUM_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME4 + "1", ONDISK_LARGE_FILE_MAX_SIZE);

      assertTrue("Expecting 1 in-memory file(s). Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  replace in-memory file with another in-memory file
  replace in-memory file with another on-disk file
  replace on-disk file with another on-disk file
   */
  @Test
  public void TestRenameSmallFiles2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // replace in-memory file with an other in-memory file
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME2, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 2", countInMemoryDBFiles() == 2);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, INMEMORY_SMALL_FILE_MAX_SIZE);

      // replace in-memory file with on-disk file
      // create on-disk small file
      writeFile(dfs, FILE_NAME1, ONDISK_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);

      //replace on disk file with another ondsik file
      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 2", countAllOnDiskDBFiles() == 2);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);
      verifyFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*

   */
  @Test
  public void TestRenameSmallFiles3() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSNamesystem namesystem = cluster.getNamesystem();

      /*create smallfile1
      create largefile1
      mv smallfile1 largefile1
      test largefile is deleted*/
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 1 block but found: " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.rename(new Path(FILE_NAME1), new Path(FILE_NAME2), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 0 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 0);
      dfs.delete(new Path(FILE_NAME2));
      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      /*create smallfile1
      create largefile1
      mv largefile1 smallfile1
      test smallfile is deleted*/
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Expecting 1 block but found: " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.rename(new Path(FILE_NAME2), new Path(FILE_NAME1), Options.Rename.OVERWRITE);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 1 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 1);
      dfs.delete(new Path(FILE_NAME1));
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 0 block but foudn " + namesystem.getTotalBlocks(), namesystem.getTotalBlocks() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Delete a directory that has both small and large files
 */
  @Test
  public void TestDelete1() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      for (int i = 0; i < 5; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, INMEMORY_SMALL_FILE_MAX_SIZE);
      }

      for (int i = 5; i < 10; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, ONDISK_SMALL_FILE_MAX_SIZE);
      }

      for (int i = 10; i < 15; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, ONDISK_MEDIUM_FILE_MAX_SIZE);
      }

      for (int i = 15; i < 20; i++) {
        writeFile(dfs, "/dir/file-db-file" + i, ONDISK_LARGE_FILE_MAX_SIZE);
      }

      for (int i = 20; i < 25; i++) {
        writeFile(dfs, "/dir/file2" + i, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      }

      assertTrue("Count of db file should be 10", countDBFiles() == 20);
      assertTrue("Expecting 5 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 5);

      dfs.delete(new Path("/dir"), true);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 0 block but foudn " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  appending large amount to data to a file stored in the database.
  the file should migrate to the datanodes
 */
  @Test
  public void TestAppendMigrateToDataNodes() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();


      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);

      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_SMALL_FILE_MAX_SIZE, ONDISK_SMALL_FILE_MAX_SIZE - INMEMORY_SMALL_FILE_MAX_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskSmallDBFiles() == 1);


      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_SMALL_FILE_MAX_SIZE, ONDISK_MEDIUM_FILE_MAX_SIZE - ONDISK_SMALL_FILE_MAX_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_MEDIUM_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_MEDIUM_FILE_MAX_SIZE, ONDISK_LARGE_FILE_MAX_SIZE - ONDISK_MEDIUM_FILE_MAX_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskMediumDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_LARGE_FILE_MAX_SIZE, 1);
      out.close();
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      assertTrue("Expecting 1 block but foudn " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE + 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  during append if sync or flush is called then store the file on the datanodes
  */
  @Test
  public void TestAppendSync() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_SMALL_FILE_MAX_SIZE, 1);
      out.hflush();
      writeData(out, INMEMORY_SMALL_FILE_MAX_SIZE+1, 1);
      out.close();

      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE + 2 );
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
   Test appending to a file stored in the database
  */
  @Test
  public void TestAppend() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();



      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);

      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE );

      assertTrue("Count of db file should be 1", countDBFiles() == 1);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_LARGE_FILE_MAX_SIZE,  1024);
      out.close();

      assertTrue("Count of db file should be 0", countDBFiles() == 0);

      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE + 1024 );
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Test overwrite
  overwrite in-memory file with another in-memory file
  overwrite on-disk file with another on-disk file
  overwrite in-memory file with on-disk file and visa versa
 */
  @Test
  public void TestOverwrite() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      // overwrite in-memory file with another in-memory file
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, ONDISK_MEDIUM_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_MEDIUM_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_MEDIUM_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite on-disk file with another on-disk file
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskSmallDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_MEDIUM_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_MEDIUM_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskMediumDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      // overwrite in-memory file with on-disk file and visa versa
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME2, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 2", countAllOnDiskDBFiles() == 1);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 0);
      assertTrue("Count of db file should be 0", countOnDiskSmallDBFiles() == 1);
      dfs.delete(new Path(FILE_NAME2));

      assertTrue("Count of db file should be 0", countDBFiles() == 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Test overwrite

 */
  @Test
  public void TestOverwrite2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      assertTrue("Count of db file should be 0", countDBFiles() == 0);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Expecting 0 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);
      dfs.delete(new Path(FILE_NAME1));

      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE + 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countOnDiskLargeDBFiles() == 1);
      assertTrue("Expecting 0 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 0);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * write small file using hdfs like client.
   * the file should be stored on the DNs
   * @throws IOException
   */
  @Test
  public void TestHdfsCompatibility1() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, false);
      conf.setBoolean(hdfsClinetEmulationForSF,true);
      FileSystem hdfsClient = FileSystem.newInstance(conf);

      writeFile(hdfsClient, FILE_NAME2, INMEMORY_SMALL_FILE_MAX_SIZE);

      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);

      verifyFile(hdfsClient, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Testing append using HDFS client.
  When a HDFS client tries to append to small file that is stored in the database then
  the file is first move to the datanodes and then the append is performed. HopsFS is
  responsible for seemlessly moving the small files to the datanodes.
   */
  @Test
  public void TestHdfsCompatibility2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, false);
      conf.setBoolean(hdfsClinetEmulationForSF,true);
      FileSystem hdfsClient = FileSystem.newInstance(conf);

      FSDataOutputStream out = hdfsClient.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_SMALL_FILE_MAX_SIZE, ONDISK_SMALL_FILE_MAX_SIZE - INMEMORY_SMALL_FILE_MAX_SIZE);
      out.close();

      assertTrue("Count of db file should be 0", countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 block but found " + cluster.getNamesystem().getTotalBlocks(), cluster.getNamesystem().getTotalBlocks() == 1);
      verifyFile(hdfsClient, FILE_NAME1, ONDISK_SMALL_FILE_MAX_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void TestSmallFilesWithNoDataNodes() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestSmallFilesReplication() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);
      assertTrue("Count of db file should be 1", countInMemoryDBFiles() == 1);
      verifyFile(dfs, FILE_NAME1, INMEMORY_SMALL_FILE_MAX_SIZE);

      dfs.setReplication(new Path(FILE_NAME1),(short)10);

      if(dfs.getFileStatus(new Path(FILE_NAME1)).getReplication() != 10){
        fail("Unable to set replication for a small file");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestSmallFileByteBufferReader() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int ONDISK_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_MEDIUM_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT);
      final int ONDISK_LARGE_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT);
      final int INMEMORY_SMALL_FILE_MAX_SIZE = conf.getInt(DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY, DFSConfigKeys.DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT);
      final String FILE_NAME1 = "/TEST-FLIE1";
      final String FILE_NAME2 = "/TEST-FLIE2";

      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(0).format(true).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      writeFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);
      verifyFile(dfs, FILE_NAME1, ONDISK_LARGE_FILE_MAX_SIZE);

      FSDataInputStream read = dfs.open(new Path(FILE_NAME1));
      ByteBuffer buf = ByteBuffer.allocate(ONDISK_LARGE_FILE_MAX_SIZE);
      while (buf.hasRemaining()) {
        int readCount = read.read(buf);
        if (readCount == -1) {
          // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
          // that has more remaining than the amount of data in the stream.
          throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

}
