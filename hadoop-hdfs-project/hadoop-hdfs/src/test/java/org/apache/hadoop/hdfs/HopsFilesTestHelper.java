/*
 * Copyright (C) 2019 LogicalClocks.
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
package org.apache.hadoop.hdfs;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.security.UsersGroups;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.*;

public class HopsFilesTestHelper {

  public static void writeFile(DistributedFileSystem dfs, String name, int size,
                              boolean overwrite) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) dfs.create(new Path(name), overwrite);
    writeData(os, 0, size);
    os.close();
  }

  public static void writeFile(FileSystem fs, String name, int size) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) fs.create(new Path(name), (short) 1);
    writeData(os, 0, size);
    os.close();
  }

  public static void writeData(FSDataOutputStream os, int existingSize, int size) throws IOException {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++, existingSize++) {
      byte number = (byte) (existingSize % 128);
      data[i] = number;
    }
    os.write(data);
  }

  /**
   * This method reads the file using different read methods.
   */
  public static void verifyFile(FileSystem dfs, String file, int size) throws IOException {
      //verify size
      long sizeDFS = dfs.getFileStatus(new Path(file)).getLen();
      assertTrue("Expected: "+size+" Actual: "+sizeDFS, sizeDFS == size);

    //verify content
    //reading one byte at a time.
    FSDataInputStream is = dfs.open(new Path(file));
    byte[] buffer = new byte[size];
    IOUtils.readFully(is, buffer, 0, size);
    is.close();
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted. Expecting: " + (i % 128) + " got: " + buffer[i] +
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
    return countOnDiskLargeDBFiles() + countOnDiskMediumDBFiles() + countOnDiskSmallDBFiles();
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
}
