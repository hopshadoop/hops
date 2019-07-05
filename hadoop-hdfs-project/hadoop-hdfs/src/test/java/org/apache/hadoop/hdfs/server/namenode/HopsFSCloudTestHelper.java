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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.IOException;

import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HopsFSCloudTestHelper {

  static final Log LOG = LogFactory.getLog(HopsFSCloudTestHelper.class);
  @Rule
  public TestName name = new TestName();

  static void writeFile(DistributedFileSystem dfs, String name, int size, boolean overwrite) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) dfs.create(new Path(name), overwrite);
    writeData(os, 0, size);
    os.close();
  }

  public static void writeFile(FileSystem fs, String name, int size) throws IOException {
    FSDataOutputStream os = (FSDataOutputStream) fs.create(new Path(name), (short) 1);
    writeData(os, 0, size);
    os.close();
  }

  static void writeData(FSDataOutputStream os, int existingSize, int size) throws IOException {
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
    long actuallen = dfs.getFileStatus(new Path(file)).getLen();
//    assertTrue("File size Mismatch. Expecting: "+size+" Got: "+actuallen, size == actuallen);

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

  public static StorageType[][] genStorageTypes(int numDataNodes) {

    StorageType[][] types = new StorageType[numDataNodes][];
    for (int i = 0; i < numDataNodes; i++) {
      types[i] = new StorageType[]{StorageType.DISK, StorageType.CLOUD};
    }
    return types;
  }

  public static void purgeS3() throws IOException {
   purgeS3("unittesting");
  }

  public static void purgeS3(String prefix) throws IOException {
    LOG.info("HopsFS-Cloud. Purging all buckets");
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
    conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());

    CloudPersistenceProvider cloudConnector =
            CloudPersistenceProviderFactory.getCloudClient(conf);
    cloudConnector.deleteAllBuckets(prefix);
    cloudConnector.shutdown();
  }

}
