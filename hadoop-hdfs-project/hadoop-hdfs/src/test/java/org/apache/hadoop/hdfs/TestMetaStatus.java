/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the cd Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetaStatus;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

public class TestMetaStatus {
  @Test
  public void test() throws Exception {
    Logger.getRootLogger().setLevel(Level.WARN);

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_SLICER_BATCH_SIZE, 2);
    conf.setInt(DFS_REPLICATION_KEY, 3);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());

    try {
      String basePath = "/Projects/some_proj/Resources";
      fs.mkdirs(new Path(basePath));

      int files = 10;
      for (int i = 0; i < files; i++) {
        fs.create(new Path(basePath + "/file_" + i), (short) 3).close();
      }

      dfs.setMetaStatus(new Path(basePath), MetaStatus.META_ENABLED);


      long count = (long) new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {

          MetadataLogDataAccess<MetadataLogEntry> dataAccess =
                  (MetadataLogDataAccess) HdfsStorageFactory
                          .getDataAccess(MetadataLogDataAccess.class);
          return (Object) new Long(dataAccess.countAll());
        }
      }.handle();

      assert count == files + 1;

    } finally {
      cluster.shutdown();
    }
  }
}

