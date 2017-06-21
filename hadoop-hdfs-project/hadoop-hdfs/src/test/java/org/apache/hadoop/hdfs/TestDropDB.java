/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the cd Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import static org.junit.Assert.assertEquals;

/**
 * This class tests various cases during file creation.
 */
public class TestDropDB {
  //this fails subsequent tests as the command does not recreate
  //disk data tables.


//  @Test
//  public void testDropDB() throws IOException {
//    MiniDFSCluster cluster = null;
//    try {
//      Configuration conf = new HdfsConfiguration();
//      String[] argv = {"-dropAndCreateDB"};
//      try {
//        NameNode.createNameNode(argv, conf);
//      } catch (ExitUtil.ExitException e) {
//        assertEquals("Drop and re-create db should have succeeded", 0, e.status);
//      }
//
//    } finally {
//    }
//  }
}
