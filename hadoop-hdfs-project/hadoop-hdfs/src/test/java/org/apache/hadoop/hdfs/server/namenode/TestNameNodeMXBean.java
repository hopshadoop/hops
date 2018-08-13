/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.NoMlockCacheManipulator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link NameNodeMXBean} implementation
 */
public class TestNameNodeMXBean {

  /**
   * Used to assert equality between doubles
   */
  private static final double DELTA = 0.000001;

  static {
    NativeIO.POSIX.setCacheManipulator(new NoMlockCacheManipulator());
  }
  
  @SuppressWarnings({"unchecked", "deprecation"})
  @Test
  public void testNameNodeMXBeanInfo() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
      NativeIO.POSIX.getCacheManipulator().getMemlockLimit());
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName =
          new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo");
      // get attribute "ClusterId"
      String clusterId = (String) mbs.getAttribute(mxbeanName, "ClusterId");
      assertEquals(fsn.getClusterId(), clusterId);
      // get attribute "BlockPoolId"
      String blockpoolId = (String) mbs.getAttribute(mxbeanName, "BlockPoolId");
      assertEquals(fsn.getBlockPoolId(), blockpoolId);
      // get attribute "Version"
      String version = (String) mbs.getAttribute(mxbeanName, "Version");
      assertEquals(fsn.getVersion(), version);
      assertTrue(version.equals(
          VersionInfo.getVersion() + ", r" + VersionInfo.getRevision()));
      // get attribute "Used"
      Long used = (Long) mbs.getAttribute(mxbeanName, "Used");
      assertEquals(fsn.getUsed(), used.longValue());
      // get attribute "Total"
      Long total = (Long) mbs.getAttribute(mxbeanName, "Total");
      assertEquals(fsn.getTotal(), total.longValue());
      // get attribute "safemode"
      String safemode = (String) mbs.getAttribute(mxbeanName, "Safemode");
      assertEquals(fsn.getSafemode(), safemode);
      // get attribute nondfs
      Long nondfs = (Long) (mbs.getAttribute(mxbeanName, "NonDfsUsedSpace"));
      assertEquals(fsn.getNonDfsUsedSpace(), nondfs.longValue());
      // get attribute percentremaining
      Float percentremaining =
          (Float) (mbs.getAttribute(mxbeanName, "PercentRemaining"));
      assertEquals(fsn.getPercentRemaining(), percentremaining,
          DELTA);
      // get attribute Totalblocks
      Long totalblocks = (Long) (mbs.getAttribute(mxbeanName, "TotalBlocks"));
      assertEquals(fsn.getTotalBlocks(), totalblocks.longValue());
      // get attribute alivenodeinfo
      String alivenodeinfo =
          (String) (mbs.getAttribute(mxbeanName, "LiveNodes"));
      Map<String, Map<String, Object>> liveNodes =
          (Map<String, Map<String, Object>>) JSON.parse(alivenodeinfo);
      assertTrue(liveNodes.size() > 0);
      for (Map<String, Object> liveNode : liveNodes.values()) {
        assertTrue(liveNode.containsKey("nonDfsUsedSpace"));
        assertTrue(((Long) liveNode.get("nonDfsUsedSpace")) > 0);
        assertTrue(liveNode.containsKey("capacity"));
        assertTrue(((Long) liveNode.get("capacity")) > 0);
        assertTrue(liveNode.containsKey("numBlocks"));
        assertTrue(((Long) liveNode.get("numBlocks")) == 0);
      }
      assertEquals(fsn.getLiveNodes(), alivenodeinfo);
      // get attribute deadnodeinfo
      String deadnodeinfo =
          (String) (mbs.getAttribute(mxbeanName, "DeadNodes"));
      assertEquals(fsn.getDeadNodes(), deadnodeinfo);
      // get attribute NodeUsage
      String nodeUsage = (String) (mbs.getAttribute(mxbeanName,
          "NodeUsage"));
      assertEquals("Bad value for NodeUsage", fsn.getNodeUsage(), nodeUsage);
      // get attribute "NNStarted"
      String nnStarted = (String) mbs.getAttribute(mxbeanName, "NNStarted");
      assertEquals("Bad value for NNStarted", fsn.getNNStarted(), nnStarted);
      // get attribute "CompileInfo"
      String compileInfo = (String) mbs.getAttribute(mxbeanName, "CompileInfo");
      assertEquals("Bad value for CompileInfo", fsn.getCompileInfo(), compileInfo);
      // get attribute CorruptFiles
      String corruptFiles = (String) (mbs.getAttribute(mxbeanName,
          "CorruptFiles"));
      assertEquals("Bad value for CorruptFiles", fsn.getCorruptFiles(), corruptFiles);
      assertEquals(0L, mbs.getAttribute(mxbeanName, "CacheUsed"));
      assertEquals(NativeIO.POSIX.getCacheManipulator().getMemlockLimit() * 
          cluster.getDataNodes().size(),
              mbs.getAttribute(mxbeanName, "CacheCapacity"));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
