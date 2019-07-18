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

package org.apache.hadoop.yarn.nodelabels;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.InlineDispatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class TestFileSystemNodeLabelsStore extends NodeLabelTestBase {
  MockNodeLabelManager mgr = null;
  Configuration conf = null;
  String storeClassName = null;

  private static class MockNodeLabelManager extends
      CommonNodeLabelsManager {
    @Override
    protected void initDispatcher(Configuration conf) {
      super.dispatcher = new InlineDispatcher();
    }

    @Override
    protected void startDispatcher() {
      // do nothing
    }
    
    @Override
    protected void stopDispatcher() {
      // do nothing
    }
  }
  
  public TestFileSystemNodeLabelsStore(String className) {
    this.storeClassName = className;
  }
  
  @Parameterized.Parameters
  public static Collection<String[]> getParameters() {
    return Arrays.asList(
        new String[][] { { FileSystemNodeLabelsStore.class.getCanonicalName() },
            { NonAppendableFSNodeLabelStore.class.getCanonicalName() } });
  }

  @Before
  public void before() throws IOException {
    mgr = new MockNodeLabelManager();
    conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_IMPL_CLASS, storeClassName);
    File tempDir = File.createTempFile("nlb", ".tmp");
    tempDir.delete();
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
    mgr.init(conf);
    mgr.start();
  }

  @After
  public void after() throws IOException {
    if (mgr.store instanceof FileSystemNodeLabelsStore) {
      FileSystemNodeLabelsStore fsStore =
          ((FileSystemNodeLabelsStore) mgr.store);
      fsStore.getFs().delete(fsStore.getFsWorkingPath(), true);
    }
    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testRecoverWithMirror() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n4"), toSet("p4"), toNodeId("n5"), toSet("p5"),
        toNodeId("n6"), toSet("p6"), toNodeId("n7"), toSet("p6")));

    /*
     * node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
     */

    mgr.removeFromClusterNodeLabels(toSet("p1"));
    mgr.removeFromClusterNodeLabels(Arrays.asList("p3", "p5"));

    /*
     * After removed p2: n2 p4: n4 p6: n6, n7
     */
    // shutdown mgr and start a new mgr
    mgr.stop();

    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    // check variables
    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
        toSet("p2"), toNodeId("n4"), toSet("p4"), toNodeId("n6"), toSet("p6"),
        toNodeId("n7"), toSet("p6")));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(),
            ImmutableMap.of(
            "p6", toSet(toNodeId("n6"), toNodeId("n7")),
            "p4", toSet(toNodeId("n4")),
            "p2", toSet(toNodeId("n2"))));

    // stutdown mgr and start a new mgr
    mgr.stop();
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    // check variables
    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
        toSet("p2"), toNodeId("n4"), toSet("p4"), toNodeId("n6"), toSet("p6"),
        toNodeId("n7"), toSet("p6")));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(),
            ImmutableMap.of(
            "p6", toSet(toNodeId("n6"), toNodeId("n7")),
            "p4", toSet(toNodeId("n4")),
            "p2", toSet(toNodeId("n2"))));
    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testRecoverWithDistributedNodeLabels() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n4"), toSet("p4"), toNodeId("n5"), toSet("p5"),
        toNodeId("n6"), toSet("p6"), toNodeId("n7"), toSet("p6")));

    mgr.removeFromClusterNodeLabels(toSet("p1"));
    mgr.removeFromClusterNodeLabels(Arrays.asList("p3", "p5"));
    mgr.stop();

    mgr = new MockNodeLabelManager();
    Configuration cf = new Configuration(conf);
    cf.set(YarnConfiguration.NODELABEL_CONFIGURATION_TYPE,
        YarnConfiguration.DISTRIBUTED_NODELABEL_CONFIGURATION_TYPE);
    mgr.init(cf);
    mgr.start();

    // check variables
    Assert.assertEquals(3, mgr.getClusterNodeLabels().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    Assert.assertTrue("During recovery in distributed node-labels setup, "
        + "node to labels mapping should not be recovered ", mgr
        .getNodeLabels().size() == 0);

    mgr.stop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test(timeout = 10000)
  public void testEditlogRecover() throws Exception {
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p1", "p2", "p3"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p4"));
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p5", "p6"));
    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n4"), toSet("p4"), toNodeId("n5"), toSet("p5"),
        toNodeId("n6"), toSet("p6"), toNodeId("n7"), toSet("p6")));

    /*
     * node -> partition p1: n1 p2: n2 p3: n3 p4: n4 p5: n5 p6: n6, n7
     */

    mgr.removeFromClusterNodeLabels(toSet("p1"));
    mgr.removeFromClusterNodeLabels(Arrays.asList("p3", "p5"));

    /*
     * After removed p2: n2 p4: n4 p6: n6, n7
     */
    // shutdown mgr and start a new mgr
    mgr.stop();

    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    // check variables
    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
        toSet("p2"), toNodeId("n4"), toSet("p4"), toNodeId("n6"), toSet("p6"),
        toNodeId("n7"), toSet("p6")));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(),
            ImmutableMap.of(
            "p6", toSet(toNodeId("n6"), toNodeId("n7")),
            "p4", toSet(toNodeId("n4")),
            "p2", toSet(toNodeId("n2"))));
    mgr.stop();
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test (timeout = 10000)
  public void testSerilizationAfterRecovery() throws Exception {
    // Add to cluster node labels, p2/p6 are non-exclusive.
    mgr.addToCluserNodeLabels(Arrays.asList(NodeLabel.newInstance("p1", true),
        NodeLabel.newInstance("p2", false), NodeLabel.newInstance("p3", true),
        NodeLabel.newInstance("p4", true), NodeLabel.newInstance("p5", true),
        NodeLabel.newInstance("p6", false)));

    mgr.replaceLabelsOnNode(ImmutableMap.of(toNodeId("n1"), toSet("p1"),
        toNodeId("n2"), toSet("p2")));
    mgr.replaceLabelsOnNode((Map) ImmutableMap.of(toNodeId("n3"), toSet("p3"),
        toNodeId("n4"), toSet("p4"), toNodeId("n5"), toSet("p5"),
        toNodeId("n6"), toSet("p6"), toNodeId("n7"), toSet("p6")));

    /*
     * node -> labels 
     * p1: n1 
     * p2: n2 
     * p3: n3
     * p4: n4 
     * p5: n5 
     * p6: n6, n7
     */

    mgr.removeFromClusterNodeLabels(toSet("p1"));
    mgr.removeFromClusterNodeLabels(Arrays.asList("p3", "p5"));

    /*
     * After removed 
     * p2: n2 
     * p4: n4 
     * p6: n6, n7
     */

    // shutdown mgr and start a new mgr
    mgr.stop();

    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    // check variables
    Assert.assertEquals(3, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6")));

    assertMapContains(mgr.getNodeLabels(), ImmutableMap.of(toNodeId("n2"),
        toSet("p2"), toNodeId("n4"), toSet("p4"), toNodeId("n6"), toSet("p6"),
        toNodeId("n7"), toSet("p6")));
    assertLabelsToNodesEquals(mgr.getLabelsToNodes(),
        ImmutableMap.of(
        "p6", toSet(toNodeId("n6"), toNodeId("n7")),
        "p4", toSet(toNodeId("n4")),
        "p2", toSet(toNodeId("n2"))));

    Assert.assertFalse(mgr.isExclusiveNodeLabel("p2"));
    Assert.assertTrue(mgr.isExclusiveNodeLabel("p4"));
    Assert.assertFalse(mgr.isExclusiveNodeLabel("p6"));

    /*
     * Add label p7,p8 then shutdown
     */
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p7", "p8"));
    mgr.stop();
    
    /*
     * Restart, add label p9 and shutdown
     */
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();
    mgr.addToCluserNodeLabelsWithDefaultExclusivity(toSet("p9"));
    mgr.stop();
    
    /*
     * Recovery, and see if p9 added
     */
    mgr = new MockNodeLabelManager();
    mgr.init(conf);
    mgr.start();

    // check variables
    Assert.assertEquals(6, mgr.getClusterNodeLabelNames().size());
    Assert.assertTrue(mgr.getClusterNodeLabelNames().containsAll(
        Arrays.asList("p2", "p4", "p6", "p7", "p8", "p9")));
    mgr.stop();
  }

  @Test
  public void testRootMkdirOnInitStore() throws Exception {
    final FileSystem mockFs = Mockito.mock(FileSystem.class);
    FileSystemNodeLabelsStore mockStore = new FileSystemNodeLabelsStore() {
      public void initFileSystem(Configuration config) throws IOException {
        setFs(mockFs);
      }
    };

    mockStore.setFs(mockFs);
    verifyMkdirsCount(mockStore, true, 1);
    verifyMkdirsCount(mockStore, false, 2);
    verifyMkdirsCount(mockStore, true, 3);
    verifyMkdirsCount(mockStore, false, 4);
  }

  private void verifyMkdirsCount(FileSystemNodeLabelsStore store,
                                 boolean existsRetVal, int expectedNumOfCalls)
      throws Exception {
    Mockito.when(store.getFs().exists(Mockito.any(
        Path.class))).thenReturn(existsRetVal);
    store.init(conf, mgr);
    Mockito.verify(store.getFs(), Mockito.times(
        expectedNumOfCalls)).mkdirs(Mockito.any(Path
        .class));
  }
}
