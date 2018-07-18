/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import static io.hops.transaction.lock.LockFactory.getInstance;
import io.hops.transaction.lock.TransactionLocks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.util.LightWeightCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRetryCacheWithHA {

  private static final Log LOG = LogFactory.getLog(TestRetryCacheWithHA.class);
  
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;
  private static Configuration conf = new HdfsConfiguration();
  
  private static final int BlockSize = 1024;
  private static final short DataNodes = 3;
  private final static Map<String, Object> results = new HashMap<String, Object>();

  /**
   * A dummy invocation handler extending RetryInvocationHandler. We can use
   * a boolean flag to control whether the method invocation succeeds or not.
   */
  private static class DummyRetryInvocationHandler extends
      RetryInvocationHandler {

    static AtomicBoolean block = new AtomicBoolean(false);
    
    DummyRetryInvocationHandler(
        FailoverProxyProvider<ClientProtocol> proxyProvider,
        RetryPolicy retryPolicy) {
      super(proxyProvider, retryPolicy);
    }
    
    @Override
    protected Object invokeMethod(Method method, Object[] args)
        throws Throwable {
      Object result = super.invokeMethod(method, args);
      if (block.get()) {
        throw new UnknownHostException("Fake Exception");
      } else {
        return result;
      }
    }
  }
  
  @Before
  public void setup() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BlockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 0);
    conf.set(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_KEY,"1000,2");
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(2))
        .numDataNodes(DataNodes).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem(0);
  }
  
  @After
  public void cleanup() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * 1. Run a set of operations
   * 2. Trigger the NN failover
   * 3. Check the retry cache on the original standby NN
   */
  @Test
  public void testRetryCacheOnStandbyNN() throws Exception {
    // 1. run operations
    DFSTestUtil.runOperations(cluster, dfs, conf, BlockSize, 0);
    
    List<FSNamesystem> fsns = new ArrayList<>();
    List<LightWeightCache<CacheEntry, CacheEntry>> cacheSets = new ArrayList<>();
    // check retry cache in NN1
    fsns.add(cluster.getNamesystem(0));
    fsns.add(cluster.getNamesystem(1));
    cacheSets.add((LightWeightCache<CacheEntry, CacheEntry>) fsns.get(0).getRetryCache().getCacheSet());
    cacheSets.add((LightWeightCache<CacheEntry, CacheEntry>) fsns.get(1).getRetryCache().getCacheSet());
    int usedNN = 0;
    if (cacheSets.get(0).size() < cacheSets.get(1).size()) {
      usedNN = 1;
    }
    assertEquals(11, cacheSets.get(usedNN).size() + cacheSets.get(1-  usedNN).size());
    
    Map<CacheEntry, CacheEntry> oldEntries = new HashMap<CacheEntry, CacheEntry>();
    Iterator<CacheEntry> iter = cacheSets.get(usedNN).iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    iter = cacheSets.get(1 - usedNN).iterator();
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      oldEntries.put(entry, entry);
    }
    
    cluster.shutdownNameNode(usedNN);
    cluster.waitActive(1 - usedNN);

    // 3. check the retry cache on the new active NN
    fillCacheFromDB(oldEntries, fsns.get(1 - usedNN));
    assertEquals(11, cacheSets.get(1 - usedNN).size());
    iter = cacheSets.get(1 - usedNN).iterator();
    
    while (iter.hasNext()) {
      CacheEntry entry = iter.next();
      assertTrue(oldEntries.containsKey(entry));
    }
  }
  
  private void fillCacheFromDB(Map<CacheEntry, CacheEntry> oldEntries, final FSNamesystem namesystem) throws IOException {
    for (final CacheEntry entry : oldEntries.keySet()) {
      HopsTransactionalRequestHandler rh = new HopsTransactionalRequestHandler(HDFSOperationType.CONCAT) {
        @Override
        public void acquireLock(TransactionLocks locks) throws IOException {
          LockFactory lf = getInstance();
          locks.add(lf.getRetryCacheEntryLock(entry.getClientId(), entry.getCallId()));
        }
        
        @Override
        public Object performTask() throws IOException {
          namesystem.getRetryCache().getCacheSet().get(entry);
          return null;
        }
      };
      rh.handle();
      
    }
  }
  
}
