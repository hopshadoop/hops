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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
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
  private static final int CHECKTIMES = 10;
  private static final int ResponseSize = 3;
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
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 0);
    conf.set(DFSConfigKeys.DFS_CLIENT_RETRY_POLICY_SPEC_KEY,"1000,2");
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_DIRECTIVES_NUM_RESPONSES, ResponseSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_LIST_CACHE_POOLS_NUM_RESPONSES, ResponseSize);
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
  
  abstract class AtMostOnceOp {
    private final String name;
    final DFSClient client;
    
    AtMostOnceOp(String name, DFSClient client) {
      this.name = name;
      this.client = client;
    }
    
    abstract void prepare() throws Exception;
    abstract void invoke() throws Exception;
    abstract boolean checkNamenodeBeforeReturn() throws Exception;
    abstract Object getResult();
  }


  /** addCacheDirective */
  class AddCacheDirectiveInfoOp extends AtMostOnceOp {
    private CacheDirectiveInfo directive;
    private Long result;

    AddCacheDirectiveInfoOp(DFSClient client,
        CacheDirectiveInfo directive) {
      super("addCacheDirective", client);
      this.directive = directive;
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
    }

    @Override
    void invoke() throws Exception {
      result = client.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CacheDirectiveEntry> iter =
            dfs.listCacheDirectives(
                new CacheDirectiveInfo.Builder().
                    setPool(directive.getPool()).
                    setPath(directive.getPath()).
                    build());
        if (iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return result;
    }
  }

  /** modifyCacheDirective */
  class ModifyCacheDirectiveInfoOp extends AtMostOnceOp {
    private final CacheDirectiveInfo directive;
    private final short newReplication;
    private long id;

    ModifyCacheDirectiveInfoOp(DFSClient client,
        CacheDirectiveInfo directive, short newReplication) {
      super("modifyCacheDirective", client);
      this.directive = directive;
      this.newReplication = newReplication;
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      id = client.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    void invoke() throws Exception {
      client.modifyCacheDirective(
          new CacheDirectiveInfo.Builder().
              setId(id).
              setReplication(newReplication).
              build(), EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CacheDirectiveEntry> iter =
            dfs.listCacheDirectives(
                new CacheDirectiveInfo.Builder().
                    setPool(directive.getPool()).
                    setPath(directive.getPath()).
                    build());
        while (iter.hasNext()) {
          CacheDirectiveInfo result = iter.next().getInfo();
          if ((result.getId() == id) &&
              (result.getReplication().shortValue() == newReplication)) {
            return true;
          }
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** removeCacheDirective */
  class RemoveCacheDirectiveInfoOp extends AtMostOnceOp {
    private CacheDirectiveInfo directive;
    private long id;

    RemoveCacheDirectiveInfoOp(DFSClient client, String pool,
        String path) {
      super("removeCacheDirective", client);
      this.directive = new CacheDirectiveInfo.Builder().
          setPool(pool).
          setPath(new Path(path)).
          build();
    }

    @Override
    void prepare() throws Exception {
      dfs.addCachePool(new CachePoolInfo(directive.getPool()));
      id = dfs.addCacheDirective(directive, EnumSet.of(CacheFlag.FORCE));
    }

    @Override
    void invoke() throws Exception {
      client.removeCacheDirective(id);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CacheDirectiveEntry> iter =
            dfs.listCacheDirectives(
                new CacheDirectiveInfo.Builder().
                  setPool(directive.getPool()).
                  setPath(directive.getPath()).
                  build());
        if (!iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** addCachePool */
  class AddCachePoolOp extends AtMostOnceOp {
    private String pool;

    AddCachePoolOp(DFSClient client, String pool) {
      super("addCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
    }

    @Override
    void invoke() throws Exception {
      client.addCachePool(new CachePoolInfo(pool));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
        if (iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** modifyCachePool */
  class ModifyCachePoolOp extends AtMostOnceOp {
    String pool;

    ModifyCachePoolOp(DFSClient client, String pool) {
      super("modifyCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
      client.addCachePool(new CachePoolInfo(pool).setLimit(10l));
    }

    @Override
    void invoke() throws Exception {
      client.modifyCachePool(new CachePoolInfo(pool).setLimit(99l));
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
        if (iter.hasNext() && (long)iter.next().getInfo().getLimit() == 99) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
    }
  }

  /** removeCachePool */
  class RemoveCachePoolOp extends AtMostOnceOp {
    private String pool;

    RemoveCachePoolOp(DFSClient client, String pool) {
      super("removeCachePool", client);
      this.pool = pool;
    }

    @Override
    void prepare() throws Exception {
      client.addCachePool(new CachePoolInfo(pool));
    }

    @Override
    void invoke() throws Exception {
      client.removeCachePool(pool);
    }

    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
        if (!iter.hasNext()) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }

    @Override
    Object getResult() {
      return null;
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
    assertEquals(21, cacheSets.get(usedNN).size() + cacheSets.get(1-  usedNN).size());
    
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
    assertEquals(21, cacheSets.get(1 - usedNN).size());
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
  
  /**
   * Add a list of cache pools, list cache pools,
   * switch active NN, and list cache pools again.
   */
  @Test (timeout=120000)
  public void testListCachePools() throws Exception {
    final int poolCount = 7;
    HashSet<String> poolNames = new HashSet<String>(poolCount);
    for (int i=0; i<poolCount; i++) {
      String poolName = "testListCachePools-" + i;
      dfs.addCachePool(new CachePoolInfo(poolName));
      poolNames.add(poolName);
    }
    listCachePools(poolNames, 0);

    cluster.shutdownNameNode(0);
    cluster.waitActive(1);
    listCachePools(poolNames, 1);
  }

  /**
   * Add a list of cache directives, list cache directives,
   * switch active NN, and list cache directives again.
   */
  @Test (timeout=120000)
  public void testListCacheDirectives() throws Exception {
    final int poolCount = 7;
    HashSet<String> poolNames = new HashSet<String>(poolCount);
    Path path = new Path("/p");
    for (int i=0; i<poolCount; i++) {
      String poolName = "testListCacheDirectives-" + i;
      CacheDirectiveInfo directiveInfo =
        new CacheDirectiveInfo.Builder().setPool(poolName).setPath(path).build();
      dfs.addCachePool(new CachePoolInfo(poolName));
      dfs.addCacheDirective(directiveInfo, EnumSet.of(CacheFlag.FORCE));
      poolNames.add(poolName);
    }
    listCacheDirectives(poolNames, 0);

    cluster.shutdownNameNode(0);
    cluster.waitActive(1);
    listCacheDirectives(poolNames, 1);
  }

  @SuppressWarnings("unchecked")
  private void listCachePools(
      HashSet<String> poolNames, int active) throws Exception {
    HashSet<String> tmpNames = (HashSet<String>)poolNames.clone();
    RemoteIterator<CachePoolEntry> pools = dfs.listCachePools();
    int poolCount = poolNames.size();
    for (int i=0; i<poolCount; i++) {
      CachePoolEntry pool = pools.next();
      String pollName = pool.getInfo().getPoolName();
      assertTrue("The pool name should be expected", tmpNames.remove(pollName));
      if (i % 2 == 0) {
        int standby = active;
        active = (standby == 0) ? 1 : 0;
        cluster.shutdownNameNode(standby);
        cluster.waitActive(active);
        cluster.restartNameNode(standby, false);
      }
    }
    assertTrue("All pools must be found", tmpNames.isEmpty());
  }

  @SuppressWarnings("unchecked")
  private void listCacheDirectives(
      HashSet<String> poolNames, int active) throws Exception {
    HashSet<String> tmpNames = (HashSet<String>)poolNames.clone();
    RemoteIterator<CacheDirectiveEntry> directives = dfs.listCacheDirectives(null);
    int poolCount = poolNames.size();
    for (int i=0; i<poolCount; i++) {
      CacheDirectiveEntry directive = directives.next();
      String pollName = directive.getInfo().getPool();
      assertTrue("The pool name should be expected", tmpNames.remove(pollName));
      if (i % 2 == 0) {
        int standby = active;
        active = (standby == 0) ? 1 : 0;
        cluster.shutdownNameNode(standby);
        cluster.waitActive(active);
        cluster.restartNameNode(standby, false);
      }
    }
    assertTrue("All pools must be found", tmpNames.isEmpty());
  }
  
  /** setXAttr */
  class SetXAttrOp extends AtMostOnceOp {
    private final String src;
    
    SetXAttrOp(DFSClient client, String src) {
      super("setXAttr", client);
      this.src = src;
    }
    
    @Override
    void prepare() throws Exception {
      Path p = new Path(src);
      if (!dfs.exists(p)) {
        DFSTestUtil.createFile(dfs, p, BlockSize, DataNodes, 0);
      }
    }
    
    @Override
    void invoke() throws Exception {
      client.setXAttr(src, "user.key", "value".getBytes(),
          EnumSet.of(XAttrSetFlag.CREATE));
    }
    
    @Override
    boolean checkNamenodeBeforeReturn() throws Exception {
      for (int i = 0; i < CHECKTIMES; i++) {
        Map<String, byte[]> iter = dfs.getXAttrs(new Path(src));
        Set<String> keySet = iter.keySet();
        if (keySet.contains("user.key")) {
          return true;
        }
        Thread.sleep(1000);
      }
      return false;
    }
    
    @Override
    Object getResult() {
      return null;
    }
  }
  
}
