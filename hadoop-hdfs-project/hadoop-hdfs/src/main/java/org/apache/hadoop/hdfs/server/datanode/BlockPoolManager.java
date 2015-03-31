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
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Log LOG = DataNode.LOG;
  
  private final Map<String, BPOfferService> bpByNameserviceId =
    Maps.newHashMap();
  private final Map<String, BPOfferService> bpByBlockPoolId =
    Maps.newHashMap();
  private final List<BPOfferService> offerServices =
      new CopyOnWriteArrayList<>();

  private final DataNode dn;

  //This lock is used only to ensure exclusion of refreshNamenodes
  private final Object refreshNamenodesLock = new Object();
  
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions
        .checkArgument(offerServices.contains(bpos), "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns a list of BPOfferService objects. The underlying list
   * implementation is a CopyOnWriteArrayList so it can be safely
   * iterated while BPOfferServices are being added or removed.
   *
   * Caution: The BPOfferService returned could be shutdown any time.
   */
  synchronized List<BPOfferService> getAllNamenodeThreads() {
    return Collections.unmodifiableList(offerServices);
  }

  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  synchronized void remove(BPOfferService t) {
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // It's possible that the block pool never successfully registered
      // with any NN, so it was never added it to this map
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed; ) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  void shutDownAll(List<BPOfferService> bposList) throws InterruptedException {
    for (BPOfferService bpos : bposList) {
      bpos.stop(); //interrupts the threads
    }
    //now join
    for (BPOfferService bpos : bposList) {
      bpos.join();
    }
  }
  
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser()
          .doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                  for (BPOfferService bpos : offerServices) {
                    bpos.start();
                  }
                  return null;
                }
              });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  void joinAll() {
    for (BPOfferService bpos : this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }
  
  void refreshNamenodes(Configuration conf) throws IOException {
    synchronized (refreshNamenodesLock) {
//      List<InetSocketAddress> namenodes = DFSUtil.getNameNodesRPCAddresses(conf);
      List<InetSocketAddress> namenodes = DFSUtil.getNameNodesServiceRpcAddresses(conf);
      offerServices.add(createBPOS(namenodes));
      startAll();
    }
  }

  /**
   * Extracted out for test purposes.
   */
  protected BPOfferService createBPOS(List<InetSocketAddress> nnAddrs) {
    return new BPOfferService(nnAddrs, dn);
  }
}