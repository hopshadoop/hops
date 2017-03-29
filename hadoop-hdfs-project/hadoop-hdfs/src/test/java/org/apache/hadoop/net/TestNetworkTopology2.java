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

package org.apache.hadoop.net;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.ipc.Server;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.*;

public class TestNetworkTopology2 {

  @Test
  public void testCreateInvalidTopology() throws Exception {
    final NetworkTopology invalCluster = new NetworkTopology();
    final Random rand = new Random(System.currentTimeMillis());



    class worker implements Callable{
      int id = 0;
      worker(int id){
        this.id = id;
      }
      @Override
      public Object call() throws InterruptedException {
        DatanodeDescriptor d = new DatanodeDescriptor(new DatanodeID("1.1.1.1:"+id));
//        Thread.sleep(rand.nextInt(100));
        invalCluster.add(d);
        return null;
      }
    };


    int threadsCount =20;
    List threads = new ArrayList<worker>();
    for(int i = 0 ; i < threadsCount; i++){
     threads.add(new worker(i));
    }
    ExecutorService es = Executors.newFixedThreadPool(20);
    es.invokeAll(threads);
  }

}
