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
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.List;

/**
 * This class is used to specify the setup of namenodes when instantiating
 * a MiniDFSCluster. It consists of a set of namenodes.
 */
@InterfaceAudience.LimitedPrivate(
    {"HBase", "HDFS", "Hive", "MapReduce", "Pig", "HOPS"})
@InterfaceStability.Unstable
public class MiniDFSNNTopology {

  private final List<NNConf> namenodes = Lists.newArrayList();

  public MiniDFSNNTopology() {
  }

  public static MiniDFSNNTopology simpleSingleNN(int nameNodePort,
      int nameNodeHttpPort) {
    return new MiniDFSNNTopology().addNameNode(
        new MiniDFSNNTopology.NNConf(null).setHttpPort(nameNodeHttpPort)
            .setIpcPort(nameNodePort));
  }

  public MiniDFSNNTopology addNameNode(NNConf namenode) {
    this.namenodes.add(namenode);
    return this;
  }

  public int countNameNodes() {
    return namenodes.size();
  }
  
  public NNConf getOnlyNameNode() {
    Preconditions
        .checkState(countNameNodes() == 1, "must have exactly one NN!");
    return namenodes.get(0);
  }

  /**
   * @return true if all of the NNs in the cluster have their HTTP
   * port specified to be non-ephemeral.
   */
  public boolean allHttpPortsSpecified() {
    for (NNConf nn : namenodes) {
      if (nn.getHttpPort() == 0) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * @return true if all of the NNs in the cluster have their IPC
   * port specified to be non-ephemeral.
   */
  public boolean allIpcPortsSpecified() {
    for (NNConf nn : namenodes) {
      if (nn.getIpcPort() == 0) {
        return false;
      }
    }
    return true;
  }

  public List<NNConf> getNamenodes() {
    return namenodes;
  }

  public static class NNConf {
    private String nnId;
    private int httpPort;
    private int ipcPort;
    private String clusterId;

    public NNConf(String nnId) {
      this.nnId = nnId;
    }

    String getNnId() {
      return nnId;
    }

    int getIpcPort() {
      return ipcPort;
    }
    
    int getHttpPort() {
      return httpPort;
    }

    String getClusterId() {
      return clusterId;
    }

    public NNConf setHttpPort(int httpPort) {
      this.httpPort = httpPort;
      return this;
    }

    public NNConf setIpcPort(int ipcPort) {
      this.ipcPort = ipcPort;
      return this;
    }
    
    public NNConf setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }
  }

  /**
   * Set up an HOPS Topology
   */
  public static MiniDFSNNTopology simpleHOPSTopology(int nnCount) {
    if (nnCount < 1) {
      return null;
    }
    MiniDFSNNTopology topology = new MiniDFSNNTopology();
    for (int i = 1; i <= nnCount; i++) {
      topology.addNameNode(new MiniDFSNNTopology.NNConf("nn" + i));
    }
    return topology;
  }
}
