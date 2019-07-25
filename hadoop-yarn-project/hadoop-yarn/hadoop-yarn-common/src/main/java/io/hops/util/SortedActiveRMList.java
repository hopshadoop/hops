/*
 * Copyright (C) 2015 hops.io.
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
package io.hops.util;

import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.leader_election.proto.ActiveNodeProtos;
import io.hops.util.impl.ActiveRMPBImpl;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class SortedActiveRMList {

  private SortedActiveNodeList activeNodes;
  Random random = new Random();

  public SortedActiveRMList(ActiveNodeProtos.SortedActiveNodeListProto proto) {
    activeNodes = new SortedActiveNodeListPBImpl(proto);
  }

  public SortedActiveRMList(List<ActiveNode> listActiveRM) {
    if (listActiveRM == null) {
      throw new NullPointerException("List of active namenodes was null");
    }
    activeNodes = new SortedActiveNodeListPBImpl(listActiveRM);
  }
  
  public ActiveNodeProtos.SortedActiveNodeListProto getProto() {
    return ((SortedActiveNodeListPBImpl) activeNodes).getProto();
  }

  public List<ActiveNode> getActiveNodes() {
    return activeNodes.getActiveNodes();
  }

  public ActiveNode getActiveNode(InetSocketAddress address) {
    return activeNodes.getActiveNode(address);
  }

  public ActiveNode getLeader() {
    return activeNodes.getLeader();
  }

  public boolean isEmpty() {
    return activeNodes.isEmpty();
  }
}
