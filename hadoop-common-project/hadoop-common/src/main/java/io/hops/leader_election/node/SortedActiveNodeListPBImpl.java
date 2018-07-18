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
package io.hops.leader_election.node;

import io.hops.leader_election.proto.ActiveNodeProtos.ActiveNodeProto;
import io.hops.leader_election.proto.ActiveNodeProtos.SortedActiveNodeListProto;
import io.hops.leader_election.proto.ActiveNodeProtos.SortedActiveNodeListProtoOrBuilder;

import java.net.InetSocketAddress;
import java.util.*;

//TODO change it to avoid going through the proto when it is not needed
public class SortedActiveNodeListPBImpl implements SortedActiveNodeList {

  SortedActiveNodeListProto proto =
      SortedActiveNodeListProto.getDefaultInstance();
  SortedActiveNodeListProto.Builder builder = null;
  boolean viaProto = false;
  Random random = new Random();

  public SortedActiveNodeListPBImpl(SortedActiveNodeListProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public SortedActiveNodeListPBImpl(List<ActiveNode> listActiveNodes) {
    if (listActiveNodes == null) {
      throw new NullPointerException("List of active namenodes was null");
    }
    maybeInitBuilder();
    for (ActiveNode node : listActiveNodes) {
      builder.addActiveNode(((ActiveNodePBImpl) node).getProto());
    }
  }

  public SortedActiveNodeListProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SortedActiveNodeListProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int size() {
    SortedActiveNodeListProtoOrBuilder p = viaProto ? proto : builder;
    return p.getActiveNodeCount();
  }

  @Override
  public boolean isEmpty() {
    SortedActiveNodeListProtoOrBuilder p = viaProto ? proto : builder;
    return p.getActiveNodeCount() == 0;
  }

  @Override
  public List<ActiveNode> getSortedActiveNodes() {
    SortedActiveNodeListProtoOrBuilder p = viaProto ? proto : builder;
    List<ActiveNode> activeNodes = new ArrayList<ActiveNode>();
    for (ActiveNodeProto nodeProto : p.getActiveNodeList()) {
      activeNodes.add(new ActiveNodePBImpl(nodeProto));
    }
    Collections.sort(activeNodes);
    return activeNodes;
  }

  @Override
  public List<ActiveNode> getActiveNodes() {
    SortedActiveNodeListProtoOrBuilder p = viaProto ? proto : builder;
    List<ActiveNode> activeNodes = new ArrayList<ActiveNode>();
    for (ActiveNodeProto nodeProto : p.getActiveNodeList()) {
      activeNodes.add(new ActiveNodePBImpl(nodeProto));
    }
    return activeNodes;
  }

  @Override
  public ActiveNode getActiveNode(InetSocketAddress address) {
    SortedActiveNodeListProtoOrBuilder p = viaProto ? proto : builder;
    for (ActiveNodeProto namenodeProto : p.getActiveNodeList()) {
      ActiveNode namenode = new ActiveNodePBImpl(namenodeProto);
      if (namenode.getRpcServerAddressForClients().equals(address) ||
              namenode.getRpcServerAddressForDatanodes().equals(address)) {
        return namenode;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    if (this.isEmpty()) {
      return "No Active NameNodes";
    } else {
      return Arrays.toString(this.getSortedActiveNodes().toArray());
    }
  }
  @Override
  public ActiveNode getLeader() {
    //in our case the node wiht smallest id is the leader
    if (this.isEmpty()) {
      return null;
    }
    return this.getSortedActiveNodes().get(0);
  }
}
