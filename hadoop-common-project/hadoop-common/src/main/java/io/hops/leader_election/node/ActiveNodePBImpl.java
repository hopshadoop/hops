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
import io.hops.leader_election.proto.ActiveNodeProtos.ActiveNodeProtoOrBuilder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

//TODO change it to avoid going through the proto when it is not needed
public class ActiveNodePBImpl implements ActiveNode {

  protected ActiveNodeProto proto = ActiveNodeProto.getDefaultInstance();
  protected ActiveNodeProto.Builder builder = null;
  protected boolean viaProto = false;

  public ActiveNodePBImpl(ActiveNodeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ActiveNodePBImpl(long id, String hostname, String ipAddress, int port,
      String httpAddress) {
    maybeInitBuilder();
    builder.setId(id);
    builder.setHostname(hostname);
    builder.setIpAddress(ipAddress);
    builder.setPort(port);
    builder.setHttpAddress(httpAddress);
  }

  public ActiveNodeProto getProto() {
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

  protected void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ActiveNodeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getHostname() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHostname();
  }

  public void setHostname(String hostname) {
    maybeInitBuilder();
    builder.setHostname(hostname);
  }

  @Override
  public long getId() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getId();
  }

  public void setId(long id) {
    maybeInitBuilder();
    builder.setId(id);
  }

  @Override
  public String getIpAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIpAddress();
  }

  public void setIpAddress(String ipAddress) {
    maybeInitBuilder();
    builder.setIpAddress(ipAddress);
  }

  @Override
  public int getPort() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getPort();
  }

  public void setPort(int port) {
    maybeInitBuilder();
    builder.setPort(port);
  }

  @Override
  public String getHttpAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHttpAddress();
  }

  public void setHttpAddress(String httpAddress) {
    maybeInitBuilder();
    builder.setHttpAddress(httpAddress);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return createSocketAddrForHost(p.getIpAddress(), p.getPort());
  }

  @Override
  public boolean equals(Object obj) {
    // objects are equal if the belong to same NN
    // namenode id is not taken in to account
    // sometimes the id of the namenode may change even without 
    //namenode restart
    if (!(obj instanceof ActiveNode)) {
      return false;
    }
    ActiveNode that = (ActiveNode) obj;
    return this.getInetSocketAddress().equals(that.getInetSocketAddress());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + this.getInetSocketAddress().hashCode();
    return hash;
  }

  @Override
  public int compareTo(ActiveNode o) {

    if (this.getId() < o.getId()) {
      return -1;
    } else if (this.getId() == o.getId()) {
      return 0;
    } else if (this.getId() > o.getId()) {
      return 1;
    } else {
      throw new IllegalStateException("I write horrible code");
    }
  }

  @Override
  public String toString() {
    return "Active NN (" + this.getId() + ") address " + getInetSocketAddress();
  }

  public static InetSocketAddress createSocketAddrForHost(String host,
      int port) {
    InetSocketAddress addr;
    try {
      InetAddress iaddr = InetAddress.getByName(host);
      // if there is a static entry for the host, make the returned
      // address look like the original given host
      if (host != null) {
        iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
      }
      addr = new InetSocketAddress(iaddr, port);
    } catch (UnknownHostException e) {
      addr = InetSocketAddress.createUnresolved(host, port);
    }
    return addr;
  }
}
