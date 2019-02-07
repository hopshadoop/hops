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
import io.hops.metadata.election.entity.LeDescriptor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

//TODO change it to avoid going through the proto when it is not needed
public class ActiveNodePBImpl implements ActiveNode {

  protected ActiveNodeProto proto = ActiveNodeProto.getDefaultInstance();
  protected ActiveNodeProto.Builder builder = null;
  protected boolean viaProto = false;

//  public ActiveNodePBImpl(ActiveNodeProto proto) {
//    this.proto = proto;
//    viaProto = true;
//  }

  public ActiveNodePBImpl(ActiveNodeProto proto){
    this(proto.getId(),
            proto.getRpcHostname(),
            proto.getRpcIpAddress(),
            proto.getRpcPort(),
            proto.getHttpAddress(),
            proto.getServiceIpAddress(),
            proto.getServicePort(),
            proto.getLocationDomainId());
  }
  
  public ActiveNodePBImpl(long id, String hostname, String ipAddress, int port,
      String httpAddress, String serviceRpcIp, int serviceRpcPort) {
    this(id, hostname, ipAddress, port, httpAddress, serviceRpcIp,
        serviceRpcPort, LeDescriptor.DEFAULT_LOCATION_DOMAIN_ID);
  }
  
  public ActiveNodePBImpl(long id, String hostname, String ipAddress, int port,
      String httpAddress, String serviceRpcIp, int serviceRpcPort,
      int locationDomainId) {
    maybeInitBuilder();
    builder.setId(id);
    builder.setRpcHostname(hostname);
    builder.setRpcIpAddress(ipAddress);
    builder.setRpcPort(port);
    builder.setHttpAddress(httpAddress);
    builder.setServiceIpAddress(serviceRpcIp);
    builder.setServicePort(serviceRpcPort);
    builder.setLocationDomainId(locationDomainId);
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
    return p.getRpcHostname();
  }

  public void setHostname(String hostname) {
    maybeInitBuilder();
    builder.setRpcHostname(hostname);
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
  public String getRpcServerIpAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcIpAddress();
  }

  public void setIpAddress(String ipAddress) {
    maybeInitBuilder();
    builder.setRpcIpAddress(ipAddress);
  }

  @Override
  public int getRpcServerPort() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  public void setPort(int port) {
    maybeInitBuilder();
    builder.setRpcPort(port);
  }

  @Override
  public String getHttpAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHttpAddress();
  }
  
  @Override
  public int getLocationDomainId() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLocationDomainId();
  }
  
  public void setHttpAddress(String httpAddress) {
    maybeInitBuilder();
    builder.setHttpAddress(httpAddress);
  }

  @Override
  public String getServiceRpcIpAddress() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getServiceIpAddress();
  }

  public void setServiceRpcIpAddress(String serviceRpcIpAddress) {
    maybeInitBuilder();
    builder.setServiceIpAddress(serviceRpcIpAddress);
  }

  @Override
  public int getServiceRpcPort() {
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    return p.getServicePort();
  }

  public void setServiceRpcPort(int port) {
    maybeInitBuilder();
    builder.setServicePort(port);
  }

  InetSocketAddress rpcAddressForDatanodes = null;
  @Override
  public InetSocketAddress getRpcServerAddressForDatanodes() {
    if(rpcAddressForDatanodes != null){
      return rpcAddressForDatanodes;
    }
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    if( p.getServicePort() != 0 && p.getServiceIpAddress() != "") {
        rpcAddressForDatanodes = createSocketAddrForHost(p.getServiceIpAddress(), p.getServicePort());
    } else {
      //fallback
      rpcAddressForDatanodes = getRpcServerAddressForClients();
    }
    return rpcAddressForDatanodes;
  }

  InetSocketAddress rpcAddressForClients = null;
  @Override
  public InetSocketAddress getRpcServerAddressForClients() {
    if(rpcAddressForClients != null){
      return rpcAddressForClients;
    }
    ActiveNodeProtoOrBuilder p = viaProto ? proto : builder;
    if(p.getRpcPort() != 0 && p.getRpcIpAddress() != "") {
      rpcAddressForClients = createSocketAddrForHost(p.getRpcIpAddress(), p.getRpcPort());
    }else{
      return null;
    }
    return rpcAddressForClients;
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
    return this.getRpcServerAddressForClients().equals(that.getRpcServerAddressForClients());
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + this.getRpcServerAddressForClients().hashCode();
    return hash;
  }

  @Override
  public int compareTo(ActiveNode o) {

    if (this.getId() < o.getId()) {
      return -1;
    } else if (this.getId() == o.getId()) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public String toString() {
    String msg = "Active NN (" + this.getId() + ") Client's RPC: " + getRpcServerAddressForClients()+ " DataNode's RPC: "+ getRpcServerAddressForDatanodes();
    return msg;
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
