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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.StartContainersRequestProtoOrBuilder;

public class StartContainersRequestPBImpl extends StartContainersRequest {
  StartContainersRequestProto proto = StartContainersRequestProto
    .getDefaultInstance();
  StartContainersRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<StartContainerRequest> requests = null;
  private ByteBuffer keyStore = null;
  private ByteBuffer trustStore = null;

  public StartContainersRequestPBImpl() {
    builder = StartContainersRequestProto.newBuilder();
  }

  public StartContainersRequestPBImpl(StartContainersRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public StartContainersRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (requests != null) {
      addLocalRequestsToProto();
    }
    if (this.keyStore != null) {
      builder.setKeyStore(convertToProtoFormat(this.keyStore));
    }
    if (this.trustStore != null) {
      builder.setTrustStore(convertToProtoFormat(this.trustStore));
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = StartContainersRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addLocalRequestsToProto() {
    maybeInitBuilder();
    builder.clearStartContainerRequest();
    List<StartContainerRequestProto> protoList =
        new ArrayList<StartContainerRequestProto>();
    for (StartContainerRequest r : this.requests) {
      protoList.add(convertToProtoFormat(r));
    }
    builder.addAllStartContainerRequest(protoList);
  }

  private void initLocalRequests() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<StartContainerRequestProto> requestList =
        p.getStartContainerRequestList();
    this.requests = new ArrayList<StartContainerRequest>();
    for (StartContainerRequestProto r : requestList) {
      this.requests.add(convertFromProtoFormat(r));
    }
  }

  @Override
  public void setStartContainerRequests(List<StartContainerRequest> requests) {
    maybeInitBuilder();
    if (requests == null) {
      builder.clearStartContainerRequest();
    }
    this.requests = requests;
  }

  @Override
  public List<StartContainerRequest> getStartContainerRequests() {
    if (this.requests != null) {
      return this.requests;
    }
    initLocalRequests();
    return this.requests;
  }

  @Override
  public ByteBuffer getKeyStore() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    if(this.keyStore != null) {
      return this.keyStore;
    }
    if (!p.hasKeyStore()) {
      return null;
    }
    this.keyStore = convertFromProtoFormat(p.getKeyStore());
    return this.keyStore;
  }
  
  @Override
  public void setKeyStore(ByteBuffer keyStore) {
    maybeInitBuilder();
    if (keyStore == null) {
      builder.clearKeyStore();
    }
    this.keyStore = keyStore;
  }
  
  @Override
  public String getKeyStorePassword() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasKeyStorePassword()) {
      return null;
    }
    return p.getKeyStorePassword();
  }
  
  @Override
  public void setKeyStorePassword(String password) {
    maybeInitBuilder();
    if (password == null) {
      builder.clearKeyStorePassword();
      return;
    }
    builder.setKeyStorePassword(password);
  }
  
  @Override
  public ByteBuffer getTrustStore() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.trustStore != null) {
      return this.trustStore;
    }
    if (!p.hasTrustStore()) {
      return null;
    }
    this.trustStore = convertFromProtoFormat(p.getTrustStore());
    return this.trustStore;
  }
  
  @Override
  public void setTrustStore(ByteBuffer trustStore) {
    maybeInitBuilder();
    if (trustStore == null) {
      builder.clearTrustStore();
    }
    this.trustStore = trustStore;
  }
  
  @Override
  public String getTrustStorePassword() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrustStorePassword()) {
      return null;
    }
    return p.getTrustStorePassword();
  }
  
  @Override
  public void setTrustStorePassword(String password) {
    maybeInitBuilder();
    if (password == null) {
      builder.clearTrustStorePassword();
      return;
    }
    builder.setTrustStorePassword(password);
  }
  
  @Override
  public String getJWT() {
    StartContainersRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasJwt()) {
      return null;
    }
    return p.getJwt();
  }
  
  @Override
  public void setJWT(String jwt) {
    maybeInitBuilder();
    if (jwt == null) {
      builder.clearJwt();
      return;
    }
    builder.setJwt(jwt);
  }
  
  private ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }
  
  private ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }
  
  private StartContainerRequestPBImpl convertFromProtoFormat(
      StartContainerRequestProto p) {
    return new StartContainerRequestPBImpl(p);
  }

  private StartContainerRequestProto convertToProtoFormat(
      StartContainerRequest t) {
    return ((StartContainerRequestPBImpl) t).getProto();
  }
}
