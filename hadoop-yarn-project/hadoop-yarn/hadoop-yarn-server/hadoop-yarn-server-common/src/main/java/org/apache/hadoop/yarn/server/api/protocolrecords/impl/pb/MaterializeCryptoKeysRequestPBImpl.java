/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysRequest;

import java.nio.ByteBuffer;

public class MaterializeCryptoKeysRequestPBImpl extends
    MaterializeCryptoKeysRequest {
  YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto proto =
      YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto
          .getDefaultInstance();
  YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto.Builder
      builder = null;
  boolean viaProto = false;
  
  public MaterializeCryptoKeysRequestPBImpl() {
    builder = YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto
        .newBuilder();
  }
  
  public MaterializeCryptoKeysRequestPBImpl(YarnServerCommonServiceProtos
      .MaterializeCryptoKeysRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto getProto() {
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
      builder = YarnServerCommonServiceProtos
          .MaterializeCryptoKeysRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public String getUsername() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    
    return (p.hasUsername()) ? p.getUsername() : null;
  }
  
  @Override
  public void setUsername(String username) {
    maybeInitBuilder();
    if (username == null) {
      builder.clearUsername();
      return;
    }
    builder.setUsername(username);
  }
  
  @Override
  public ByteBuffer getKeystore() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    
    if (!p.hasKeystore()) {
      return null;
    }
    
    return ProtoUtils.convertFromProtoFormat(p.getKeystore());
  }
  
  @Override
  public void setKeystore(ByteBuffer keystore) {
    maybeInitBuilder();
    if (keystore == null) {
      builder.clearKeystore();
      return;
    }
    builder.setKeystore(ProtoUtils.convertToProtoFormat(keystore));
  }
  
  @Override
  public String getKeystorePassword() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    
    return (p.hasKeystorePassword()) ? p.getKeystorePassword() : null;
  }
  
  @Override
  public void setKeystorePassword(String password) {
    maybeInitBuilder();
    if (password == null) {
      builder.clearKeystorePassword();
      return;
    }
    builder.setKeystorePassword(password);
  }
  
  @Override
  public ByteBuffer getTruststore() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    
    if (!p.hasTruststore()) {
      return null;
    }
    
    return ProtoUtils.convertFromProtoFormat(p.getTruststore());
  }
  
  @Override
  public void setTruststore(ByteBuffer truststore) {
    maybeInitBuilder();
    if (truststore == null) {
      builder.clearTruststore();
      return;
    }
    builder.setTruststore(ProtoUtils.convertToProtoFormat(truststore));
  }
  
  @Override
  public String getTruststorePassword() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    
    return (p.hasTruststorePassword()) ? p.getTruststorePassword() : null;
  }
  
  @Override
  public void setTruststorePassword(String password) {
    maybeInitBuilder();
    if (password == null) {
      builder.clearTruststorePassword();
      return;
    }
    builder.setTruststorePassword(password);
  }

  @Override
  public String getUserFolder() {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProtoOrBuilder p =
        viaProto ? proto : builder;

    return (p.hasUserFolder()) ? p.getUserFolder() : null;
  }

  @Override
  public void setUserFolder(String userFolder) {
    maybeInitBuilder();
    if (userFolder == null) {
      builder.clearUserFolder();
      return;
    }
    builder.setUserFolder(userFolder);
  }
}
