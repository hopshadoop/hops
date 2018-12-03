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

import com.google.protobuf.ByteString;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;

import java.nio.ByteBuffer;

public class UpdatedCryptoForAppPBImpl extends UpdatedCryptoForApp {
  YarnServerCommonServiceProtos.UpdatedCryptoForAppProto proto = YarnServerCommonServiceProtos
      .UpdatedCryptoForAppProto.getDefaultInstance();
  YarnServerCommonServiceProtos.UpdatedCryptoForAppProto.Builder builder = null;
  boolean viaProto = false;
  
  private ByteBuffer keyStore;
  private ByteBuffer trustStore;
  
  public UpdatedCryptoForAppPBImpl() {
    builder = YarnServerCommonServiceProtos.UpdatedCryptoForAppProto.newBuilder();
  }
  
  public UpdatedCryptoForAppPBImpl(YarnServerCommonServiceProtos.UpdatedCryptoForAppProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized YarnServerCommonServiceProtos.UpdatedCryptoForAppProto getProto() {
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
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  private final ByteBuffer convertFromProtoFormat(ByteString byteString) {
    return ProtoUtils.convertFromProtoFormat(byteString);
  }
  
  private final ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
    return ProtoUtils.convertToProtoFormat(byteBuffer);
  }
  
  private synchronized void mergeLocalToBuilder() {
    if (this.keyStore != null) {
      builder.setKeyStore(convertToProtoFormat(this.keyStore));
    }
    if (this.trustStore != null) {
      builder.setTrustStore(convertToProtoFormat(this.trustStore));
    }
  }
  
  private synchronized void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.UpdatedCryptoForAppProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public ByteBuffer getKeyStore() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (this.keyStore != null) {
      return this.keyStore.asReadOnlyBuffer();
    }
    if (!p.hasKeyStore()) {
      return null;
    }
    this.keyStore = convertFromProtoFormat(p.getKeyStore());
    return this.keyStore.asReadOnlyBuffer();
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
  public char[] getKeyStorePassword() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasKeyStorePassword()) {
      return null;
    }
    return p.getKeyStorePassword().toCharArray();
  }
  
  @Override
  public void setKeyStorePassword(char[] keyStorePassword) {
    maybeInitBuilder();
    if (keyStorePassword == null) {
      builder.clearKeyStorePassword();
      return;
    }
    builder.setKeyStorePassword(String.valueOf(keyStorePassword));
  }
  
  @Override
  public ByteBuffer getTrustStore() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (this.trustStore != null) {
      return this.trustStore.asReadOnlyBuffer();
    }
    if (!p.hasTrustStore()) {
      return null;
    }
    this.trustStore = convertFromProtoFormat(p.getTrustStore());
    return this.trustStore.asReadOnlyBuffer();
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
  public char[] getTrustStorePassword() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrustStorePassword()) {
      return null;
    }
    return p.getTrustStorePassword().toCharArray();
  }
  
  @Override
  public void setTrustStorePassword(char[] trustStorePassword) {
    maybeInitBuilder();
    if (trustStorePassword == null) {
      builder.clearTrustStorePassword();
      return;
    }
    builder.setTrustStorePassword(String.valueOf(trustStorePassword));
  }
  
  @Override
  public int getVersion() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasVersion()) {
      return -1;
    }
    return p.getVersion();
  }
  
  @Override
  public void setVersion(int version) {
    maybeInitBuilder();
    if (version == -1) {
      builder.clearVersion();
      return;
    }
    builder.setVersion(version);
  }
  
  @Override
  public String getJWT() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
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
  
  @Override
  public long getJWTExpiration() {
    YarnServerCommonServiceProtos.UpdatedCryptoForAppProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasJwtExpiration()) {
      return -1L;
    }
    return p.getJwtExpiration();
  }
  
  @Override
  public void setJWTExpiration(long jwtExpiration) {
    maybeInitBuilder();
    if (jwtExpiration == -1L) {
      builder.clearJwtExpiration();
      return;
    }
    builder.setJwtExpiration(jwtExpiration);
  }
}
