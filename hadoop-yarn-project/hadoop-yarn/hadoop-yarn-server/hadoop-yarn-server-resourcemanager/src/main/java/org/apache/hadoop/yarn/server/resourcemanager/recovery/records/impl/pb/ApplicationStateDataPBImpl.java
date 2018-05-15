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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.RMAppStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

public class ApplicationStateDataPBImpl extends ApplicationStateData {
  ApplicationStateDataProto proto = 
            ApplicationStateDataProto.getDefaultInstance();
  ApplicationStateDataProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationSubmissionContext applicationSubmissionContext = null;
  private byte[] keyStore;
  private byte[] trustStore;
  
  public ApplicationStateDataPBImpl() {
    builder = ApplicationStateDataProto.newBuilder();
  }

  public ApplicationStateDataPBImpl(
      ApplicationStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationStateDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.applicationSubmissionContext != null) {
      builder.setApplicationSubmissionContext(
          ((ApplicationSubmissionContextPBImpl)applicationSubmissionContext)
          .getProto());
    }
    if (this.keyStore != null) {
      builder.setKeyStore(ByteString.copyFrom(this.keyStore));
    }
    if (this.trustStore != null) {
      builder.setTrustStore(ByteString.copyFrom(this.trustStore));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getSubmitTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubmitTime()) {
      return -1;
    }
    return (p.getSubmitTime());
  }

  @Override
  public void setSubmitTime(long submitTime) {
    maybeInitBuilder();
    builder.setSubmitTime(submitTime);
  }

  @Override
  public long getStartTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public String getUser() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return (p.getUser());

  }
  
  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    builder.setUser(user);
  }
  
  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if(applicationSubmissionContext != null) {
      return applicationSubmissionContext;
    }
    if (!p.hasApplicationSubmissionContext()) {
      return null;
    }
    applicationSubmissionContext = 
        new ApplicationSubmissionContextPBImpl(
            p.getApplicationSubmissionContext());
    return applicationSubmissionContext;
  }

  @Override
  public void setApplicationSubmissionContext(
      ApplicationSubmissionContext context) {
    maybeInitBuilder();
    if (context == null) {
      builder.clearApplicationSubmissionContext();
    }
    this.applicationSubmissionContext = context;
  }

  @Override
  public RMAppState getState() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationState()) {
      return null;
    }
    return convertFromProtoFormat(p.getApplicationState());
  }

  @Override
  public void setState(RMAppState finalState) {
    maybeInitBuilder();
    if (finalState == null) {
      builder.clearApplicationState();
      return;
    }
    builder.setApplicationState(convertToProtoFormat(finalState));
  }

  @Override
  public String getDiagnostics() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public long getFinishTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
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
  
  @Override
  public CallerContext getCallerContext() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    RpcHeaderProtos.RPCCallerContextProto pbContext = p.getCallerContext();
    if (pbContext != null) {
      CallerContext context = new CallerContext.Builder(pbContext.getContext())
          .setSignature(pbContext.getSignature().toByteArray()).build();
      return context;
    }

    return null;
  }

  @Override
  public void setCallerContext(CallerContext callerContext) {
    if (callerContext != null) {
      maybeInitBuilder();

      RpcHeaderProtos.RPCCallerContextProto.Builder b = RpcHeaderProtos.RPCCallerContextProto
          .newBuilder();
      if (callerContext.getContext() != null) {
        b.setContext(callerContext.getContext());
      }
      if (callerContext.getSignature() != null) {
        b.setSignature(ByteString.copyFrom(callerContext.getSignature()));
      }

      builder.setCallerContext(b);
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
  
  @Override
  public byte[] getKeyStore() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.keyStore != null) {
      return this.keyStore;
    }
    if (!p.hasKeyStore()) {
      return null;
    }
    this.keyStore = p.getKeyStore().toByteArray();
    return this.keyStore;
  }
  
  @Override
  public void setKeyStore(byte[] keyStore) {
    maybeInitBuilder();
    if (keyStore == null) {
      builder.clearKeyStore();
      return;
    }
    this.keyStore = keyStore;
  }
  
  @Override
  public char[] getKeyStorePassword() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public byte[] getTrustStore() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (this.trustStore != null) {
      return this.trustStore;
    }
    if (!p.hasTrustStore()) {
      return null;
    }
    this.trustStore = p.getTrustStore().toByteArray();
    return this.trustStore;
  }
  
  @Override
  public void setTrustStore(byte[] trustStore) {
    maybeInitBuilder();
    if (trustStore == null) {
      builder.clearTrustStore();
      return;
    }
    this.trustStore = trustStore;
  }
  
  @Override
  public char[] getTrustStorePassword() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public Integer getCryptoMaterialVersion() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCryptoMaterialVersion();
  }
  
  @Override
  public void setCryptoMaterialVersion(Integer cryptoMaterialVersion) {
    maybeInitBuilder();
    builder.setCryptoMaterialVersion(cryptoMaterialVersion);
  }
  
  @Override
  public long getCertificateExpiration() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCertificateExpiration();
  }
  
  @Override
  public void setCertificateExpiration(long certificateExpiration) {
    maybeInitBuilder();
    builder.setCertificateExpiration(certificateExpiration);
  }
  
  @Override
  public boolean isDuringMaterialRotation() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getIsDuringMaterialRotation();
  }
  
  @Override
  public void setIsDuringMaterialRotation(boolean isDuringMaterialRotation) {
    maybeInitBuilder();
    builder.setIsDuringMaterialRotation(isDuringMaterialRotation);
  }
  
  @Override
  public long getMaterialRotationStartTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMaterialRotationStart()) {
      return -1L;
    }
    return p.getMaterialRotationStart();
  }
  
  @Override
  public void setMaterialRotationStartTime(long materialRotationStartTime) {
    maybeInitBuilder();
    builder.setMaterialRotationStart(materialRotationStartTime);
  }
  
  private static String RM_APP_PREFIX = "RMAPP_";
  public static RMAppStateProto convertToProtoFormat(RMAppState e) {
    return RMAppStateProto.valueOf(RM_APP_PREFIX + e.name());
  }
  public static RMAppState convertFromProtoFormat(RMAppStateProto e) {
    return RMAppState.valueOf(e.name().replace(RM_APP_PREFIX, ""));
  }
}
