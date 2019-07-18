/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;

import com.google.protobuf.TextFormat;

/**
 * Protocol buffer based implementation of
 * {@link UpdateApplicationHomeSubClusterRequest} .
 */
@Private
@Unstable
public class UpdateApplicationHomeSubClusterRequestPBImpl
    extends UpdateApplicationHomeSubClusterRequest {

  private UpdateApplicationHomeSubClusterRequestProto proto =
      UpdateApplicationHomeSubClusterRequestProto.getDefaultInstance();
  private UpdateApplicationHomeSubClusterRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public UpdateApplicationHomeSubClusterRequestPBImpl() {
    builder = UpdateApplicationHomeSubClusterRequestProto.newBuilder();
  }

  public UpdateApplicationHomeSubClusterRequestPBImpl(
      UpdateApplicationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateApplicationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateApplicationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public ApplicationHomeSubCluster getApplicationHomeSubCluster() {
    UpdateApplicationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  public void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationInfo) {
    maybeInitBuilder();
    if (applicationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    builder.setAppSubclusterMap(convertToProtoFormat(applicationInfo));
  }

  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}
