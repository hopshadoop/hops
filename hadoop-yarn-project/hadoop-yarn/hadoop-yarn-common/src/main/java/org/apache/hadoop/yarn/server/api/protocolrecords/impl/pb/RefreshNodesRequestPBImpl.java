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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.DecommissionType;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.DecommissionTypeProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshNodesRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class RefreshNodesRequestPBImpl extends RefreshNodesRequest {
  RefreshNodesRequestProto proto = RefreshNodesRequestProto.getDefaultInstance();
  RefreshNodesRequestProto.Builder builder = null;
  boolean viaProto = false;
  private DecommissionType decommissionType;

  public RefreshNodesRequestPBImpl() {
    builder = RefreshNodesRequestProto.newBuilder();
  }

  public RefreshNodesRequestPBImpl(RefreshNodesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized RefreshNodesRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.decommissionType != null) {
      builder.setDecommissionType(convertToProtoFormat(this.decommissionType));
    }
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RefreshNodesRequestProto.newBuilder(proto);
    }
    viaProto = false;
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
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public synchronized void setDecommissionType(
      DecommissionType decommissionType) {
    maybeInitBuilder();
    this.decommissionType = decommissionType;
    mergeLocalToBuilder();
  }

  @Override
  public synchronized DecommissionType getDecommissionType() {
    RefreshNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getDecommissionType());
  }

  @Override
  public synchronized void setDecommissionTimeout(Integer timeout) {
    maybeInitBuilder();
    if (timeout != null) {
      builder.setDecommissionTimeout(timeout);
    } else {
      builder.clearDecommissionTimeout();
    }
  }

  @Override
  public synchronized Integer getDecommissionTimeout() {
    RefreshNodesRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasDecommissionTimeout()? p.getDecommissionTimeout() : null;
  }

  private DecommissionType convertFromProtoFormat(DecommissionTypeProto p) {
    return DecommissionType.valueOf(p.name());
  }

  private DecommissionTypeProto convertToProtoFormat(DecommissionType t) {
    return DecommissionTypeProto.valueOf(t.name());
  }
}
