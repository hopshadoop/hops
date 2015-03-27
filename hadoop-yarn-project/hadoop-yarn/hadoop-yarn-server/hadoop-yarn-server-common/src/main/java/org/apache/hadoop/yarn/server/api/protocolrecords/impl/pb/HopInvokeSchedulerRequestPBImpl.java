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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RMNodeMsgRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RMNodeMsgRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.HopInvokeSchedulerRequest;

public class HopInvokeSchedulerRequestPBImpl
    extends ProtoBase<RMNodeMsgRequestProto>
    implements HopInvokeSchedulerRequest {

  RMNodeMsgRequestProto proto = RMNodeMsgRequestProto.getDefaultInstance();
  RMNodeMsgRequestProto.Builder builder = null;
  boolean viaProto = false;
  private int id = Integer.MIN_VALUE;
  private int type = Integer.MIN_VALUE;

  public HopInvokeSchedulerRequestPBImpl() {
    builder = RMNodeMsgRequestProto.newBuilder();
  }

  public HopInvokeSchedulerRequestPBImpl(RMNodeMsgRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public RMNodeMsgRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.id != Integer.MIN_VALUE) {
      builder.setId(this.id);
    }
    if (this.type != Integer.MIN_VALUE) {
      builder.setType(this.type);
    }
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
      builder = RMNodeMsgRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getRMNodeID() {
    RMNodeMsgRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.id != Integer.MIN_VALUE) {
      return this.id;
    }
    if (!p.hasId()) {
      return Integer.MIN_VALUE;
    }

    return p.getId();
  }

  @Override
  public int getType() {
    RMNodeMsgRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.type != Integer.MIN_VALUE) {
      return this.type;
    }
    if (!p.hasType()) {
      return Integer.MIN_VALUE;
    }

    return p.getType();
  }

  @Override
  public void setRMNodeID(int id) {
    maybeInitBuilder();
    builder.setId(id);
  }

  @Override
  public void setType(int type) {
    maybeInitBuilder();
    builder.setType(type);
  }
}