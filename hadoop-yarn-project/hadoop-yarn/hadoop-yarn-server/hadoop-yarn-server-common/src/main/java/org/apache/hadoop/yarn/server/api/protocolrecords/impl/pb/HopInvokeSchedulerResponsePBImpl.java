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
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RMNodeMsgResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RMNodeMsgResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.HopInvokeSchedulerResponse;

public class HopInvokeSchedulerResponsePBImpl
    extends ProtoBase<YarnServerCommonServiceProtos.RMNodeMsgResponseProto>
    implements HopInvokeSchedulerResponse {

  RMNodeMsgResponseProto proto = RMNodeMsgResponseProto.getDefaultInstance();
  RMNodeMsgResponseProto.Builder builder = null;
  private int result = 0;
  boolean viaProto = false;

  public HopInvokeSchedulerResponsePBImpl() {
    builder = RMNodeMsgResponseProto.newBuilder();
  }

  public HopInvokeSchedulerResponsePBImpl(RMNodeMsgResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public YarnServerCommonServiceProtos.RMNodeMsgResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.result != 0) {
      builder.setResult(this.result);
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
      builder = RMNodeMsgResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getResult() {
    RMNodeMsgResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.result != 0) {
      return this.result;
    }
    if (!p.hasResult()) {
      return 0;
    }

    return p.getResult();
  }

  @Override
  public void setResult(int result) {
    maybeInitBuilder();
    builder.setResult(result);
  }
}