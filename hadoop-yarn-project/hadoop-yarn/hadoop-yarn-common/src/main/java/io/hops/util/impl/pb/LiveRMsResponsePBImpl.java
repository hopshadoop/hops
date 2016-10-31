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
package io.hops.util.impl.pb;

import io.hops.util.ActiveRM;
import io.hops.util.LiveRMsResponse;
import io.hops.util.SortedActiveRMList;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListResponseProto;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListRequestProto;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListResponseProtoOrBuilder;

//TODO change it to avoid going through the proto when it is not needed
public class LiveRMsResponsePBImpl extends ProtoBase<ActiveRMListResponseProto>
    implements LiveRMsResponse {
  ActiveRMListResponseProto proto =
      ActiveRMListResponseProto.getDefaultInstance();
  ActiveRMListResponseProto.Builder builder = null;
  boolean viaProto = false;


  public LiveRMsResponsePBImpl() {
    builder = ActiveRMListResponseProto.newBuilder();
  }

  public LiveRMsResponsePBImpl(ActiveRMListResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ActiveRMListResponseProto getProto() {
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
      builder = ActiveRMListResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public SortedActiveRMList getLiveRMsList() {
    ActiveRMListResponseProtoOrBuilder p = viaProto ? proto : builder;

    return new SortedActiveRMList(p.getActiveRmsList());
  }

  @Override
  public ActiveRM getLeader() {
    return (ActiveRM) getLiveRMsList().getLeader();
  }

  @Override
  public void setLiveRMsList(SortedActiveRMList list) {
    maybeInitBuilder();

    builder.setActiveRmsList(((SortedActiveRMList) list).getProto());
  }

}
