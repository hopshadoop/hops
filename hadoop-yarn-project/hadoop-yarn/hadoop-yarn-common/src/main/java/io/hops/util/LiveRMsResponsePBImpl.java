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
package io.hops.util;

import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

//TODO change it to avoid going through the proto when it is not needed
public class LiveRMsResponsePBImpl extends ProtoBase<YarnGroupMembershipService.ActiveRMListResponseProto>
    implements LiveRMsResponse {
  YarnGroupMembershipService.ActiveRMListResponseProto proto =
      YarnGroupMembershipService.ActiveRMListResponseProto.getDefaultInstance();
  YarnGroupMembershipService.ActiveRMListResponseProto.Builder builder = null;
  boolean viaProto = false;


  public LiveRMsResponsePBImpl() {
    builder = YarnGroupMembershipService.ActiveRMListResponseProto.newBuilder();
  }

  public LiveRMsResponsePBImpl(YarnGroupMembershipService.ActiveRMListResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnGroupMembershipService.ActiveRMListResponseProto getProto() {
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
      builder = YarnGroupMembershipService.ActiveRMListResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public SortedActiveRMList getLiveRMsList() {
    YarnGroupMembershipService.ActiveRMListResponseProtoOrBuilder p = viaProto ? proto : builder;

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
