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
package io.hops.util.impl.pb.client;

import com.google.protobuf.ServiceException;
import io.hops.util.GroupMembership;
import io.hops.util.GroupMembershipPB;
import io.hops.util.LiveRMsResponse;
import io.hops.util.impl.pb.LiveRMsResponsePBImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class GroupMembershipPBClientImpl implements GroupMembership, Closeable {
  private GroupMembershipPB proxy;

  public GroupMembershipPBClientImpl(long clientVersion, InetSocketAddress addr,
      Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, GroupMembershipPB.class,
        ProtobufRpcEngine.class);
    proxy = (GroupMembershipPB) RPC
        .getProxy(GroupMembershipPB.class, clientVersion, addr, conf);
  }

  @Override
  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  @Override
  public LiveRMsResponse getLiveRMList() throws IOException, YarnException {
    ActiveRMListRequestProto requestProto =
        ActiveRMListRequestProto.getDefaultInstance();
    try {
      return new LiveRMsResponsePBImpl(proxy.getLiveRMList(null, requestProto));
    } catch (ServiceException e) {
      RPCUtil.unwrapAndThrowException(e);
      return null;
    }
  }
}
