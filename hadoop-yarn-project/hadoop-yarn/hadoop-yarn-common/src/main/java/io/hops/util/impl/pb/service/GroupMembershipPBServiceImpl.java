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
package io.hops.util.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.hops.util.GroupMembership;
import io.hops.util.GroupMembershipPB;
import io.hops.util.LiveRMsResponse;
import io.hops.util.impl.pb.LiveRMsResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListResponseProto;
import org.apache.hadoop.yarn.proto.GroupMembership.ActiveRMListRequestProto;
import java.io.IOException;

public class GroupMembershipPBServiceImpl implements GroupMembershipPB {

  private GroupMembership real;

  public GroupMembershipPBServiceImpl(GroupMembership impl) {
    this.real = impl;
  }

  @Override
  public ActiveRMListResponseProto getLiveRMList(RpcController controller,
          ActiveRMListRequestProto proto) throws ServiceException {

    try {
      LiveRMsResponse response = real.getLiveRMList();
      return ((LiveRMsResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
