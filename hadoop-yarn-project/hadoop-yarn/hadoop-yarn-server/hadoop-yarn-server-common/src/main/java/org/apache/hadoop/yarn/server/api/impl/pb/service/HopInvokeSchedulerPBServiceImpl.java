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
package org.apache.hadoop.yarn.server.api.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.HopInvokeScheduler;
import org.apache.hadoop.yarn.server.api.protocolrecords.HopInvokeSchedulerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.HopInvokeSchedulerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.HopInvokeSchedulerResponsePBImpl;

import java.io.IOException;

public class HopInvokeSchedulerPBServiceImpl {

  private HopInvokeScheduler real;

  public HopInvokeSchedulerPBServiceImpl(HopInvokeScheduler impl) {
    this.real = impl;
  }

  //@Override
  public YarnServerCommonServiceProtos.RMNodeMsgResponseProto hopinvokescheduler(
      RpcController controller,
      YarnServerCommonServiceProtos.RMNodeMsgRequestProto proto)
      throws ServiceException {
    HopInvokeSchedulerRequestPBImpl request =
        new HopInvokeSchedulerRequestPBImpl(proto);
    try {
      HopInvokeSchedulerResponse response =
          real.hopInvokeSchedulerRequest(request);
      return ((HopInvokeSchedulerResponsePBImpl) response).getProto();
    } catch (YarnException e) {
      throw new ServiceException(e);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
