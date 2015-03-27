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
package org.apache.hadoop.yarn.server.api.impl.pb.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.server.api.HopInvokeSchedulerPB;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HopInvokeSchedulerPBClientImpl {

  private HopInvokeSchedulerPB proxy;

  public void close() {
    if (this.proxy != null) {
      RPC.stopProxy(this.proxy);
    }
  }

  public HopInvokeSchedulerPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, HopInvokeSchedulerPB.class,
        ProtobufRpcEngine.class);
    proxy = (HopInvokeSchedulerPB) RPC
        .getProxy(HopInvokeSchedulerPB.class, clientVersion, addr, conf);
  }
}
