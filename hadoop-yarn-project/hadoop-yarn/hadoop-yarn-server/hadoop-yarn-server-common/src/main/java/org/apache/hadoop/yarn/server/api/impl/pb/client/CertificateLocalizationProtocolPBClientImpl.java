/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocol;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.MaterializeCryptoKeysRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.MaterializeCryptoKeysResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveCryptoKeysRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveCryptoKeysResponsePBImpl;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public class CertificateLocalizationProtocolPBClientImpl implements
    CertificateLocalizationProtocol, Closeable {
  
  private CertificateLocalizationProtocolPB proxy;
  
  public CertificateLocalizationProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, CertificateLocalizationProtocolPB.class,
        ProtobufRpcEngine.class);
    proxy = (CertificateLocalizationProtocolPB) RPC.getProxy
        (CertificateLocalizationProtocolPB.class, clientVersion, addr, conf);
  }
  
  @Override
  public void close() throws IOException {
    if (null != this.proxy) {
      RPC.stopProxy(this.proxy);
    }
  }
  
  @Override
  public MaterializeCryptoKeysResponse materializeCrypto(
      MaterializeCryptoKeysRequest request) throws YarnException, IOException {
    YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto requestProto =
        ((MaterializeCryptoKeysRequestPBImpl) request).getProto();
    try {
      return new MaterializeCryptoKeysResponsePBImpl(proxy.materializeCrypto
          (null, requestProto));
    } catch (ServiceException ex) {
      RPCUtil.unwrapAndThrowException(ex);
      return null;
    }
  }
  
  @Override
  public RemoveCryptoKeysResponse removeCrypto(RemoveCryptoKeysRequest request)
      throws YarnException, IOException {
    YarnServerCommonServiceProtos.RemoveCryptoKeysRequestProto requestProto =
        ((RemoveCryptoKeysRequestPBImpl) request).getProto();
    try {
      return new RemoveCryptoKeysResponsePBImpl(proxy.removeCrypto(null,
          requestProto));
    } catch (ServiceException ex) {
      RPCUtil.unwrapAndThrowException(ex);
      return null;
    }
  }
}
