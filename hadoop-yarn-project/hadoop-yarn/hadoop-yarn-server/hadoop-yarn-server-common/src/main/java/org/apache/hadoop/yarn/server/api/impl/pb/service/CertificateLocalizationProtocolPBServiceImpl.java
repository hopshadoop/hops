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
package org.apache.hadoop.yarn.server.api.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocol;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.MaterializeCryptoKeysRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.MaterializeCryptoKeysResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveCryptoKeysRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveCryptoKeysResponsePBImpl;

import java.io.IOException;

public class CertificateLocalizationProtocolPBServiceImpl implements
    CertificateLocalizationProtocolPB {
  
  private CertificateLocalizationProtocol real;
  
  public CertificateLocalizationProtocolPBServiceImpl(CertificateLocalizationProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public YarnServerCommonServiceProtos.MaterializeCryptoKeysResponseProto materializeCrypto(
      RpcController controller,
      YarnServerCommonServiceProtos.MaterializeCryptoKeysRequestProto proto)
      throws ServiceException {
    MaterializeCryptoKeysRequestPBImpl request = new
        MaterializeCryptoKeysRequestPBImpl(proto);
    
    try {
      MaterializeCryptoKeysResponse response = real.materializeCrypto(request);
      return ((MaterializeCryptoKeysResponsePBImpl) response).getProto();
    } catch (YarnException ex) {
      throw new ServiceException(ex);
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }
  
  @Override
  public YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto removeCrypto(
      RpcController controller,
      YarnServerCommonServiceProtos.RemoveCryptoKeysRequestProto proto)
      throws ServiceException {
    RemoveCryptoKeysRequestPBImpl request = new
        RemoveCryptoKeysRequestPBImpl(proto);
    
    try {
      RemoveCryptoKeysResponse response = real.removeCrypto(request);
      return ((RemoveCryptoKeysResponsePBImpl) response).getProto();
    } catch (YarnException ex) {
      throw new ServiceException(ex);
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }
}
