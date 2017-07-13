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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysResponse;

public class RemoveCryptoKeysResponsePBImpl extends RemoveCryptoKeysResponse {
  YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto proto =
      YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto
          .getDefaultInstance();
  YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto.Builder
      builder = null;
  boolean viaProto = false;
  
  public RemoveCryptoKeysResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto
        .newBuilder();
  }
  
  public RemoveCryptoKeysResponsePBImpl(YarnServerCommonServiceProtos
      .RemoveCryptoKeysResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto getProto() {
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
      builder = YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProto
          .newBuilder();
    }
    viaProto = false;
  }
  
  @Override
  public boolean getSuccess() {
    YarnServerCommonServiceProtos.RemoveCryptoKeysResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    
    return (p.hasSuccess()) ? p.getSuccess() : false;
  }
  
  @Override
  public void setSuccess(boolean success) {
    maybeInitBuilder();
    builder.setSuccess(success);
  }
}
