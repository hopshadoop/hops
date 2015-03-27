/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksResponseProto;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import java.io.IOException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link NamenodeProtocolPB} to the
 * {@link NamenodeProtocol} server implementation.
 */
public class NamenodeProtocolServerSideTranslatorPB
    implements NamenodeProtocolPB {
  private final NamenodeProtocol impl;

  public NamenodeProtocolServerSideTranslatorPB(NamenodeProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetBlocksResponseProto getBlocks(RpcController unused,
      GetBlocksRequestProto request) throws ServiceException {
    DatanodeInfo dnInfo =
        new DatanodeInfo(PBHelper.convert(request.getDatanode()));
    BlocksWithLocations blocks;
    try {
      blocks = impl.getBlocks(dnInfo, request.getSize());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return GetBlocksResponseProto.newBuilder()
        .setBlocks(PBHelper.convert(blocks)).build();
  }

  @Override
  public GetBlockKeysResponseProto getBlockKeys(RpcController unused,
      GetBlockKeysRequestProto request) throws ServiceException {
    ExportedBlockKeys keys;
    try {
      keys = impl.getBlockKeys();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    GetBlockKeysResponseProto.Builder builder =
        GetBlockKeysResponseProto.newBuilder();
    if (keys != null) {
      builder.setKeys(PBHelper.convert(keys));
    }
    return builder.build();
  }

  @Override
  public VersionResponseProto versionRequest(RpcController controller,
      VersionRequestProto request) throws ServiceException {
    NamespaceInfo info;
    try {
      info = impl.versionRequest();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return VersionResponseProto.newBuilder().setInfo(PBHelper.convert(info))
        .build();
  }
}
