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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.VersionRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlockKeysResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.GetBlocksRequestProto;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.ipc.ProtocolTranslator;

/**
 * This class is the client side translator to translate the requests made on
 * {@link NamenodeProtocol} interfaces to the RPC server implementing
 * {@link NamenodeProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class NamenodeProtocolTranslatorPB
    implements NamenodeProtocol, ProtocolMetaInterface, Closeable, ProtocolTranslator {
  /**
   * RpcController is not used and hence is set to null
   */
  private final static RpcController NULL_CONTROLLER = null;
  
  /*
   * Protobuf requests with no parameters instantiated only once
   */
  private static final GetBlockKeysRequestProto VOID_GET_BLOCKKEYS_REQUEST =
      GetBlockKeysRequestProto.newBuilder().build();
  private static final VersionRequestProto VOID_VERSION_REQUEST =
      VersionRequestProto.newBuilder().build();

  final private NamenodeProtocolPB rpcProxy;
  
  public NamenodeProtocolTranslatorPB(NamenodeProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }
  
  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    GetBlocksRequestProto req = GetBlocksRequestProto.newBuilder()
        .setDatanode(PBHelper.convert((DatanodeID) datanode)).setSize(size)
        .build();
    try {
      return PBHelper
          .convert(rpcProxy.getBlocks(NULL_CONTROLLER, req).getBlocks());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    try {
      GetBlockKeysResponseProto rsp =
          rpcProxy.getBlockKeys(NULL_CONTROLLER, VOID_GET_BLOCKKEYS_REQUEST);
      return rsp.hasKeys() ? PBHelper.convert(rsp.getKeys()) : null;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    try {
      return PBHelper.convert(
          rpcProxy.versionRequest(NULL_CONTROLLER, VOID_VERSION_REQUEST)
              .getInfo());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy, NamenodeProtocolPB.class,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(NamenodeProtocolPB.class), methodName);
  }
}
