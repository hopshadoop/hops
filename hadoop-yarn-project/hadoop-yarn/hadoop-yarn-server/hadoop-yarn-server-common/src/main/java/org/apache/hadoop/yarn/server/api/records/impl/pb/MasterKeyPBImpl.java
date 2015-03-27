/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.records.MasterKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MasterKeyPBImpl extends ProtoBase<MasterKeyProto>
    implements MasterKey {

  MasterKeyProto proto = MasterKeyProto.getDefaultInstance();
  MasterKeyProto.Builder builder = null;
  boolean viaProto = false;

  public MasterKeyPBImpl() {
    builder = MasterKeyProto.newBuilder();
  }

  public MasterKeyPBImpl(MasterKeyProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized MasterKeyProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = MasterKeyProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getKeyId() {
    MasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getKeyId());
  }

  @Override
  public synchronized void setKeyId(int id) {
    maybeInitBuilder();
    builder.setKeyId((id));
  }

  @Override
  public synchronized ByteBuffer getBytes() {
    MasterKeyProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getBytes());
  }

  @Override
  public synchronized void setBytes(ByteBuffer bytes) {
    maybeInitBuilder();
    builder.setBytes(convertToProtoFormat(bytes));
  }

  @Override
  public int hashCode() {
    return getKeyId();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MasterKey)) {
      return false;
    }
    MasterKey other = (MasterKey) obj;
    if (this.getKeyId() != other.getKeyId()) {
      return false;
    }
    return this.getBytes().equals(other.getBytes());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, getKeyId());
    WritableUtils.writeVInt(out, getBytes().array().length);
    out.write(getBytes().array());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    setKeyId(WritableUtils.readVInt(in));
    int len = WritableUtils.readVInt(in);
    byte[] keyBytes = new byte[len];
    in.readFully(keyBytes);
    setBytes(ByteBuffer.wrap(keyBytes));
  }

}
