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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class NodeHeartbeatResponsePBImpl
    extends ProtoBase<NodeHeartbeatResponseProto>
    implements NodeHeartbeatResponse {
  NodeHeartbeatResponseProto proto =
      NodeHeartbeatResponseProto.getDefaultInstance();
  NodeHeartbeatResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<ContainerId> containersToCleanup = null;
  private List<ApplicationId> applicationsToCleanup = null;
  private MasterKey containerTokenMasterKey = null;
  private MasterKey nmTokenMasterKey = null;
  
  public NodeHeartbeatResponsePBImpl() {
    builder = NodeHeartbeatResponseProto.newBuilder();
  }

  public NodeHeartbeatResponsePBImpl(NodeHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public synchronized  NodeHeartbeatResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  public synchronized byte[] getByteArray(){
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto.toByteArray();
  }
  
  private void mergeLocalToBuilder() {
    if (this.containersToCleanup != null) {
      addContainersToCleanupToProto();
    }
    if (this.applicationsToCleanup != null) {
      addApplicationsToCleanupToProto();
    }
    if (this.containerTokenMasterKey != null) {
      builder.setContainerTokenMasterKey(
          convertToProtoFormat(this.containerTokenMasterKey));
    }
    if (this.nmTokenMasterKey != null) {
      builder.setNmTokenMasterKey(convertToProtoFormat(this.nmTokenMasterKey));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  
  @Override
  public synchronized int getResponseId() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public synchronized MasterKey getContainerTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerTokenMasterKey != null) {
      return this.containerTokenMasterKey;
    }
    if (!p.hasContainerTokenMasterKey()) {
      return null;
    }
    this.containerTokenMasterKey =
        convertFromProtoFormat(p.getContainerTokenMasterKey());
    return this.containerTokenMasterKey;
  }

  @Override
  public void setContainerTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) {
      builder.clearContainerTokenMasterKey();
    }
    this.containerTokenMasterKey = masterKey;
  }

  @Override
  public synchronized MasterKey getNMTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nmTokenMasterKey != null) {
      return this.nmTokenMasterKey;
    }
    if (!p.hasNmTokenMasterKey()) {
      return null;
    }
    this.nmTokenMasterKey = convertFromProtoFormat(p.getNmTokenMasterKey());
    return this.nmTokenMasterKey;
  }

  @Override
  public void setNMTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) {
      builder.clearNmTokenMasterKey();
    }
    this.nmTokenMasterKey = masterKey;
  }

  @Override
  public synchronized NodeAction getNodeAction() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeAction()) {
      return null;
    }
    return (convertFromProtoFormat(p.getNodeAction()));
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
      return;
    }
    builder.setNodeAction(convertToProtoFormat(nodeAction));
  }

  @Override
  public synchronized String getDiagnosticsMessage() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsMessage()) {
      return null;
    }
    return p.getDiagnosticsMessage();
  }

  @Override
  public void setDiagnosticsMessage(String diagnosticsMessage) {
    maybeInitBuilder();
    if (diagnosticsMessage == null) {
      builder.clearDiagnosticsMessage();
      return;
    }
    builder.setDiagnosticsMessage((diagnosticsMessage));
  }

  @Override
  public List<ContainerId> getContainersToCleanup() {
    initContainersToCleanup();
    return this.containersToCleanup;
  }

  private synchronized void initContainersToCleanup() {
    if (this.containersToCleanup != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToCleanupList();
    this.containersToCleanup = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToCleanup.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllContainersToCleanup(
      final List<ContainerId> containersToCleanup) {
    if (containersToCleanup == null) {
      return;
    }
    initContainersToCleanup();
    this.containersToCleanup.addAll(containersToCleanup);
  }

  private void addContainersToCleanupToProto() {
    maybeInitBuilder();
    builder.clearContainersToCleanup();
    if (containersToCleanup == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {

      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToCleanup(iterable);
  }

  @Override
  public List<ApplicationId> getApplicationsToCleanup() {
    initApplicationsToCleanup();
    return this.applicationsToCleanup;
  }

  private synchronized void initApplicationsToCleanup() {
    if (this.applicationsToCleanup != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationIdProto> list = p.getApplicationsToCleanupList();
    this.applicationsToCleanup = new ArrayList<ApplicationId>();

    for (ApplicationIdProto c : list) {
      this.applicationsToCleanup.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllApplicationsToCleanup(
      final List<ApplicationId> applicationsToCleanup) {
    if (applicationsToCleanup == null) {
      return;
    }
    initApplicationsToCleanup();
    this.applicationsToCleanup.addAll(applicationsToCleanup);
  }

  private void addApplicationsToCleanupToProto() {
    maybeInitBuilder();
    builder.clearApplicationsToCleanup();
    if (applicationsToCleanup == null) {
      return;
    }
    Iterable<ApplicationIdProto> iterable = new Iterable<ApplicationIdProto>() {

      @Override
      public Iterator<ApplicationIdProto> iterator() {
        return new Iterator<ApplicationIdProto>() {

          Iterator<ApplicationId> iter = applicationsToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ApplicationIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllApplicationsToCleanup(iterable);
  }

  @Override
  public synchronized long getNextHeartBeatInterval() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNextHeartBeatInterval());
  }

  @Override
  public void setNextHeartBeatInterval(long nextHeartBeatInterval) {
    maybeInitBuilder();
    builder.setNextHeartBeatInterval(nextHeartBeatInterval);
  }
  
  @Override
  public synchronized boolean getNextheartbeat() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNextheartbeat());
  }
  
  @Override
  public void setNextheartbeat(boolean nextHeartbeat) {
    maybeInitBuilder();
    builder.setNextheartbeat(nextHeartbeat);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private NodeAction convertFromProtoFormat(NodeActionProto p) {
    return NodeAction.valueOf(p.name());
  }

  private NodeActionProto convertToProtoFormat(NodeAction t) {
    return NodeActionProto.valueOf(t.name());
  }

  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl) t).getProto();
  }
}

