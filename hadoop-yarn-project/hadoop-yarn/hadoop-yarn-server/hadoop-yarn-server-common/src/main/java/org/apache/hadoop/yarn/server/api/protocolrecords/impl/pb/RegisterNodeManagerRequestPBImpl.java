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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeLabelPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeLabelProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeLabelsProto.Builder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;

public class RegisterNodeManagerRequestPBImpl extends RegisterNodeManagerRequest {
  RegisterNodeManagerRequestProto proto = RegisterNodeManagerRequestProto.getDefaultInstance();
  RegisterNodeManagerRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private Resource resource = null;
  private NodeId nodeId = null;
  private List<NMContainerStatus> containerStatuses = null;
  private Map<ApplicationId, UpdatedCryptoForApp> runningApplications = null;
  private Set<NodeLabel> labels = null;

  public RegisterNodeManagerRequestPBImpl() {
    builder = RegisterNodeManagerRequestProto.newBuilder();
  }

  public RegisterNodeManagerRequestPBImpl(RegisterNodeManagerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RegisterNodeManagerRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerStatuses != null) {
      addNMContainerStatusesToProto();
    }
    if (this.runningApplications != null) {
      addRunningApplicationsToProto();
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.labels != null) {
      builder.clearNodeLabels();
      Builder newBuilder = NodeLabelsProto.newBuilder();
      for (NodeLabel label : labels) {
        newBuilder.addNodeLabels(convertToProtoFormat(label));
      }
      builder.setNodeLabels(newBuilder.build());
    }
  }

  private synchronized void addNMContainerStatusesToProto() {
    maybeInitBuilder();
    builder.clearContainerStatuses();
    List<NMContainerStatusProto> list =
        new ArrayList<NMContainerStatusProto>();
    for (NMContainerStatus status : this.containerStatuses) {
      list.add(convertToProtoFormat(status));
    }
    builder.addAllContainerStatuses(list);
  }

    
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNodeManagerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Resource getResource() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) 
      builder.clearResource();
    this.resource = resource;
  }

  @Override
  public NodeId getNodeId() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) 
      builder.clearNodeId();
    this.nodeId = nodeId;
  }

  @Override
  public int getHttpPort() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpPort()) {
      return 0;
    }
    return (p.getHttpPort());
  }

  @Override
  public void setHttpPort(int httpPort) {
    maybeInitBuilder();
    builder.setHttpPort(httpPort);
  }
  
  @Override
  public Map<ApplicationId, UpdatedCryptoForApp> getRunningApplications() {
    initRunningApplications();
    return runningApplications;
  }
  
  private void initRunningApplications() {
    if (this.runningApplications != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<YarnServerCommonServiceProtos.RunningApplicationsProto> list = p.getRunningApplicationsList();
    this.runningApplications = new HashMap<>(list.size());
    for (YarnServerCommonServiceProtos.RunningApplicationsProto rup : list) {
      ApplicationId appId = convertFromProtoFormat(rup.getAppId());
      UpdatedCryptoForApp upc = convertFromProtoFormat(rup.getSecurityMaterialVersion());
      this.runningApplications.put(appId, upc);
    }
  }

  @Override
  public void setRunningApplications(Map<ApplicationId, UpdatedCryptoForApp> apps) {
    if (apps == null) {
      return;
    }
    initRunningApplications();
    this.runningApplications.putAll(apps);
  }
  
  private void addRunningApplicationsToProto() {
    maybeInitBuilder();
    builder.clearRunningApplications();
    if (runningApplications == null) {
      return;
    }
    
    for (Map.Entry<ApplicationId, UpdatedCryptoForApp> entry : runningApplications.entrySet()) {
      builder.addRunningApplications(YarnServerCommonServiceProtos.RunningApplicationsProto.newBuilder()
      .setAppId(convertToProtoFormat(entry.getKey()))
      .setSecurityMaterialVersion(convertToProtoFormat(entry.getValue())));
    }
  }

  @Override
  public List<NMContainerStatus> getNMContainerStatuses() {
    initContainerRecoveryReports();
    return containerStatuses;
  }
  
  private void initContainerRecoveryReports() {
    if (this.containerStatuses != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<NMContainerStatusProto> list = p.getContainerStatusesList();
    this.containerStatuses = new ArrayList<NMContainerStatus>();
    for (NMContainerStatusProto c : list) {
      this.containerStatuses.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void setContainerStatuses(
      List<NMContainerStatus> containerReports) {
    if (containerReports == null) {
      return;
    }
    initContainerRecoveryReports();
    this.containerStatuses.addAll(containerReports);
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  @Override
  public String getNMVersion() {
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNmVersion()) {
      return "";
    }
    return (p.getNmVersion());
  }

  @Override
  public void setNMVersion(String version) {
    maybeInitBuilder();
    builder.setNmVersion(version);
  }
  
  @Override
  public Set<NodeLabel> getNodeLabels() {
    initNodeLabels();
    return this.labels;
  }

  @Override
  public void setNodeLabels(Set<NodeLabel> nodeLabels) {
    maybeInitBuilder();
    builder.clearNodeLabels();
    this.labels = nodeLabels;
  }
  
  private void initNodeLabels() {
    if (this.labels != null) {
      return;
    }
    RegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeLabels()) {
      labels=null;
      return;
    }
    NodeLabelsProto nodeLabels = p.getNodeLabels();
    labels = new HashSet<NodeLabel>();
    for(NodeLabelProto nlp : nodeLabels.getNodeLabelsList()) {
      labels.add(convertFromProtoFormat(nlp));
    }
  }

  private NodeLabelPBImpl convertFromProtoFormat(NodeLabelProto p) {
    return new NodeLabelPBImpl(p);
  }

  private NodeLabelProto convertToProtoFormat(NodeLabel t) {
    return ((NodeLabelPBImpl)t).getProto();
  }

  private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }
  
  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
  
  private UpdatedCryptoForAppPBImpl convertFromProtoFormat(YarnServerCommonServiceProtos.UpdatedCryptoForAppProto p) {
    return new UpdatedCryptoForAppPBImpl(p);
  }
  
  private YarnServerCommonServiceProtos.UpdatedCryptoForAppProto convertToProtoFormat(UpdatedCryptoForApp t) {
    return ((UpdatedCryptoForAppPBImpl) t).getProto();
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl)t).getProto();
  }

  private NMContainerStatusPBImpl convertFromProtoFormat(NMContainerStatusProto c) {
    return new NMContainerStatusPBImpl(c);
  }
  
  private NMContainerStatusProto convertToProtoFormat(NMContainerStatus c) {
    return ((NMContainerStatusPBImpl)c).getProto();
  }
}