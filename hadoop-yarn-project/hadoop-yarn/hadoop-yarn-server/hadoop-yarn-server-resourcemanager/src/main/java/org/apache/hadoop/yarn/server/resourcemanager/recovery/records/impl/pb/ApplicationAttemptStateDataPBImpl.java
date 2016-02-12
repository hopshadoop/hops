/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerStatusPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ApplicationAttemptStateDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RMAppAttemptStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ApplicationAttemptStateDataPBImpl
    extends ProtoBase<ApplicationAttemptStateDataProto>
    implements ApplicationAttemptStateData {

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  ApplicationAttemptStateDataProto proto =
      ApplicationAttemptStateDataProto.getDefaultInstance();
  ApplicationAttemptStateDataProto.Builder builder = null;
  boolean viaProto = false;

  private ApplicationAttemptId attemptId = null;
  private Container masterContainer = null;
  private ByteBuffer appAttemptTokens = null;

  public ApplicationAttemptStateDataPBImpl() {
    builder = ApplicationAttemptStateDataProto.newBuilder();
  }

  public ApplicationAttemptStateDataPBImpl(
      ApplicationAttemptStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationAttemptStateDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.attemptId != null) {
      builder.setAttemptId(((ApplicationAttemptIdPBImpl) attemptId).getProto());
    }
    if (this.masterContainer != null) {
      builder
          .setMasterContainer(((ContainerPBImpl) masterContainer).getProto());
    }
    if (this.appAttemptTokens != null) {
      builder.setAppAttemptTokens(convertToProtoFormat(this.appAttemptTokens));
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
      builder = ApplicationAttemptStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationAttemptId getAttemptId() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (attemptId != null) {
      return attemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    attemptId = new ApplicationAttemptIdPBImpl(p.getAttemptId());
    return attemptId;
  }

  @Override
  public void setAttemptId(ApplicationAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) {
      builder.clearAttemptId();
    }
    this.attemptId = attemptId;
  }

  @Override
  public Container getMasterContainer() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (masterContainer != null) {
      return masterContainer;
    }
    if (!p.hasMasterContainer()) {
      return null;
    }
    masterContainer = new ContainerPBImpl(p.getMasterContainer());
    return masterContainer;
  }

  @Override
  public void setMasterContainer(Container container) {
    maybeInitBuilder();
    if (container == null) {
      builder.clearMasterContainer();
    }
    this.masterContainer = container;
  }

  @Override
  public ByteBuffer getAppAttemptTokens() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (appAttemptTokens != null) {
      return appAttemptTokens;
    }
    if (!p.hasAppAttemptTokens()) {
      return null;
    }
    this.appAttemptTokens = convertFromProtoFormat(p.getAppAttemptTokens());
    return appAttemptTokens;
  }

  @Override
  public void setAppAttemptTokens(ByteBuffer attemptTokens) {
    maybeInitBuilder();
    if (attemptTokens == null) {
      builder.clearAppAttemptTokens();
    }
    this.appAttemptTokens = attemptTokens;
  }

  @Override
  public RMAppAttemptState getState() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getAppAttemptState());
  }

  @Override
  public void setState(RMAppAttemptState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearAppAttemptState();
      return;
    }
    builder.setAppAttemptState(convertToProtoFormat(state));
  }

  @Override
  public String getFinalTrackingUrl() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalTrackingUrl()) {
      return null;
    }
    return p.getFinalTrackingUrl();
  }

  @Override
  public void setFinalTrackingUrl(String url) {
    maybeInitBuilder();
    if (url == null) {
      builder.clearFinalTrackingUrl();
      return;
    }
    builder.setFinalTrackingUrl(url);
  }

  @Override
  public String getDiagnostics() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public long getStartTime() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public float getProgress() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
  }

  @Override
  public String getHost() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getHost();
  }

  @Override
  public void setHost(String Host) {
    maybeInitBuilder();
    builder.setHost(Host);
  }

  @Override
  public int getRpcPort() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public void setRpcPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort(rpcPort);
  }

  @Override
  public FinalApplicationStatus getFinalApplicationStatus() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public void setFinalApplicationStatus(FinalApplicationStatus finishState) {
    maybeInitBuilder();
    if (finishState == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder.setFinalApplicationStatus(convertToProtoFormat(finishState));
  }

  @Override
  public void addALLRanNodes(List<YarnProtos.NodeIdProto> ranNodes) {
    maybeInitBuilder();
    builder.addAllRanNodes(ranNodes);
  }

  @Override
  public Set<NodeId> getRanNodes() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;

    Set<NodeId> ranNodes = new HashSet<NodeId>();
    for (YarnProtos.NodeIdProto nodeIdProto : p.getRanNodesList()) {
      ranNodes.add(convertFromProtoFormat(nodeIdProto));
    }
    return ranNodes;
  }

  @Override
  public List<ContainerStatus> getJustFinishedContainers() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerStatus> justFinishedContainers =
        new ArrayList<ContainerStatus>();
    for (YarnProtos.ContainerStatusProto containerStatusProto : p.
        getJustFinishedFontainersList()) {
      justFinishedContainers.add(convertFromProtoFormat(containerStatusProto));
    }
    return justFinishedContainers;
  }

  @Override
  public void addAllJustFinishedContainers(
      List<YarnProtos.ContainerStatusProto> justFinishedContainers) {
    maybeInitBuilder();
    builder.addAllJustFinishedFontainers(justFinishedContainers);
  }

  public static ApplicationAttemptStateData newApplicationAttemptStateData(
      ApplicationAttemptId attemptId, Container container,
      ByteBuffer attemptTokens, long startTime, RMAppAttemptState finalState,
      String finalTrackingUrl, String diagnostics,
      FinalApplicationStatus amUnregisteredFinalStatus, int exitStatus, Set<NodeId> ranNodes,
      List<ContainerStatus> justFinishedContainers, float progress, String host,
      int rpcPort) {
    ApplicationAttemptStateData attemptStateData =
        recordFactory.newRecordInstance(ApplicationAttemptStateData.class);
    attemptStateData.setAttemptId(attemptId);
    attemptStateData.setMasterContainer(container);
    attemptStateData.setAppAttemptTokens(attemptTokens);
    attemptStateData.setState(finalState);
    attemptStateData.setFinalTrackingUrl(finalTrackingUrl);
    attemptStateData.setDiagnostics(diagnostics);
    attemptStateData.setStartTime(startTime);
    attemptStateData.setFinalApplicationStatus(amUnregisteredFinalStatus);
    attemptStateData.setAMContainerExitStatus(exitStatus);
    attemptStateData.setProgress(progress);
    attemptStateData.setHost(host);
    attemptStateData.setRpcPort(rpcPort);

    List<YarnProtos.NodeIdProto> nodeIds =
        new ArrayList<YarnProtos.NodeIdProto>();
    for (NodeId nodeId : ranNodes) {
      nodeIds.add(((NodeIdPBImpl) nodeId).getProto());
    }
    attemptStateData.addALLRanNodes(nodeIds);

    if (justFinishedContainers != null) {
      List<YarnProtos.ContainerStatusProto> justFinishedContainersProto =
          new ArrayList<YarnProtos.ContainerStatusProto>();
      for (ContainerStatus containerStatus : justFinishedContainers) {
        justFinishedContainersProto
            .add(((ContainerStatusPBImpl) containerStatus).getProto());
      }
      attemptStateData
          .addAllJustFinishedContainers(justFinishedContainersProto);
    }
    return attemptStateData;
  }

  private static String RM_APP_ATTEMPT_PREFIX = "RMATTEMPT_";

  public static RMAppAttemptStateProto convertToProtoFormat(
      RMAppAttemptState e) {
    return RMAppAttemptStateProto.valueOf(RM_APP_ATTEMPT_PREFIX + e.name());
  }

  public static RMAppAttemptState convertFromProtoFormat(
      RMAppAttemptStateProto e) {
    return RMAppAttemptState.
        valueOf(e.name().replace(RM_APP_ATTEMPT_PREFIX, ""));
  }

  public static NodeId convertFromProtoFormat(YarnProtos.NodeIdProto n) {
    return ProtoUtils.convertFromProtoFormat(n);
  }

  public static ContainerStatus convertFromProtoFormat(
      YarnProtos.ContainerStatusProto c) {
    return new ContainerStatusPBImpl(c);
  }

  private FinalApplicationStatusProto convertToProtoFormat(
      FinalApplicationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  private FinalApplicationStatus convertFromProtoFormat(
      FinalApplicationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }
  
  @Override
  public int getAMContainerExitStatus() {
    ApplicationAttemptStateDataProtoOrBuilder p = viaProto ? proto : builder;
    int exitStatus = p.getAmContainerExitStatus();
    return exitStatus;
  }

  @Override
  public void setAMContainerExitStatus(int exitStatus) {
    maybeInitBuilder();
    builder.setAmContainerExitStatus(exitStatus);
  }
}
