/*
 * Copyright 2016 Apache Software Foundation.
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
package io.hops.util;

import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.hops.metadata.yarn.dal.RMNodeApplicationsDataAccess;

/**
 *
 * @author gautier
 */
public class DBUtilityTests {
  public static Map<String, io.hops.metadata.yarn.entity.RMNode> getAllRMNodess() throws IOException {
    LightWeightRequestHandler getRMNodesHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMNodeDataAccess DA = (RMNodeDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMNodeDataAccess.class);
        Map<String, io.hops.metadata.yarn.entity.RMNode> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String, io.hops.metadata.yarn.entity.RMNode>) getRMNodesHandler.handle();
  }
  
  public static Map<String, io.hops.metadata.yarn.entity.Resource> getAllResources() throws IOException {
    LightWeightRequestHandler getResourcesHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        ResourceDataAccess DA = (ResourceDataAccess) YarnAPIStorageFactory.
                getDataAccess(ResourceDataAccess.class);
        Map<String, io.hops.metadata.yarn.entity.Resource> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String, io.hops.metadata.yarn.entity.Resource>) getResourcesHandler.handle();
  }
  
    public static List<PendingEvent> getAllPendingEvents() throws IOException {
    LightWeightRequestHandler getPendingEventsHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        PendingEventDataAccess DA = (PendingEventDataAccess) YarnAPIStorageFactory.
                getDataAccess(PendingEventDataAccess.class);
        List<PendingEvent> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (List<PendingEvent>) getPendingEventsHandler.handle();
  }

  public static Map<String, Map<Integer, List<UpdatedContainerInfo>>> getAllUCIs()
          throws IOException {
    LightWeightRequestHandler getUCIHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        UpdatedContainerInfoDataAccess DA = (UpdatedContainerInfoDataAccess) YarnAPIStorageFactory.
                getDataAccess(UpdatedContainerInfoDataAccess.class);
        Map<String, Map<Integer, List<UpdatedContainerInfo>>> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String, Map<Integer, List<UpdatedContainerInfo>>>) getUCIHandler.
            handle();
  }
  
  public static Map<String, io.hops.metadata.yarn.entity.ContainerStatus> getAllContainerStatus()
          throws IOException {
    LightWeightRequestHandler getContainerStatusHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        ContainerStatusDataAccess DA = (ContainerStatusDataAccess) YarnAPIStorageFactory.
                getDataAccess(ContainerStatusDataAccess.class);
        Map<String,io.hops.metadata.yarn.entity.ContainerStatus> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String,io.hops.metadata.yarn.entity.ContainerStatus>) getContainerStatusHandler.
            handle();
  }
  
  public static  Map<String, Boolean> getAllNextHeartbeat()
          throws IOException {
    LightWeightRequestHandler getNextHeartBeatHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        NextHeartbeatDataAccess DA = (NextHeartbeatDataAccess) YarnAPIStorageFactory.
                getDataAccess(NextHeartbeatDataAccess.class);
         Map<String, Boolean> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return ( Map<String, Boolean>) getNextHeartBeatHandler.
            handle();
  }
  
  public static  Map<String, Set<ContainerId>> getAllContainersToCleanUp()
          throws IOException {
    LightWeightRequestHandler getContainerToCleanHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        ContainerIdToCleanDataAccess DA = (ContainerIdToCleanDataAccess) YarnAPIStorageFactory.
                getDataAccess(ContainerIdToCleanDataAccess.class);
         Map<String, Set<ContainerId>> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return ( Map<String, Set<ContainerId>>) getContainerToCleanHandler.
            handle();
  }
  
  public static  Map<String, List<ContainerId>> getAllAppsToCleanup()
          throws IOException {
    LightWeightRequestHandler getAppsToCleanHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMNodeApplicationsDataAccess DA = (RMNodeApplicationsDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMNodeApplicationsDataAccess.class);
         Map<String, List<ContainerId>> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return ( Map<String, List<ContainerId>>) getAppsToCleanHandler.
            handle();
  }
}
