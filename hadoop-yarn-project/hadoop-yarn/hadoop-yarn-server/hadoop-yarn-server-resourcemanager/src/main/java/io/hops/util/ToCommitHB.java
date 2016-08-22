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

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;

public class ToCommitHB {

  final String nodeId;
  String pendingEventType;
  String pendingEventStatus;
  UpdatedContainerInfo uci;

  public ToCommitHB(String nodeId) {
    this.nodeId = nodeId;
  }

  public void addPendingEvent(String pendingEventType, String pendingEventStatus) {
    this.pendingEventStatus = pendingEventStatus;
    this.pendingEventType = pendingEventType;
  }

  public void addNodeUpdateQueue(UpdatedContainerInfo uci) {
    this.uci = uci;
  }

  public void commit() {
    final PendingEvent pendingEvent = new PendingEvent(nodeId, pendingEventType,
            pendingEventStatus, 0);

    for (Con ) {
      final io.hops.metadata.yarn.entity.UpdatedContainerInfo 
    }
    uciToPersist = new io.hops.metadata.yarn.entity.UpdatedContainerInfo(nodeId,
                      uci.get, 0, 0)
      LightWeightRequestHandler removeFinishedApplication
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        PendingEventDataAccess peDA
                = (PendingEventDataAccess) RMStorageFactory
                .getDataAccess(PendingEventDataAccess.class);
        peDA.add(pendingEvent);

        UpdatedContainerInfoDataAccess uciDA
                = (UpdatedContainerInfoDataAccess) RMStorageFactory
                .getDataAccess(UpdatedContainerInfoDataAccess.class);
        uciDA.add();
        connector.commit();
        return null;
      }
    };
    removeFinishedApplication.handle();
  }
}
