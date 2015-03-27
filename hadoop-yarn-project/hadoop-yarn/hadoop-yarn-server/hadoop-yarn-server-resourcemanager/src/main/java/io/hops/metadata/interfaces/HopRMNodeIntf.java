/*
 * Copyright (C) 2015 hops.io.
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
package io.hops.metadata.interfaces;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public interface HopRMNodeIntf {

  public int getId();

  public void setId(int id);

  public String getCurrentStateNoPersistence();

  public String getCurrentState();

  public int getResourceId();

  public void setResourceId(int id);

  public int getNodebaseId();

  public void setNodebaseId(int id);

  public int getRmcontextId();

  public NodeHeartbeatResponse getLastNodeHeartBeatResponseNoPersistence();

  public String getHostNameNoPersistence();

  public int getCommandPortNoPersistence();

  public int getHttpPortNoPeristence();

  public NodeId getNodeIDNoPersistance();

  public String getNodeAddressNoPersistence();

  public String getHttpAddressNoPersistence();

  public String getHealthReportNoPersistance();

  public void setHealthReportNoPersistance(String healthReport);

  public void setLastHealthReportTimeNoPersistance(long lastHealthReportTime);

  public long getLastHealthReportTimeNoPersistance();

  public void setRMContext(RMContext rmcontext);

  public RMContext getRMContext();

  public boolean getNextHeartBeat();
}