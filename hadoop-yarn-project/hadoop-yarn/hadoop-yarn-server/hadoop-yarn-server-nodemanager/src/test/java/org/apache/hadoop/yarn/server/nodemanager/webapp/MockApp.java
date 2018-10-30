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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

public class MockApp implements Application {

  final String user;
  final String userFolder;
  final ApplicationId appId;
  Map<ContainerId, Container> containers = new HashMap<ContainerId, Container>();
  ApplicationState appState;
  Application app;
  int x509Version;
  long jwtExpiration;

  public MockApp(int uniqId) {
    this("mockUser", 1234, uniqId, "mockUserFolder");
  }

  public MockApp(String user, long clusterTimeStamp, int uniqId, String userFolder) {
    super();
    this.user = user;
    this.userFolder = userFolder;
    // Add an application and the corresponding containers
    RecordFactory recordFactory = RecordFactoryProvider
        .getRecordFactory(new Configuration());
    this.appId = BuilderUtils.newApplicationId(recordFactory, clusterTimeStamp,
        uniqId);
    appState = ApplicationState.NEW;
    this.x509Version = 0;
    this.jwtExpiration = -1L;
  }

  public void setState(ApplicationState state) {
    this.appState = state;
  }

  public String getUser() {
    return user;
  }

  public String getUserFolder() {
    return userFolder;
  }

  public Map<ContainerId, Container> getContainers() {
    return containers;
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public ApplicationState getApplicationState() {
    return appState;
  }

  @Override
  public int getX509Version() {
    return x509Version;
  }
  
  @Override
  public void setX509Version(int x509Version) {
    this.x509Version = x509Version;
  }
  
  @Override
  public long getJWTExpiration() {
    return jwtExpiration;
  }
  
  @Override
  public void setJWTExpiration(long jwtExpiration) {
    this.jwtExpiration = jwtExpiration;
  }
  
  public void handle(ApplicationEvent event) {}

}
