/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public final class RMAppSecurityActionsFactory {
  
  private static volatile RMAppSecurityActionsFactory _INSTANCE;
  private RMAppSecurityActions actor = null;
  
  private RMAppSecurityActionsFactory() {
  }
  
  public static RMAppSecurityActionsFactory getInstance() {
    if (_INSTANCE == null) {
      synchronized (RMAppSecurityActionsFactory.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new RMAppSecurityActionsFactory();
        }
      }
    }
    return _INSTANCE;
  }
  
  public synchronized RMAppSecurityActions getActor(Configuration conf) throws Exception {
    if (actor != null) {
      return actor;
    }
    String actorClass = conf.get(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        YarnConfiguration.HOPS_RM_SECURITY_ACTOR_DEFAULT);
    Class<?> clazz = conf.getClassByName(actorClass);
    actor = (RMAppSecurityActions) ReflectionUtils.newInstance(clazz, conf);
    actor.init();
    return actor;
  }
  
  @VisibleForTesting
  public void clear() {
    actor = null;
  }
  
  @VisibleForTesting
  public void register(RMAppSecurityActions actor) {
    this.actor = actor;
  }
}
