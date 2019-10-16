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
package io.hops.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Factory for SecurityActions.
 * Depending on the client it will create a *single* instance of FsSecurityActions
 * or RMAppSecurityActions
 */
public final class HopsSecurityActionsFactory<ACTOR extends AbstractSecurityActions> {
  
  private static volatile HopsSecurityActionsFactory _INSTANCE;
  private final Map<String, ACTOR> actors;
  
  private HopsSecurityActionsFactory() {
    actors = new ConcurrentHashMap<>(2);
  }
  
  public static HopsSecurityActionsFactory getInstance() {
    if (_INSTANCE == null) {
      synchronized (HopsSecurityActionsFactory.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new HopsSecurityActionsFactory();
        }
      }
    }
    return _INSTANCE;
  }
  
  public ACTOR getActor(final Configuration conf, String actorClass) throws Exception {
    ACTOR actor = actors.computeIfAbsent(actorClass, new Function<String, ACTOR>() {
      @Override
      public ACTOR apply(String s) {
        return createActor(conf, s);
      }
    });
    if (actor != null) {
      return actor;
    }
    throw new RuntimeException("Could not load class " + actorClass);
  }
  
  private ACTOR createActor(Configuration conf, String actorClass) {
    try {
      Class<?> clazz = conf.getClassByName(actorClass);
      if (clazz != null && !AbstractSecurityActions.class.isAssignableFrom(clazz)) {
        throw new RuntimeException(clazz + " is not " + AbstractSecurityActions.class);
      }
      ACTOR actor = (ACTOR) ReflectionUtils.newInstance(clazz, conf);
      actor.init(conf);
      actor.start();
      return actor;
    } catch (ClassNotFoundException ex) {
      return null;
    }
  }
  
  @VisibleForTesting
  public void clear() {
    actors.clear();
  }
  
  @VisibleForTesting
  public void register(String actorClass, ACTOR actor) {
    actors.put(actorClass, actor);
  }
}
