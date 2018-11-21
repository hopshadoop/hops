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

import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialGeneratedEvent;

import static org.junit.Assert.assertFalse;

public class RMSecurityHandlersBaseTest {
  protected class MockRMAppEventHandler implements EventHandler<RMAppEvent> {
    
    private final RMAppEventType expectedEventType;
    private boolean assertionFailure;
    
    protected MockRMAppEventHandler(RMAppEventType expectedEventType) {
      this.expectedEventType = expectedEventType;
      assertionFailure = false;
    }
    
    @Override
    public void handle(RMAppEvent event) {
      if (event == null) {
        assertionFailure = true;
      } else if (!expectedEventType.equals(event.getType())) {
        assertionFailure = true;
      } else if (event.getType().equals(RMAppEventType.SECURITY_MATERIAL_GENERATED)) {
        if (!(event instanceof RMAppSecurityMaterialGeneratedEvent)) {
          assertionFailure = true;
        }
      }
    }
    
    protected void verifyEvent() {
      assertFalse(assertionFailure);
    }
    
  }
}
