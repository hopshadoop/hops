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
package io.hops.streaming;

import static io.hops.streaming.DBEvent.receivedEvents;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ContainerStatusEventReceiver {

  private static final Log LOG = LogFactory.getLog(
          ContainerStatusEventReceiver.class);

  public void createAndAddToQueue(String containerId, String rmnodeId,
          String state, String diagnostics, int exitStatus,
          int uciId, int pendingEventId) {

    ContainerStatusEvent event = new ContainerStatusEvent(containerId, rmnodeId,
            state, diagnostics, exitStatus, uciId, pendingEventId);
    receivedEvents.add(event);

  }
}
