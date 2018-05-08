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
package org.apache.hadoop.yarn.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("rawtypes")
public class DrainDispatcher extends AsyncDispatcher {

  public DrainDispatcher() {
    this(new LinkedBlockingQueue<Event>());
  }

  public DrainDispatcher(BlockingQueue<Event> eventQueue) {
    super(eventQueue);
  }

  /**
   *  Wait till event thread enters WAITING state (i.e. waiting for new events).
   */
  public void waitForEventThreadToWait() {
    while (!isEventThreadWaiting()) {
      Thread.yield();
    }
  }

  /**
   * Busy loop waiting for all queued events to drain.
   */
  public void await() {
    while (!isDrained()) {
      Thread.yield();
    }
  }
  
  public boolean unregisterHandlerForEvent(Class<? extends Enum> eventType, boolean drain) {
    if (drain) {
      await();
    }
    EventHandler eventHandler = eventDispatchers.remove(eventType);
    return eventHandler != null;
  }
}
