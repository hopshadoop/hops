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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

@Private
@Unstable
public class SchedulerApplication {

  private Queue queue;//recovered
  private final String user;//recovered
  private SchedulerApplicationAttempt currentAttempt;
      //recovered in fifoscheduler

  public SchedulerApplication(Queue queue, String user) {
    this.queue = queue;
    this.user = user;
  }

  public Queue getQueue() {
    return queue;
  }
  
  public void setQueue(Queue queue) {
    this.queue = queue;
  }

  public String getUser() {
    return user;
  }

  public SchedulerApplicationAttempt getCurrentAppAttempt() {
    return currentAttempt;
  }

  public void setCurrentAppAttempt(SchedulerApplicationAttempt currentAttempt,
      TransactionState ts) {
    if (this.currentAttempt != null && ts != null) {
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
              this.currentAttempt.appSchedulingInfo.applicationId).
          getFiCaSchedulerAppInfo(this.currentAttempt.
              getApplicationAttemptId()).remove(this.currentAttempt);
    }
    this.currentAttempt = currentAttempt;
    if (ts != null) {
      ((TransactionStateImpl) ts).getSchedulerApplicationInfos(
              this.currentAttempt.appSchedulingInfo.applicationId)
          .setFiCaSchedulerAppInfo(currentAttempt);
    }
  }

  public void stop(RMAppState rmAppFinalState) {
    queue.getMetrics().finishApp(user, rmAppFinalState);
  }

}
