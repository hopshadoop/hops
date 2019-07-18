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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;

/**
 * State of a Queue.
 * <p>
 * A queue is in one of:
 * <ul>
 *   <li>{@link #RUNNING} - normal state.</li>
 *   <li>{@link #STOPPED} - not accepting new application submissions.</li>
 *   <li>
 *     {@link #DRAINING} - not accepting new application submissions
 *     and waiting for applications finish.
 *   </li>
 * </ul>
 * 
 * @see QueueInfo
 * @see ApplicationClientProtocol#getQueueInfo(org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest)
 */
@Public
@Stable
public enum QueueState {
  /**
   * Stopped - Not accepting submissions of new applications.
   */
  STOPPED,

  /**
   * Draining - Not accepting submissions of new applications,
   * and waiting for applications finish.
   */
  DRAINING,
  /**
   * Running - normal operation.
   */
  RUNNING
}