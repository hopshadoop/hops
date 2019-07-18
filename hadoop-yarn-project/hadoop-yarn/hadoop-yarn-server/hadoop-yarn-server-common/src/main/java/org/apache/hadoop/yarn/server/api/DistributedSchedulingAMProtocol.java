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

package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * <p>
 * This protocol extends the <code>ApplicationMasterProtocol</code>. It is used
 * by the <code>DistributedScheduler</code> running on the NodeManager to wrap
 * the request / response objects of the <code>registerApplicationMaster</code>
 * and <code>allocate</code> methods of the protocol with additional information
 * required to perform distributed scheduling.
 * </p>
 */
public interface DistributedSchedulingAMProtocol
    extends ApplicationMasterProtocol {

  /**
   * <p>
   * Extends the <code>registerApplicationMaster</code> to wrap the response
   * with additional metadata.
   * </p>
   *
   * @param request
   *          ApplicationMaster registration request
   * @return A <code>RegisterDistributedSchedulingAMResponse</code> that
   *         contains a standard AM registration response along with additional
   *         information required for distributed scheduling
   * @throws YarnException YarnException
   * @throws IOException IOException
   */
  @Public
  @Unstable
  @Idempotent
  RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
            RegisterApplicationMasterRequest request)
            throws YarnException, IOException;

  /**
   * <p>
   * Extends the <code>allocate</code> to wrap the response with additional
   * metadata.
   * </p>
   *
   * @param request
   *          ApplicationMaster allocate request
   * @return A <code>DistributedSchedulingAllocateResponse</code> that contains
   *         a standard AM allocate response along with additional information
   *         required for distributed scheduling
   * @throws YarnException YarnException
   * @throws IOException IOException
   */
  @Public
  @Unstable
  @Idempotent
  DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException;
}
