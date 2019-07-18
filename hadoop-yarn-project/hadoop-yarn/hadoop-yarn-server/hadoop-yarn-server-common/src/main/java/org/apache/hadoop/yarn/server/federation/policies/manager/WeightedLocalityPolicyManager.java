/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.manager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.LocalityMulticastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.WeightedRandomRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import java.nio.ByteBuffer;

/**
 * Policy that allows operator to configure "weights" for routing. This picks a
 * {@link WeightedRandomRouterPolicy} for the router and a {@link
 * LocalityMulticastAMRMProxyPolicy} for the amrmproxy as they are designed to
 * work together.
 */
public class WeightedLocalityPolicyManager
    extends AbstractPolicyManager {

  private WeightedPolicyInfo weightedPolicyInfo;

  public WeightedLocalityPolicyManager() {
    //this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy =  WeightedRandomRouterPolicy.class;
    amrmProxyFederationPolicy = LocalityMulticastAMRMProxyPolicy.class;
    weightedPolicyInfo = new WeightedPolicyInfo();
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    return SubClusterPolicyConfiguration
        .newInstance(getQueue(), this.getClass().getCanonicalName(), buf);
  }

  @VisibleForTesting
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    return weightedPolicyInfo;
  }

  @VisibleForTesting
  public void setWeightedPolicyInfo(
      WeightedPolicyInfo weightedPolicyInfo) {
    this.weightedPolicyInfo = weightedPolicyInfo;
  }

}
