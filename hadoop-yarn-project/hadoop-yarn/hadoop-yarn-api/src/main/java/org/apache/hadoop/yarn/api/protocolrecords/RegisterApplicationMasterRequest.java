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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.util.Records;
/**
 * The request sent by the {@code ApplicationMaster} to {@code ResourceManager}
 * on registration.
 * <p>
 * The registration includes details such as:
 * <ul>
 *   <li>Hostname on which the AM is running.</li>
 *   <li>RPC Port</li>
 *   <li>Tracking URL</li>
 * </ul>
 * 
 * @see ApplicationMasterProtocol#registerApplicationMaster(RegisterApplicationMasterRequest)
 */
@Public
@Stable
public abstract class RegisterApplicationMasterRequest {

  /**
   * Create a new instance of <code>RegisterApplicationMasterRequest</code>.
   * If <em>port, trackingUrl</em> is not used, use the following default value:
   * <ul>
   *  <li>port: -1</li>
   *  <li>trackingUrl: null</li>
   * </ul>
   * The port is allowed to be any integer larger than or equal to -1.
   * @return the new instance of <code>RegisterApplicationMasterRequest</code>
   */
  @Public
  @Stable
  public static RegisterApplicationMasterRequest newInstance(String host,
      int port, String trackingUrl) {
    RegisterApplicationMasterRequest request =
        Records.newRecord(RegisterApplicationMasterRequest.class);
    request.setHost(host);
    request.setRpcPort(port);
    request.setTrackingUrl(trackingUrl);
    return request;
  }

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code> is 
   * running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code> is running
   */
  @Public
  @Stable
  public abstract String getHost();
  
  /**
   * Set the <em>host</em> on which the <code>ApplicationMaster</code> is 
   * running.
   * @param host <em>host</em> on which the <code>ApplicationMaster</code> 
   *             is running
   */
  @Public
  @Stable
  public abstract void setHost(String host);

  /**
   * Get the <em>RPC port</em> on which the {@code ApplicationMaster} is
   * responding.
   * @return the <em>RPC port</em> on which the {@code ApplicationMaster}
   *         is responding
   */
  @Public
  @Stable
  public abstract int getRpcPort();
  
  /**
   * Set the <em>RPC port</em> on which the {@code ApplicationMaster} is
   * responding.
   * @param port <em>RPC port</em> on which the {@code ApplicationMaster}
   *             is responding
   */
  @Public
  @Stable
  public abstract void setRpcPort(int port);

  /**
   * Get the <em>tracking URL</em> for the <code>ApplicationMaster</code>.
   * This url if contains scheme then that will be used by resource manager
   * web application proxy otherwise it will default to http.
   * @return <em>tracking URL</em> for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract String getTrackingUrl();
  
  /**
   * Set the <em>tracking URL</em>for the <code>ApplicationMaster</code> while
   * it is running. This is the web-URL to which ResourceManager or
   * web-application proxy will redirect client/users while the application and
   * the <code>ApplicationMaster</code> are still running.
   * <p>
   * If the passed url has a scheme then that will be used by the
   * ResourceManager and web-application proxy, otherwise the scheme will
   * default to http.
   * </p>
   * <p>
   * Empty, null, "N/A" strings are all valid besides a real URL. In case an url
   * isn't explicitly passed, it defaults to "N/A" on the ResourceManager.
   * <p>
   *
   * @param trackingUrl
   *          <em>tracking URL</em>for the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract void setTrackingUrl(String trackingUrl);

  /**
   * Return all Placement Constraints specified at the Application level. The
   * mapping is from a set of allocation tags to a
   * <code>PlacementConstraint</code> associated with the tags, i.e., each
   * {@link org.apache.hadoop.yarn.api.records.SchedulingRequest} that has those
   * tags will be placed taking into account the corresponding constraint.
   *
   * @return A map of Placement Constraints.
   */
  @Public
  @Unstable
  public Map<Set<String>, PlacementConstraint> getPlacementConstraints() {
    return new HashMap<>();
  }

  /**
   * Set Placement Constraints applicable to the
   * {@link org.apache.hadoop.yarn.api.records.SchedulingRequest}s
   * of this application.
   * The mapping is from a set of allocation tags to a
   * <code>PlacementConstraint</code> associated with the tags.
   * For example:
   *  Map &lt;
   *   &lt;hb_regionserver&gt; -&gt; node_anti_affinity,
   *   &lt;hb_regionserver, hb_master&gt; -&gt; rack_affinity,
   *   ...
   *  &gt;
   * @param placementConstraints Placement Constraint Mapping.
   */
  @Public
  @Unstable
  public void setPlacementConstraints(
      Map<Set<String>, PlacementConstraint> placementConstraints) {
  }
}
