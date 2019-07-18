/*
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

package org.apache.hadoop.registry.server.integration;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.server.services.RegistryAdminService;

/**
 * Select an entry by the YARN persistence policy
 */
public class SelectByYarnPersistence
    implements RegistryAdminService.NodeSelector {
  private final String id;
  private final String targetPolicy;

  public SelectByYarnPersistence(String id, String targetPolicy) {
    Preconditions.checkArgument(!StringUtils.isEmpty(id), "id");
    Preconditions.checkArgument(!StringUtils.isEmpty(targetPolicy),
        "targetPolicy");
    this.id = id;
    this.targetPolicy = targetPolicy;
  }

  @Override
  public boolean shouldSelect(String path,
      RegistryPathStatus registryPathStatus,
      ServiceRecord serviceRecord) {
    String policy =
        serviceRecord.get(YarnRegistryAttributes.YARN_PERSISTENCE, "");
    return id.equals(serviceRecord.get(YarnRegistryAttributes.YARN_ID, ""))
           && (targetPolicy.equals(policy));
  }

  @Override
  public String toString() {
    return String.format(
        "Select by ID %s and policy %s: {}",
        id, targetPolicy);
  }
}
