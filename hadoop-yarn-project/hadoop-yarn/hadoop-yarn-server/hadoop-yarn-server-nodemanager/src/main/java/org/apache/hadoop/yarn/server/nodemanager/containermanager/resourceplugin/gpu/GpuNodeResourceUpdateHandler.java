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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

public class GpuNodeResourceUpdateHandler extends NodeResourceUpdaterPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(GpuNodeResourceUpdateHandler.class);

  @Override
  public void updateConfiguredResource(Resource res) throws YarnException {
    LOG.info("Initializing configured GPU resources for the NodeManager.");

    List<GpuDevice> usableGpus =
        GpuDiscoverer.getInstance().getGpusUsableByYarn();
    if (null == usableGpus || usableGpus.isEmpty()) {
      String message = "GPU is enabled, but couldn't find any usable GPUs on the "
          + "NodeManager.";
      LOG.error(message);
      // No gpu can be used by YARN.
      throw new YarnException(message);
    }

    long nUsableGpus = usableGpus.size();

    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    if (!configuredResourceTypes.containsKey(GPU_URI)) {
      throw new YarnException("Found " + nUsableGpus + " usable GPUs, however "
          + GPU_URI
          + " resource-type is not configured inside"
          + " resource-types.xml, please configure it to enable GPU feature or"
          + " remove " + GPU_URI + " from "
          + YarnConfiguration.NM_RESOURCE_PLUGINS);
    }

    res.setResourceValue(GPU_URI, nUsableGpus);
  }
}
