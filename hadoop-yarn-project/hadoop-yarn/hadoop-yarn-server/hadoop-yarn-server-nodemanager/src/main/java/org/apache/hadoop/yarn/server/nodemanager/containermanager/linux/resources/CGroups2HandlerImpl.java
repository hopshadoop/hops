/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CGroups2HandlerImpl extends BaseCGroupsHandler implements CGroupsHandler {
  private static final String CGROUP_PROCS_FILE = "cgroup.procs";

  CGroups2HandlerImpl(Configuration conf) {
    super(conf);
  }

  @Override
  String getProcessesFilename() {
    return CGROUP_PROCS_FILE;
  }

  @Override
  public String getPathForCGroup(CGroupController controller, String cGroupId) {
    return Paths.get(cGroupMountPath, cGroupPrefix, cGroupId).toString();
  }

  @Override
  public void initializeCGroupController(CGroupController controller) throws ResourceHandlerException {
    File rootHierarchy = new File(cGroupMountPath);
    if (!rootHierarchy.exists()) {
      throw new ResourceHandlerException("Cgroup2 root hierarchy " + rootHierarchy + " does not exist");
    }
    Path yarnHierarchy = Paths.get(cGroupMountPath, cGroupPrefix);
    if (!yarnHierarchy.toFile().exists()) {
      LOG.info("Yarn hierarchy does not exist. Creating " + yarnHierarchy);
      try {
        if (!yarnHierarchy.toFile().mkdirs()) {
          throw new ResourceHandlerException("Cannot create Yarn hierarchy " + yarnHierarchy);
        }
      } catch (SecurityException ex) {
        throw new ResourceHandlerException("No permission to create " + yarnHierarchy, ex);
      }
    } else if (!FileUtil.canWrite(yarnHierarchy.toFile())) {
      throw new ResourceHandlerException("Yarn hierarchy " + yarnHierarchy + " exists but it is not writable");
    }
  }


  @Override
  public String getControllerPath(CGroupController controller) {
    return Paths.get(cGroupMountPath, cGroupPrefix).toString();
  }
}