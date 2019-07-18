/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DockerRunCommand extends DockerCommand {
  private static final String RUN_COMMAND = "run";
  private final Map<String, String> userEnv;

  /** The following are mandatory: */
  public DockerRunCommand(String containerId, String user, String image) {
    super(RUN_COMMAND);
    super.addCommandArguments("name", containerId);
    super.addCommandArguments("user", user);
    super.addCommandArguments("image", image);
    this.userEnv = new LinkedHashMap<String, String>();
  }

  public DockerRunCommand removeContainerOnExit() {
    super.addCommandArguments("rm", "true");
    return this;
  }

  public DockerRunCommand detachOnRun() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  public DockerRunCommand setContainerWorkDir(String workdir) {
    super.addCommandArguments("workdir", workdir);
    return this;
  }

  public DockerRunCommand setNetworkType(String type) {
    super.addCommandArguments("net", type);
    return this;
  }

  public DockerRunCommand setPidNamespace(String type) {
    super.addCommandArguments("pid", type);
    return this;
  }

  public DockerRunCommand addMountLocation(String sourcePath, String
      destinationPath, String mode) {
    super.addCommandArguments("mounts", sourcePath + ":" +
        destinationPath + ":" + mode);
    return this;
  }

  public DockerRunCommand addReadWriteMountLocation(String sourcePath, String
      destinationPath) {
    return addMountLocation(sourcePath, destinationPath, "rw");
  }

  public DockerRunCommand addAllReadWriteMountLocations(List<String> paths) {
    for (String dir: paths) {
      this.addReadWriteMountLocation(dir, dir);
    }
    return this;
  }

  public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String
      destinationPath, boolean createSource) {
    boolean sourceExists = new File(sourcePath).exists();
    if (!sourceExists && !createSource) {
      return this;
    }
    return addReadOnlyMountLocation(sourcePath, destinationPath);
  }

  public DockerRunCommand addReadOnlyMountLocation(String sourcePath, String
      destinationPath) {
    return addMountLocation(sourcePath, destinationPath, "ro");
  }

  public DockerRunCommand addAllReadOnlyMountLocations(List<String> paths) {
    for (String dir: paths) {
      this.addReadOnlyMountLocation(dir, dir);
    }
    return this;
  }

  public DockerRunCommand addTmpfsMount(String mount) {
    super.addCommandArguments("tmpfs", mount);
    return this;
  }

  public DockerRunCommand setVolumeDriver(String volumeDriver) {
    super.addCommandArguments("volume-driver", volumeDriver);
    return this;
  }

  public DockerRunCommand setCGroupParent(String parentPath) {
    super.addCommandArguments("cgroup-parent", parentPath);
    return this;
  }

  /* Run a privileged container. Use with extreme care */
  public DockerRunCommand setPrivileged() {
    super.addCommandArguments("privileged", "true");
    return this;
  }

  public DockerRunCommand setCapabilities(Set<String> capabilties) {
    //first, drop all capabilities
    super.addCommandArguments("cap-drop", "ALL");

    //now, add the capabilities supplied
    for (String capability : capabilties) {
      super.addCommandArguments("cap-add", capability);
    }

    return this;
  }

  public DockerRunCommand setHostname(String hostname) {
    super.addCommandArguments("hostname", hostname);
    return this;
  }

  public DockerRunCommand addDevice(String sourceDevice, String
      destinationDevice) {
    super.addCommandArguments("devices", sourceDevice + ":" +
        destinationDevice);
    return this;
  }

  public DockerRunCommand enableDetach() {
    super.addCommandArguments("detach", "true");
    return this;
  }

  public DockerRunCommand disableDetach() {
    super.addCommandArguments("detach", "false");
    return this;
  }

  public DockerRunCommand addRuntime(String runtime) {
    super.addCommandArguments("runtime", runtime);
    return this;
  }

  public DockerRunCommand groupAdd(String[] groups) {
    super.addCommandArguments("group-add", String.join(",", groups));
    return this;
  }

  public DockerRunCommand setOverrideCommandWithArgs(
      List<String> overrideCommandWithArgs) {
    for(String override: overrideCommandWithArgs) {
      super.addCommandArguments("launch-command", override);
    }
    return this;
  }

  @Override
  public Map<String, List<String>> getDockerCommandWithArguments() {
    return super.getDockerCommandWithArguments();
  }

  public DockerRunCommand setOverrideDisabled(boolean toggle) {
    String value = Boolean.toString(toggle);
    super.addCommandArguments("use-entry-point", value);
    return this;
  }

  public DockerRunCommand setLogDir(String logDir) {
    super.addCommandArguments("log-dir", logDir);
    return this;
  }

  /**
   * Check if user defined environment variables are empty.
   *
   * @return true if user defined environment variables are not empty.
   */
  public boolean containsEnv() {
    if (userEnv.size() > 0) {
      return true;
    }
    return false;
  }

  /**
   * Get user defined environment variables.
   *
   * @return a map of user defined environment variables
   */
  public Map<String, String> getEnv() {
    return userEnv;
  }

  /**
   * Add user defined environment variables.
   *
   * @param environment A map of user defined environment variables
   */
  public final void addEnv(Map<String, String> environment) {
    userEnv.putAll(environment);
  }
}
