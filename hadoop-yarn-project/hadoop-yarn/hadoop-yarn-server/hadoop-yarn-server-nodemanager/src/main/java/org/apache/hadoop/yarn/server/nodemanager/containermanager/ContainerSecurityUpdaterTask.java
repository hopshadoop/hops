/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.TimeUnit;

abstract class ContainerSecurityUpdaterTask implements Runnable {
  private static final Log LOG = LogFactory.getLog(ContainerSecurityUpdaterTask.class);
  protected final ContainerImpl container;
  private final BackOff backOff;
  private long backOffTime;
  
  ContainerSecurityUpdaterTask(ContainerImpl container) {
    this.container = container;
    this.backOff = createBackOffPolicy();
    this.backOffTime = 0L;
  }
  
  protected abstract void removeSecurityUpdaterTask();
  protected abstract void scheduleSecurityUpdaterTask();
  protected abstract void updateStateStore() throws IOException;
  protected abstract void execute() throws IOException;
  
  @Override
  public void run() {
    try {
      TimeUnit.MILLISECONDS.sleep(backOffTime);
      if (!isContainerStillRunning()) {
        removeSecurityUpdaterTask();
        return;
      }
      execute();
      updateStateStore();
      removeSecurityUpdaterTask();
      LOG.debug("Updated security material for container: " + container.getContainerId());
    } catch (IOException ex) {
      LOG.error(ex, ex);
      removeSecurityUpdaterTask();
      backOffTime = backOff.getBackOffInMillis();
      if (backOffTime != -1) {
        LOG.warn("Re-scheduling updating security material for container " + container.getContainerId() + " after "
            + backOffTime + "ms");
        scheduleSecurityUpdaterTask();
      } else {
        LOG.error("Reached maximum number of retries for container " + container.getContainerId() + ", giving up",
            ex);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
  
  protected void writeByteBufferToFile(File target, ByteBuffer data) throws IOException {
    Set<PosixFilePermission> permissions = null;
    Path targetPath = target.toPath();
    if (!target.canWrite()) {
      permissions = addOwnerWritePermission(targetPath);
    }
    FileChannel fileChannel = new FileOutputStream(target, false).getChannel();
    fileChannel.write(data);
    fileChannel.close();
    if (permissions != null) {
      removeOwnerWritePermission(targetPath, permissions);
    }
  }
  
  protected void writeStringToFile(File target, String data) throws IOException {
    Set<PosixFilePermission> permissions = null;
    Path targetPath = target.toPath();
    if (!target.canWrite()) {
      permissions = addOwnerWritePermission(targetPath);
    }
    FileUtils.writeStringToFile(target, data);
    if (permissions != null) {
      removeOwnerWritePermission(targetPath, permissions);
    }
  }
  
  protected Set<PosixFilePermission> addOwnerWritePermission(Path target) throws IOException {
    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(target);
    if (permissions.add(PosixFilePermission.OWNER_WRITE)) {
      Files.setPosixFilePermissions(target, permissions);
    }
    return permissions;
  }
  
  protected void removeOwnerWritePermission(Path target, Set<PosixFilePermission> permissions) throws IOException {
    if (permissions.remove(PosixFilePermission.OWNER_WRITE)) {
      Files.setPosixFilePermissions(target, permissions);
    }
  }
  
  protected boolean isContainerStillRunning() {
    boolean running = container.getContainerState().equals(
        org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING);
    if (!running) {
      LOG.info("Crypto updater for container " + container.getContainerId() + " run but the container is not in " +
          "RUNNING state, instead state is: " + container.getContainerState());
    }
    return running;
  }
  
  private BackOff createBackOffPolicy() {
    return new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(200)
        .setMaximumIntervalMillis(5000)
        .setMultiplier(1.4)
        .setMaximumRetries(6)
        .build();
  }
}
