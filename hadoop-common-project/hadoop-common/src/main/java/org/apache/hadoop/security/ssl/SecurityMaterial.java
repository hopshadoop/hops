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
package org.apache.hadoop.security.ssl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class SecurityMaterial {
  public enum STATE {
    NEW,
    ONGOING,
    FINISHED
  }
  
  private final Path certFolder;
  private final AtomicBoolean tombstone;
  
  private STATE state;
  
  // Number of applications using the same crypto material
  // The same user might have multiple applications running
  // at the same time
  private int requestedApplications;
  
  public SecurityMaterial(Path certFolder) {
    this.certFolder = certFolder;
    
    requestedApplications = 1;
    
    state = STATE.NEW;
    tombstone = new AtomicBoolean(false);
  }

  public Path getCertFolder() { return certFolder; }
  
  public int getRequestedApplications() {
    return requestedApplications;
  }
  
  public void incrementRequestedApplications() {
    requestedApplications++;
  }
  
  public void decrementRequestedApplications() {
    requestedApplications--;
  }
  
  public boolean isSafeToRemove() {
    return requestedApplications == 0;
  }
  
  public synchronized void changeState(STATE state) {
    this.state = state;
  }
  
  public synchronized STATE getState() {
    return state;
  }
  
  public boolean hasBeenCanceled() {
    return tombstone.get();
  }
  
  public synchronized boolean tryToCancel() {
    if (state.equals(STATE.NEW)) {
      tombstone.set(true);
      return true;
    }
    return false;
  }
}
