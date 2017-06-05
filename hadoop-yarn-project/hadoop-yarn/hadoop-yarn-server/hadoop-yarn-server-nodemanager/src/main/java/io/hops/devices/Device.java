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
package io.hops.devices;

import java.util.Objects;

public class Device {
  
  private int majorDeviceNumber;
  private int minorDeviceNumber;
  
  public Device(int majorDeviceNumber, int minorDeviceNumber) {
    this.majorDeviceNumber = majorDeviceNumber;
    this.minorDeviceNumber = minorDeviceNumber;
  }
  
  public boolean equals(Object otherObj) {
    Device otherGpu = (Device)otherObj;
    if(this.getMajorDeviceNumber() == otherGpu.getMajorDeviceNumber()
        && this.getMinorDeviceNumber() == otherGpu.getMinorDeviceNumber()) {
      return true;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hash(minorDeviceNumber, majorDeviceNumber);
  }
  
  public int getMajorDeviceNumber() {
    return majorDeviceNumber;
  }
  
  public int getMinorDeviceNumber() {
    return minorDeviceNumber;
  }
  
  public String toString() {
    return majorDeviceNumber + ":" + minorDeviceNumber;
  }
  
}
