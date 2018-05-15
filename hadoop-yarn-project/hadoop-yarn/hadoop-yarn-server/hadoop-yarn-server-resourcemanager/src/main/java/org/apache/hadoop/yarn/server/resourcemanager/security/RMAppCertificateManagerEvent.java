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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMAppCertificateManagerEvent extends AbstractEvent<RMAppCertificateManagerEventType> {
  private final ApplicationId applicationId;
  private final String applicationUser;
  private final Integer cryptoMaterialVersion;
  
  public RMAppCertificateManagerEvent(
      ApplicationId applicationId, String applicationUser, Integer cryptoMaterialVersion,
      RMAppCertificateManagerEventType rmAppCertificateManagerEventType) {
    super(rmAppCertificateManagerEventType);
    this.applicationId = applicationId;
    this.applicationUser = applicationUser;
    this.cryptoMaterialVersion = cryptoMaterialVersion;
  }
  
  public ApplicationId getApplicationId() {
    return applicationId;
  }
  
  public String getApplicationUser() {
    return applicationUser;
  }
  
  public Integer getCryptoMaterialVersion() {
    return cryptoMaterialVersion;
  }
}
