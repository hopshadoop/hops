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

public class JWTSecurityMaterial extends SecurityMaterial {
  public static final String JWT_LOCAL_RESOURCE_FILE = "token.jwt";
  public static final String JWT_FILE_SUFFIX = "_token.jwt";
  
  private final Path tokenLocation;
  private String token;
  
  public JWTSecurityMaterial(Path certFolder, Path tokenLocation, String token) {
    super(certFolder);
    this.tokenLocation = tokenLocation;
    this.token = token;
  }
  
  public Path getTokenLocation() {
    return tokenLocation;
  }
  
  public String getToken() {
    return token;
  }
  
  public synchronized void updateToken(String token) {
    this.token = token;
  }
}
