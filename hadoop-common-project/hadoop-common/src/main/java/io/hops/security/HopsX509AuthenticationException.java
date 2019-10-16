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
package io.hops.security;

import java.io.IOException;

public class HopsX509AuthenticationException extends IOException {
  public HopsX509AuthenticationException() {
    super();
  }
  
  public HopsX509AuthenticationException(String message) {
    super(message);
  }
  
  public HopsX509AuthenticationException(Throwable throwable) {
    super(throwable);
  }
  
  public HopsX509AuthenticationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
