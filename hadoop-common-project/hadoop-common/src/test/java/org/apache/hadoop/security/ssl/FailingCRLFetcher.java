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

import java.io.IOException;
import java.net.MalformedURLException;

public class FailingCRLFetcher extends CRLFetcherAbstr {
  
  int successfulFetches = 0;
  
  @Override
  public void fetch() throws MalformedURLException, IOException {
    if (successfulFetches > 1) {
      throw new IOException("Doomed to fail");
    }
    successfulFetches++;
  }
}
