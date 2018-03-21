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
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Class that fetches a CRL from a remote (or local) location.
 */
public class RemoteCRLFetcher extends CRLFetcherAbstr {
  
  public RemoteCRLFetcher() {
  }
  
  /**
   * Fetch a CRL from a configured input URI and write it to the local filesystem. The URI typically would be a URL
   * to a web server serving the CRL
   * @throws MalformedURLException
   * @throws IOException
   */
  @Override
  public void fetch() throws MalformedURLException, IOException {
      URL url = inputURI.toURL();
      ReadableByteChannel channel = Channels.newChannel(url.openStream());
      writeToFile(channel);
      if (channel.isOpen()) {
        channel.close();
      }
  }
}
