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
package io.hops.leader_election.watchdog;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;

public class TestHttpPageNotFoundPoller {

  @Test
  public void testEmptyEndpoint() {
    Configuration conf = new Configuration(true);
    HttpPageNotFoundPoller poller = new HttpPageNotFoundPoller();
    poller.setConf(conf);
    Exception exception = Assert.assertThrows(IllegalArgumentException.class, poller::init);
    Assert.assertTrue(exception.getMessage().startsWith("Alive watchdog HTTP poller url is empty"));
  }

  @Test
  public void testNonValidUrl() {
    Configuration conf = new Configuration(true);
    conf.set(HttpUtils.POLL_ENDPOINT, "http://");
    HttpPageNotFoundPoller poller = new HttpPageNotFoundPoller();
    poller.setConf(conf);
    Assert.assertThrows(URISyntaxException.class, poller::init);
  }

  @Test
  public void testAlive() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      MockResponse response = new MockResponse().setResponseCode(200);
      server.enqueue(response);
      HttpPageNotFoundPoller poller = new HttpPageNotFoundPoller();
      Configuration conf = new Configuration(true);
      conf.set(HttpUtils.POLL_ENDPOINT, "http://" + server.getHostName() + ":" + server.getPort());
      poller.setConf(conf);
      poller.init();
      Assert.assertTrue(poller.shouldIBeAlive());
      poller.destroy();
    }
  }

  @Test
  public void testNotAlive() throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      MockResponse response = new MockResponse().setResponseCode(404);
      server.enqueue(response);
      HttpPageNotFoundPoller poller = new HttpPageNotFoundPoller();
      Configuration conf = new Configuration(true);
      conf.set(HttpUtils.POLL_ENDPOINT, "http://" + server.getHostName() + ":" + server.getPort());
      poller.setConf(conf);
      poller.init();
      Assert.assertFalse(poller.shouldIBeAlive());
      poller.destroy();
    }
  }
}
