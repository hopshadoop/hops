/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.resolvingcache;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MemcachedClientPool {

  private final List<MemcachedClient> clients;
  private int index;

  public MemcachedClientPool(int poolSize, String server) throws IOException {
    this.index = 0;
    this.clients = new ArrayList<MemcachedClient>();
    for (int i = 0; i < poolSize; i++) {
      this.clients.add(new MemcachedClient(new BinaryConnectionFactory(),
          AddrUtil.getAddresses(server.trim())));
    }
  }

  public MemcachedClient poll() {
    MemcachedClient client = clients.get(index++ % clients.size());
    if (!client.getAvailableServers().isEmpty()) {
      return client;
    }
    return null;
  }

  public void shutdown() {
    for (MemcachedClient client : clients) {
      client.shutdown();
    }
  }
}
