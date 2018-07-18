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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

/**
 * A FailoverProxyProvider implementation which allows one to configure two URIs
 * to connect to during fail-over. The first configured address is tried first,
 * and on a fail-over event the other address is tried.
 */
public class HopsLeaderFailoverProxyProvider<T> extends HopsRandomStickyFailoverProxyProvider<T> {

  public static final Log LOG =
          LogFactory.getLog(HopsLeaderFailoverProxyProvider.class);

  public HopsLeaderFailoverProxyProvider(Configuration conf, URI uri, Class xface) {
    super(conf, uri, xface);
    //override the random proxy and select the leader proxy which is at index 0
    super.currentProxyIndex = 0;
    LOG.debug(name+" index is set to: "+currentProxyIndex);
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    super.performFailover(currentProxy); // this is update the proxies and select and rondom proxy

    //override the random proxy and select the leader proxy which is at index 0
    super.currentProxyIndex = 0;
    LOG.debug(name+" failover has occured. Index overridden " +
            "and set to 0");
  }
}
