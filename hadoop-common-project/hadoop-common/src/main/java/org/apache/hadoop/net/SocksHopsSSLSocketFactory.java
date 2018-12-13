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
package org.apache.hadoop.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SOCKS_SERVER_KEY;

public class SocksHopsSSLSocketFactory extends HopsSSLSocketFactory {
  private static final Log LOG = LogFactory.getLog(SocksHopsSSLSocketFactory.class);
  
  private Proxy proxy;
  
  public SocksHopsSSLSocketFactory() {
    super();
    this.proxy = Proxy.NO_PROXY;
  }
  
  public SocksHopsSSLSocketFactory(Proxy proxy) {
    this.proxy = proxy;
  }
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String proxyStr = conf.get(HADOOP_SOCKS_SERVER_KEY);
    if ((proxyStr != null) && (proxyStr.length() > 0)) {
      setProxy(proxyStr);
    }
  }
  
  @Override
  public Configuration getConf() {
    return super.getConf();
  }
  
  @Override
  public Socket createSocket() throws IOException, UnknownHostException {
    LOG.debug("Creating SOCKS Hops SSL socket");
    Socket socks = new Socket(proxy);
    InetSocketAddress proxyAddress = (InetSocketAddress) proxy.address();
    if (getConf().getBoolean(FORCE_CONFIGURE, false)) {
      setConf(getConf());
    }
    SSLContext sslCtx = initializeSSLContext();
    SSLSocketFactory socketFactory = sslCtx.getSocketFactory();
    return socketFactory.createSocket(socks, proxyAddress.getHostName(), proxyAddress.getPort(), true);
  }
  
  
  /**
   * Set the proxy of this socket factory as described in the string
   * parameter
   *
   * @param proxyStr the proxy address using the format "host:port"
   */
  private void setProxy(String proxyStr) {
    String[] strs = proxyStr.split(":", 2);
    if (strs.length != 2)
      throw new RuntimeException("Bad SOCKS proxy parameter: " + proxyStr);
    String host = strs[0];
    int port = Integer.parseInt(strs[1]);
    this.proxy =
        new Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved(host,
            port));
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof SocksHopsSSLSocketFactory) {
      boolean superEquals = super.equals(obj);
      SocksHopsSSLSocketFactory other = (SocksHopsSSLSocketFactory) obj;
      if (proxy == null) {
        if (other.proxy != null) {
          return false;
        } else {
          return superEquals;
        }
      } else {
        if (other.proxy == null) {
          return false;
        } else {
          return superEquals && proxy.equals(other.proxy);
        }
      }
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 37 * result + proxy.hashCode();
    return result;
  }
}
