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
package io.hops.merge;

import io.hops.security.HopsFileBasedKeyStoresFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

public class SecurityUtil2 extends SecurityUtil {

  private static SSLFactory sslFactory;

  static {
    Configuration conf = new Configuration();
    if (HttpConfig2.isSecure()) {
      if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
              CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
        conf.set(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, HopsFileBasedKeyStoresFactory.class.getCanonicalName());
      }
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
      try {
        sslFactory.init();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Open a (if need be) secure connection to a URL in a secure environment
   * that is using SPNEGO to authenticate its URLs. All Namenode and Secondary
   * Namenode URLs that are protected via SPNEGO should be accessed via this
   * method.
   *
   * @param url
   *     to authenticate via SPNEGO.
   * @return A connection that has been authenticated via SPNEGO
   * @throws IOException
   *     If unable to authenticate via SPNEGO
   */
  public static URLConnection openSecureHttpConnection(URL url)
      throws IOException {
    if (!HttpConfig2.isSecure() && !UserGroupInformation.isSecurityEnabled()) {
      return url.openConnection();
    }

    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    try {
      return new AuthenticatedURL(null, sslFactory).openConnection(url, token);
    } catch (AuthenticationException e) {
      throw new IOException(
          "Exception trying to open authenticated connection to " + url, e);
    }
  }
}