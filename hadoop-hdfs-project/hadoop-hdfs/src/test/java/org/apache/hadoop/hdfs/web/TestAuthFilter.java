/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestAuthFilter {
  
  private static class DummyFilterConfig implements FilterConfig {
    final Map<String, String> map;
    
    DummyFilterConfig(Map<String, String> map) {
      this.map = map;
    }
    
    @Override
    public String getFilterName() {
      return "dummy";
    }

    @Override
    public String getInitParameter(String arg0) {
      return map.get(arg0);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
      return Collections.enumeration(map.keySet());
    }

    @Override
    public ServletContext getServletContext() {
      return null;
    }
  }
  
  @Test
  public void testGetConfiguration() throws ServletException {
    AuthFilter filter = new AuthFilter();
    Map<String, String> m = new HashMap<>();
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "xyz/thehost@REALM");
    m.put(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        "thekeytab");
    FilterConfig config = new DummyFilterConfig(m);
    Properties p = filter.getConfiguration("random", config);
    Assert
        .assertEquals("xyz/thehost@REALM", p.getProperty("kerberos.principal"));
    Assert.assertEquals("thekeytab", p.getProperty("kerberos.keytab"));
    Assert.assertEquals("true",
        p.getProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED));
  }
}
