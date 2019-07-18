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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.ServletContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;

/**
 * Encapsulates the HTTP server started by the NameNode.
 */
@InterfaceAudience.Private
public class NameNodeHttpServer {
  private HttpServer2 httpServer;
  private final Configuration conf;
  private final NameNode nn;
  
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private InetSocketAddress bindAddress;
  
  public static final String NAMENODE_ADDRESS_ATTRIBUTE_KEY =
      "name.node.address";
  public static final String FSIMAGE_ATTRIBUTE_KEY = "name.system.image";
  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";
  public static final String STARTUP_PROGRESS_ATTRIBUTE_KEY = "startup.progress";
  
  NameNodeHttpServer(Configuration conf, NameNode nn,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.nn = nn;
    this.bindAddress = bindAddress;
  }
  
  private void initWebHdfs(Configuration conf) throws IOException {
    if (WebHdfsFileSystem.isEnabled(conf)) {
      UserParam.setUserPattern(conf.get(
          DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
          DFSConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));

      // add authentication filter for webhdfs
      final String className = conf.get(
          DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_KEY,
          DFSConfigKeys.DFS_WEBHDFS_AUTHENTICATION_FILTER_DEFAULT);
      final String name = className;

      final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
      Map<String, String> params = getAuthFilterParams(conf);
      HttpServer2.defineFilter(httpServer.getWebAppContext(), name, className,
          params, new String[] { pathSpec });
      HttpServer2.LOG.info("Added filter '" + name + "' (class=" + className
          + ")");

      // add webhdfs packages
      httpServer.addJerseyResourcePackage(NamenodeWebHdfsMethods.class
          .getPackage().getName() + ";" + Param.class.getPackage().getName(),
          pathSpec);
    }
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  void start() throws IOException {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    final String infoHost = bindAddress.getHostName();
    
    final InetSocketAddress httpAddr = bindAddress;
    final String httpsAddrString = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      // If DFS_NAMENODE_HTTPS_BIND_HOST_KEY exists then it overrides the
      // host name portion of DFS_NAMENODE_HTTPS_ADDRESS_KEY.
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }
    
    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        httpAddr, httpsAddr, "hdfs",
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);

    httpServer = builder.build();
    
    if (policy.isHttpsEnabled()) {
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.getTrimmed(
          DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, infoHost + ":"
              + DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT));
      httpServer.setAttribute(DFSConfigKeys.DFS_DATANODE_HTTPS_PORT_KEY,
          datanodeSslPort.getPort());
    }

    initWebHdfs(conf);

    httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, nn);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    setupServlets(httpServer, conf);
    httpServer.start();
    
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  private Map<String, String> getAuthFilterParams(Configuration conf)
      throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    String principalInConf = conf
        .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params
          .put(
              DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
              SecurityUtil.getServerPrincipal(principalInConf,
                  bindAddress.getHostName()));
    } else if (UserGroupInformation.isSecurityEnabled()) {
      HttpServer2.LOG.error(
          "WebHDFS and security are enabled, but configuration property '"
          + DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY + "' is not set.");
    }
    String httpKeytab = conf.get(DFSUtil.getSpnegoKeytabKey(conf,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put(
          DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
          httpKeytab);
    } else if (UserGroupInformation.isSecurityEnabled()) {
      HttpServer2.LOG.error(
          "WebHDFS and security are enabled, but configuration property '"
          + DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY + "' is not set.");
    }
    String anonymousAllowed = conf
      .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED);
    if (anonymousAllowed != null && !anonymousAllowed.isEmpty()) {
    params.put(
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_SIMPLE_ANONYMOUS_ALLOWED,
        anonymousAllowed);
    }
    return params;
  }

  void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }
  
  /**
   * Sets address of namenode for use by servlets.
   * 
   * @param nameNodeAddress InetSocketAddress to set
   */
  void setNameNodeAddress(InetSocketAddress nameNodeAddress) {
    httpServer.setAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY,
        NetUtils.getConnectAddress(nameNodeAddress));
  }

  /**
   * Sets startup progress of namenode for use by servlets.
   * 
   * @param prog StartupProgress to set
   */
  void setStartupProgress(StartupProgress prog) {
    httpServer.setAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY, prog);
  }

  private static void setupServlets(HttpServer2 httpServer, Configuration conf) {
    httpServer.addInternalServlet("startupProgress",
        StartupProgressServlet.PATH_SPEC, StartupProgressServlet.class);
    httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
        true);
  }

  public static NameNode getNameNodeFromContext(ServletContext context) {
    return (NameNode) context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
  }

  static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }

  public static InetSocketAddress getNameNodeAddressFromContext(
      ServletContext context) {
    return (InetSocketAddress) context
        .getAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY);
  }
  
  /**
   * Returns StartupProgress associated with ServletContext.
   * 
   * @param context ServletContext to get
   * @return StartupProgress associated with context
   */
  static StartupProgress getStartupProgressFromContext(
      ServletContext context) {
    return (StartupProgress)context.getAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY);
  }
}
