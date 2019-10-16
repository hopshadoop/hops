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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import com.google.common.base.Preconditions;
import io.hops.common.security.FsSecurityActions;
import io.hops.common.security.HopsworksFsSecurityActions;
import io.hops.security.CertificateLocalizationCtx;
import io.hops.security.HopsSecurityActionsFactory;
import io.hops.security.HopsX509AuthenticationException;
import io.hops.security.HopsX509Authenticator;
import io.hops.security.HopsX509AuthenticatorFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

public class HopsWebHdfsHandler extends WebHdfsHandler {
  private final HopsX509Authenticator authenticator;
  private final FsSecurityActions fsSecurityActions;
  
  public HopsWebHdfsHandler(Configuration conf, Configuration confForCreate)
      throws Exception {
    super(conf, confForCreate);
    authenticator = HopsX509AuthenticatorFactory.getInstance(conf).getAuthenticator();
    fsSecurityActions = (FsSecurityActions) HopsSecurityActionsFactory.getInstance().getActor(conf,
        conf.get(DFSConfigKeys.FS_SECURITY_ACTIONS_ACTOR_KEY, DFSConfigKeys.DEFAULT_FS_SECURITY_ACTIONS_ACTOR));
  }
  
  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final HttpRequest req) throws Exception {
    Preconditions.checkArgument(req.getUri().startsWith(WEBHDFS_PREFIX));
    extractParams(req);
    buildUGI();
    injectToken();
    InetAddress remoteAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getAddress();
    X509Certificate clientCertificate = extractUserX509(ctx);
    // Do auth
    authenticator.authenticateConnection(ugi, clientCertificate, remoteAddress, "WebHDFS");
    localizeUserX509Material();
    doHandle(ctx, req);
  }
  
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    super.exceptionCaught(ctx, cause);
    try {
      CertificateLocalizationCtx.getInstance().getCertificateLocalization().removeX509Material(ugi.getUserName());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
  
  private void localizeUserX509Material()
      throws URISyntaxException, GeneralSecurityException, IOException, InterruptedException {
    HopsworksFsSecurityActions.X509CredentialsDTO credentials = fsSecurityActions.getX509Credentials(ugi.getUserName());
    if (!"jks".equals(credentials.getFileExtension())) {
      throw new IOException("Unknown X.509 format: " + credentials.getFileExtension());
    }
    ByteBuffer keyStore = ByteBuffer.wrap(Base64.decodeBase64(credentials.getkStore()));
    ByteBuffer trustStore = ByteBuffer.wrap(Base64.decodeBase64(credentials.gettStore()));
    String password = credentials.getPassword();
    
    CertificateLocalizationCtx.getInstance().getCertificateLocalization()
        .materializeCertificates(ugi.getUserName(), ugi.getUserName(), keyStore, password,
            trustStore, password);
  }
  
  private X509Certificate extractUserX509(final ChannelHandlerContext ctx) throws HopsX509AuthenticationException {
    SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
    if (sslHandler == null) {
      throw new HopsX509AuthenticationException("Could not get SSLHandler from pipeline");
    }
    // The first certificate is always the peer's own certificate
    // https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLSession.html#getPeerCertificates--
    try {
      return (X509Certificate) sslHandler.engine().getSession().getPeerCertificates()[0];
    } catch (SSLPeerUnverifiedException ex) {
      throw new HopsX509AuthenticationException(ex);
    }
  }
}
