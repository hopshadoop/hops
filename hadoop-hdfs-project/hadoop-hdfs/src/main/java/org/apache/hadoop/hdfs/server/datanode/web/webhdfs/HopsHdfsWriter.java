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

import io.hops.security.CertificateLocalizationCtx;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;

public class HopsHdfsWriter extends HdfsWriter {
  private final Configuration conf;
  private final UserGroupInformation ugi;
  
  HopsHdfsWriter(DFSClient client, OutputStream out, DefaultHttpResponse response, Configuration conf,
      UserGroupInformation ugi) {
    super(client, out, response);
    this.conf = conf;
    this.ugi = ugi;
  }
  
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpContent chunk)
      throws IOException {
    super.channelRead0(ctx, chunk);
    removeX509Credentials();
  }
  
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    super.exceptionCaught(ctx, cause);
    try {
      CertificateLocalizationCtx.getInstance().getCertificateLocalization()
          .removeX509Material(ugi.getUserName());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
  
  private void removeX509Credentials() throws IOException {
    if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      try {
        CertificateLocalizationCtx.getInstance().getCertificateLocalization()
            .removeX509Material(ugi.getUserName());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("CertificateLocalizationService interrupted while removing X.509 material");
      }
    }
  }
}
