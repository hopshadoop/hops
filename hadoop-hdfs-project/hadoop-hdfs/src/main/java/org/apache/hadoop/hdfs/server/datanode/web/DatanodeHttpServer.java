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
package org.apache.hadoop.hdfs.server.datanode.web;

import io.hops.security.HopsFileBasedKeyStoresFactory;
import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;

public class DatanodeHttpServer implements Closeable {
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final ServerSocketChannel externalHttpChannel;
  private final ServerBootstrap httpServer;
  private final SSLFactory sslFactory;
  private final ServerBootstrap httpsServer;
  private final Configuration conf;
  private final Configuration confForCreate;
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  static final Log LOG = LogFactory.getLog(DatanodeHttpServer.class);

  public DatanodeHttpServer(final Configuration conf, final InetSocketAddress
    jettyAddr, final ServerSocketChannel externalHttpChannel)
    throws IOException {
    this.conf = conf;
    this.confForCreate = new Configuration(conf);
    confForCreate.set(FsPermission.UMASK_LABEL, "000");

    this.bossGroup = new NioEventLoopGroup();
    this.workerGroup = new NioEventLoopGroup();
    this.externalHttpChannel = externalHttpChannel;
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);

    if (policy.isHttpEnabled()) {
      this.httpServer = new ServerBootstrap().group(bossGroup, workerGroup)
        .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new HttpRequestDecoder(),
            new HttpResponseEncoder(),
            new ChunkedWriteHandler(),
            new URLDispatcher(jettyAddr, conf, confForCreate));
        }
      });
      if (externalHttpChannel == null) {
        httpServer.channel(NioServerSocketChannel.class);
      } else {
        httpServer.channelFactory(new ChannelFactory<NioServerSocketChannel>() {
          @Override
          public NioServerSocketChannel newChannel() {
            return new NioServerSocketChannel(externalHttpChannel) {
              // The channel has been bounded externally via JSVC,
              // thus bind() becomes a no-op.
              @Override
              protected void doBind(SocketAddress localAddress) throws Exception {}
            };
          }
        });
      }
    } else {
      this.httpServer = null;
    }

    if (policy.isHttpsEnabled()) {
      if (conf.getBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED,
              CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
        conf.set(SSLFactory.KEYSTORES_FACTORY_CLASS_KEY, HopsFileBasedKeyStoresFactory.class.getCanonicalName());
      }
      this.sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
      try {
        sslFactory.init();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
      this.httpsServer = new ServerBootstrap().group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(
              new SslHandler(sslFactory.createSSLEngine()),
              new HttpRequestDecoder(),
              new HttpResponseEncoder(),
              new ChunkedWriteHandler(),
              new URLDispatcher(jettyAddr, conf, confForCreate));
          }
        });
    } else {
      this.httpsServer = null;
      this.sslFactory = null;
    }
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  public void start() {
    if (httpServer != null) {
      ChannelFuture f = httpServer.bind(DataNode.getInfoAddr(conf));
      f.syncUninterruptibly();
      httpAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTP traffic on " + httpAddress);
    }

    if (httpsServer != null) {
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.getTrimmed(
        DFS_DATANODE_HTTPS_ADDRESS_KEY, DFS_DATANODE_HTTPS_ADDRESS_DEFAULT));
      ChannelFuture f = httpsServer.bind(secInfoSocAddr);
      f.syncUninterruptibly();
      httpsAddress = (InetSocketAddress) f.channel().localAddress();
      LOG.info("Listening HTTPS traffic on " + httpsAddress);
    }
  }

  @Override
  public void close() throws IOException {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    if (externalHttpChannel != null) {
      externalHttpChannel.close();
    }
  }
}
