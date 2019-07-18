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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URLStreamHandlerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for URL stream handlers.
 * 
 * There is only one handler whose job is to create UrlConnections. A
 * FsUrlConnection relies on FileSystem to choose the appropriate FS
 * implementation.
 * 
 * Before returning our handler, we make sure that FileSystem knows an
 * implementation for the requested scheme/protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsUrlStreamHandlerFactory implements
    URLStreamHandlerFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(FsUrlStreamHandlerFactory.class);

  /**
   * These are the protocols with MUST NOT be exported, as doing so
   * would conflict with the standard URL handlers registered by
   * the JVM. Many things will break.
   */
  public static final String[] UNEXPORTED_PROTOCOLS = {
      "http", "https"
  };

  // The configuration holds supported FS implementation class names.
  private Configuration conf;

  // This map stores whether a protocol is know or not by FileSystem
  private Map<String, Boolean> protocols =
      new ConcurrentHashMap<String, Boolean>();

  // The URL Stream handler
  private java.net.URLStreamHandler handler;

  public FsUrlStreamHandlerFactory() {
    this(new Configuration());
  }

  public FsUrlStreamHandlerFactory(Configuration conf) {
    this.conf = new Configuration(conf);
    // force init of FileSystem code to avoid HADOOP-9041
    try {
      FileSystem.getFileSystemClass("file", conf);
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
    this.handler = new FsUrlStreamHandler(this.conf);
    for (String protocol : UNEXPORTED_PROTOCOLS) {
      protocols.put(protocol, false);
    }
  }

  @Override
  public java.net.URLStreamHandler createURLStreamHandler(String protocol) {
    LOG.debug("Creating handler for protocol {}", protocol);
    if (!protocols.containsKey(protocol)) {
      boolean known = true;
      try {
        Class<? extends FileSystem> impl
            = FileSystem.getFileSystemClass(protocol, conf);
        LOG.debug("Found implementation of {}: {}", protocol, impl);
      }
      catch (IOException ex) {
        known = false;
      }
      protocols.put(protocol, known);
    }
    if (protocols.get(protocol)) {
      LOG.debug("Using handler for protocol {}", protocol);
      return handler;
    } else {
      // FileSystem does not know the protocol, let the VM handle this
      LOG.debug("Unknown protocol {}, delegating to default implementation",
          protocol);
      return null;
    }
  }

}
