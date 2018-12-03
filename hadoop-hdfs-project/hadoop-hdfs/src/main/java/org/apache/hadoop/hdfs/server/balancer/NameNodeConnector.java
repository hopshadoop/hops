/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;

/**
 * The class provides utilities for {@link Balancer} to access a NameNode
 */
@InterfaceAudience.Private
public class NameNodeConnector implements Closeable {
  private static final Log LOG = LogFactory.getLog(NameNodeConnector.class);
  
  private static final int MAX_NOT_CHANGED_ITERATIONS = 5;
  private static boolean write2IdFile = true;
 
  /**
   * Create {@link NameNodeConnector} for the given namenodes.
   */
  public static List<NameNodeConnector> newNameNodeConnectors(
      Collection<URI> namenodes, String name, Path idPath, Configuration conf)
      throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (URI uri : namenodes) {
      NameNodeConnector nnc = new NameNodeConnector(name, uri, idPath, null, conf);
      connectors.add(nnc);
    }
    return connectors;
  }

  public static List<NameNodeConnector> newNameNodeConnectors(
      Map<URI, List<Path>> namenodes, String name, Path idPath,
      Configuration conf) throws IOException {
    final List<NameNodeConnector> connectors = new ArrayList<NameNodeConnector>(
        namenodes.size());
    for (Map.Entry<URI, List<Path>> entry : namenodes.entrySet()) {
      NameNodeConnector nnc = new NameNodeConnector(name, entry.getKey(), idPath, entry.getValue(), conf);
      connectors.add(nnc);
    }
    return connectors;
  }

  @VisibleForTesting
  public static void setWrite2IdFile(boolean write2IdFile) {
    NameNodeConnector.write2IdFile = write2IdFile;
  }
  
  final URI nameNodeUri;
  final String blockpoolID;

  final NamenodeProtocol namenode;
  final ClientProtocol client;
  private final KeyManager keyManager;
  
  final DistributedFileSystem fs;
  private final Path idPath;
  final OutputStream out;
  private final List<Path> targetPaths;
  private final AtomicLong bytesMoved = new AtomicLong();
  
  private int notChangedIterations = 0;

  public NameNodeConnector(String name, URI nameNodeUri, Path idPath, List<Path> targetPaths, Configuration conf) 
      throws IOException {
    this.nameNodeUri = nameNodeUri;
    this.idPath = idPath;
    this.targetPaths = targetPaths == null || targetPaths.isEmpty() ? Arrays.asList(new Path("/")) : targetPaths;

    this.namenode = NameNodeProxies.createProxy(conf, nameNodeUri, NamenodeProtocol.class).getProxy();
    this.client = NameNodeProxies.createProxy(conf, nameNodeUri, ClientProtocol.class).getProxy();
    this.fs = (DistributedFileSystem) FileSystem.get(nameNodeUri, conf);

    final NamespaceInfo namespaceinfo = namenode.versionRequest();
    this.blockpoolID = namespaceinfo.getBlockPoolID();

    final FsServerDefaults defaults = fs.getServerDefaults(new Path("/"));
    this.keyManager = new KeyManager(blockpoolID, namenode,
        defaults.getEncryptDataTransfer(), conf);
    // Exit if there is another one running.
    out = checkAndMarkRunning(); 
    if (out == null) {
      throw new IOException("Another " + name + " is running.");
    }
  }

  public DistributedFileSystem getDistributedFileSystem() {
    return fs;
  }

  /**
   * @return the block pool ID
   */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  AtomicLong getBytesMoved() {
    return bytesMoved;
  }

  /**
   * @return blocks with locations.
   */
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    return namenode.getBlocks(datanode, size);
  }

  /** @return live datanode storage reports. */
  public DatanodeStorageReport[] getLiveDatanodeStorageReport()
      throws IOException {
    return client.getDatanodeStorageReport(HdfsConstants.DatanodeReportType.LIVE);
  }

  /** @return the key manager */
  public KeyManager getKeyManager() {
    return keyManager;
  }
  
  /** @return the list of paths to scan/migrate */
  public List<Path> getTargetPaths() {
    return targetPaths;
  }

  /** Should the instance continue running? */
  public boolean shouldContinue(long dispatchBlockMoveBytes) {
    if (dispatchBlockMoveBytes > 0) {
      notChangedIterations = 0;
    } else {
      notChangedIterations++;
      if (notChangedIterations >= MAX_NOT_CHANGED_ITERATIONS) {
        System.out.println("No block has been moved for "
            + notChangedIterations + " iterations. Exiting...");
        return false;
      }
    }
    return true;
  }

  /**
   * The idea for making sure that there is no more than one instance
   * running in an HDFS is to create a file in the HDFS, writes the hostname
   * of the machine on which the instance is running to the file, but did not
   * close the file until it exits. 
   * 
   * This prevents the second instance from running because it can not
   * creates the file while the first one is running.
   *
   * This method checks if there is any running instance. If no, mark yes.
   * Note that this is an atomic operation.
   *
   * @return null if there is a running instance;
   *         otherwise, the output stream to the newly created file.
   */
  private OutputStream checkAndMarkRunning() throws IOException {
    try {
      final FSDataOutputStream out = fs.create(idPath);
      if (write2IdFile) {
        out.writeBytes(InetAddress.getLocalHost().getHostName());
        out.flush();
      }
      return out;
    } catch (RemoteException e) {
      if (AlreadyBeingCreatedException.class.getName()
          .equals(e.getClassName())) {
        return null;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void close() {
    keyManager.close();

    // close the output file
    IOUtils.closeStream(out);
    if (fs != null) {
      try {
        fs.delete(idPath, true);
      } catch (IOException ioe) {
        LOG.warn("Failed to delete " + idPath, ioe);
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[namenodeUri=" + nameNodeUri +
        ", id=" + blockpoolID + "]";
  }
}