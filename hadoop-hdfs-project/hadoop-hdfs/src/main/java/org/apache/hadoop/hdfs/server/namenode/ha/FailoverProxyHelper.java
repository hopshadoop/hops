package org.apache.hadoop.hdfs.server.namenode.ha;

import com.google.common.base.Preconditions;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.security.UserGroupInformation;

import javax.net.ssl.SSLException;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

public class FailoverProxyHelper {
  private static final Log LOG =
          LogFactory.getLog(FailoverProxyHelper.class);

  /**
   * A little pair object to store the address and connected RPC proxy object to
   * an NN. Note that {@link AddressRpcProxyPair#namenode} may be null.
   */
  protected static class AddressRpcProxyPair<T> {
    public InetSocketAddress address;
    public T namenode;

    public AddressRpcProxyPair(InetSocketAddress address) {
      this.address = address;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddressRpcProxyPair<?> that = (AddressRpcProxyPair<?>) o;
      return Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(address);
    }
  }

  /**
   * try connecting to the default uri and get the list of NN from it. If it
   * fails then read the list of NNs from the config file and connect to them
   * for the list of Namenodes in the system.
   */
  protected static List<ActiveNode> getActiveNamenodes(Configuration conf,
                                                         Class xface,
                                                         UserGroupInformation ugi,
                                                         URI defaultUri) throws
          IOException {
    SortedActiveNodeList anl = null;
    ClientProtocol handle = null;
    try {
      handle = (ClientProtocol) NameNodeProxies.createNonHAProxy(conf,
              NameNode.getAddress(defaultUri), xface, ugi, false).getProxy();
      anl = handle.getActiveNamenodesForClient();
    } catch (SSLException e) {
      LOG.error(e, e);
      throw e;
    } catch (Exception e) {
      // TODO: FATAL_UNAUTHORIZED might be thrown for another reason as well
      if (e instanceof RemoteException
              && ((RemoteException) e).getErrorCode().
              equals(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_UNAUTHORIZED)) {
        throw e;
      }
      if (e instanceof IOException
              && (e.getCause() instanceof FileNotFoundException)) {
        LOG.fatal("Could not find file: " + e.getMessage(), e);
        throw e;
      }
      LOG.warn("Failed to get list of NN from default NN. Default NN was " + defaultUri);
    } finally {
      if (handle != null)
        RPC.stopProxy(handle);
    }

    if (anl == null) { // default uri failed, now try the list of NNs from the config file
      List<URI> namenodes = DFSUtil.getNameNodesRPCAddressesAsURIs(conf);
      LOG.debug("Trying the list of NN from the config file  " + namenodes);
      for (URI nn : namenodes) {
        try {
          LOG.debug("Trying to connect to  " + nn);
          handle = (ClientProtocol) NameNodeProxies.createNonHAProxy(conf,
                  NameNode.getAddress(nn), xface, ugi, false).getProxy();
          if (handle != null) {
            anl = handle.getActiveNamenodesForClient();
          }
          if (anl != null && !anl.getActiveNodes().isEmpty()) {
            break; // we got the list
          }
        } catch (Exception e) {
          LOG.error(e);
        } finally {
          if (handle != null) {
            RPC.stopProxy(handle);
          }
        }
      }
    }

    if( anl != null){
      return anl.getSortedActiveNodes();
    } else{
      return null;
    }
  }
}
