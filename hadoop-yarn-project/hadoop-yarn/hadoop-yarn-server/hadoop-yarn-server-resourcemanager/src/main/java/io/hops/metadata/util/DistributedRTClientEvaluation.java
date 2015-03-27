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
package io.hops.metadata.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.util.Records;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedRTClientEvaluation {

  private static final Log LOG =
      LogFactory.getLog(DistributedRTClientEvaluation.class);

  private Configuration conf;
  private final int nbNM;
  private final int nbNMTotal;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final int hbPeriod;
  private final long duration;
  private final String output;
  private AtomicLong nbTreatedScheduler = new AtomicLong(0);
  private AtomicLong nbTreatedRT = new AtomicLong(0);

  //NodeManagers
  private final SortedMap<Integer, NodeId> nmMap =
      new TreeMap<Integer, NodeId>();

  public DistributedRTClientEvaluation(String rtAddress, int nbSimulatedNM,
      int hbPeriod, long duration, String output, int startingPort,
      int nbNMTotal) throws IOException, YarnException, InterruptedException {

    this.nbNM = nbSimulatedNM;
    this.hbPeriod = hbPeriod;
    this.duration = duration;
    this.output = output;
    this.nbNMTotal = nbNMTotal;
    conf = new YarnConfiguration();
    conf.setStrings(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, rtAddress);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class,
        ResourceScheduler.class);

    //Create NMs
    for (int i = 0; i < nbSimulatedNM; i++) {
      nmMap.put(i, NodeId.newInstance(InetAddress.getLocalHost().getHostName(),
          startingPort + i));
    }


    start();
  }

  public static void main(String[] args)
      throws IOException, YarnException, InterruptedException {
    LOG.info("version 2.0");
    if (args.length == 0) {
      //TODO display help
      return;
    }

    if (args[0].equals("run")) {
      String rtAddress = args[1];
      int nbSimulatedNM = Integer.parseInt(args[2]);
      int hbPeriod = Integer.parseInt(args[3]);
      long duration = Long.parseLong(args[4]);
      int startingPort = Integer.parseInt(args[5]);
      int nbNMTotal = Integer.parseInt(args[6]);
      String output = args[7];

      DistributedRTClientEvaluation client =
          new DistributedRTClientEvaluation(rtAddress, nbSimulatedNM, hbPeriod,
              duration, output, startingPort, nbNMTotal);

    }

  }

  public void start() throws YarnException, IOException, InterruptedException {

    //Assign nodes to worker threads
    //Register nodes
    ResourceTracker rt = (ResourceTracker) RpcClientFactoryPBImpl.get().
        getClient(ResourceTracker.class, 1, NetUtils.getConnectAddress(
                new InetSocketAddress(
                    conf.get(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS),
                    conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_PORT,
                        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT))),
            conf);
    for (NodeId nid : nmMap.values()) {
      registerClient(rt, nid);
    }
    //Wait for processing to complete
    //TODO: Get Active RMNodes from ndb instead of sleeping
    Thread.sleep(5000);
    //Send heartbeats

    for (NodeId nid : nmMap.values()) {
      //Send the Heartbeats

      executor.execute(new RTClientWorker(nid));
      Thread.sleep(hbPeriod / nbNM);

    }
    executor.shutdown();
    Thread.sleep(duration / 4);
    nbTreatedScheduler.set(0);
    nbTreatedRT.set(0);
    long start = System.currentTimeMillis();
    Thread.sleep(duration / 2);
    double nbHBTheoric = ((double) nbNM * duration / 2) / hbPeriod;
    System.out.
        printf(
            "nb treatedRT %d, nb treatedScheduler %d, theorical nbhb: %f, duration: %d\n",
            nbTreatedRT.get(), nbTreatedScheduler.get(), nbHBTheoric,
            System.currentTimeMillis() - start);
    double treatedSchedulerRate =
        (double) nbTreatedScheduler.get() / nbHBTheoric;
    double treatedRTRate = (double) nbTreatedRT.get() / nbHBTheoric;
    LOG.info("treatedSchedulerRate: " + treatedSchedulerRate);

    File file = new File(output);
    if (!file.exists()) {
      file.createNewFile();
    }
    FileWriter fileWriter = new FileWriter(output, true);

    BufferedWriter bufferWritter = new BufferedWriter(fileWriter);
    bufferWritter.write(
        nbNMTotal + "\t" + nbTreatedRT.get() + "\t" + nbTreatedScheduler.get() +
            "\t" + nbHBTheoric + "\t" + treatedRTRate + "\t" +
            treatedSchedulerRate + "\n");
    bufferWritter.close();

    Thread.sleep(1000);
    System.exit(0);
  }

  /**
   * Registers a node with the RT. If num is greater that 1, multiple requests
   * are sent for the same node and the last response is returned;
   * <p/>
   *
   * @param rt
   * @param host
   * @param port
   * @param num
   * @return
   */
  private void registerClient(ResourceTracker rt, NodeId nodeId) {
    try {
      RegisterNodeManagerRequest request =
          Records.newRecord(RegisterNodeManagerRequest.class);
      request.setHttpPort(nodeId.getPort());
      request.setNodeId(nodeId);
      Resource resource = Resource.newInstance(5012, 8);
      request.setResource(resource);
      rt.registerNodeManager(request);
    } catch (YarnException ex) {
      LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
    } catch (IOException ex) {
      LOG.error("HOP :: Error sending NodeHeartbeatResponse", ex);
    }
  }

  private class RTClientWorker implements Runnable {

    private final NodeId nodeId;

    public RTClientWorker(NodeId nodeId) {
      this.nodeId = nodeId;
    }

    @Override
    public void run() {
      ResourceTracker rt = (ResourceTracker) RpcClientFactoryPBImpl.get().
          getClient(ResourceTracker.class, 1, NetUtils.getConnectAddress(
                  new InetSocketAddress(
                      conf.get(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS),
                      conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_PORT,
                          YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT))),
              conf);
      int lastHBId = 0;

      NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
      healthStatus.setHealthReport("");
      healthStatus.setIsNodeHealthy(true);
      healthStatus.setLastHealthReportTime(1);
      MasterKey masterKey = new MasterKeyPBImpl();

      while (true) {
        try {
          Long start = System.currentTimeMillis();

          NodeHeartbeatRequest req =
              Records.newRecord(NodeHeartbeatRequest.class);
          NodeStatus status = Records.newRecord(NodeStatus.class);
          req.setLastKnownContainerTokenMasterKey(masterKey);
          req.setLastKnownNMTokenMasterKey(masterKey);
          status.setNodeId(nodeId);
          status.setResponseId(lastHBId);
          status.setNodeHealthStatus(healthStatus);
          req.setNodeStatus(status);
          LOG.info("send heartbeat node " + nodeId.toString());
          NodeHeartbeatResponse resp = rt.nodeHeartbeat(req);

          if (resp != null) {
            lastHBId = resp.getResponseId();
            nbTreatedRT.incrementAndGet();
            LOG.info("sent heartbeat node " + nodeId.toString() + " nextHB: " +
                resp.getNextheartbeat());
            if (resp.getNextheartbeat()) {
              nbTreatedScheduler.incrementAndGet();
            }
          }
          Thread.sleep(hbPeriod);
        } catch (YarnException ex) {
          Logger.getLogger(DistributedRTClientEvaluation.class.getName()).
              log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
          Logger.getLogger(DistributedRTClientEvaluation.class.getName()).
              log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
          Logger.getLogger(DistributedRTClientEvaluation.class.getName()).
              log(Level.SEVERE, null, ex);
        }
      }
    }

  }

}
