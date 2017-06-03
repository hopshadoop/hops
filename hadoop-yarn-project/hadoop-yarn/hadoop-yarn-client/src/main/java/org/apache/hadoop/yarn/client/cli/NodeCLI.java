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
package org.apache.hadoop.yarn.client.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

@Private
@Unstable
public class NodeCLI extends YarnCLI {
  private static final String NODES_PATTERN = "%16s\t%15s\t%17s\t%28s" +
    System.getProperty("line.separator");

  private static final String NODE_STATE_CMD = "states";
  private static final String NODE_ALL = "all";

  public static void main(String[] args) throws Exception {
    NodeCLI cli = new NodeCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    opts.addOption(STATUS_CMD, true, "Prints the status report of the node.");
    opts.addOption(LIST_CMD, false, "List all running nodes. " +
        "Supports optional use of -states to filter nodes " +
        "based on node state, all -all to list all nodes.");
    Option nodeStateOpt = new Option(NODE_STATE_CMD, true,
        "Works with -list to filter nodes based on input comma-separated list of node states.");
    nodeStateOpt.setValueSeparator(',');
    nodeStateOpt.setArgs(Option.UNLIMITED_VALUES);
    nodeStateOpt.setArgName("States");
    opts.addOption(nodeStateOpt);
    Option allOpt = new Option(NODE_ALL, false,
        "Works with -list to list all nodes.");
    opts.addOption(allOpt);
    opts.getOption(STATUS_CMD).setArgName("NodeId");

    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      sysout.println("Missing argument for options");
      printUsage(opts);
      return exitCode;
    }

    if (cliParser.hasOption("status")) {
      if (args.length != 2) {
        printUsage(opts);
        return exitCode;
      }
      printNodeStatus(cliParser.getOptionValue("status"));
    } else if (cliParser.hasOption("list")) {
      Set<NodeState> nodeStates = new HashSet<NodeState>();
      if (cliParser.hasOption(NODE_ALL)) {
        for (NodeState state : NodeState.values()) {
          nodeStates.add(state);
        }
      } else if (cliParser.hasOption(NODE_STATE_CMD)) {
        String[] types = cliParser.getOptionValues(NODE_STATE_CMD);
        if (types != null) {
          for (String type : types) {
            if (!type.trim().isEmpty()) {
              nodeStates.add(NodeState.valueOf(
                  org.apache.hadoop.util.StringUtils.toUpperCase(type.trim())));
            }
          }
        }
      } else {
        nodeStates.add(NodeState.RUNNING);
      }
      listClusterNodes(nodeStates);
    } else if (cliParser.hasOption(HELP_CMD)) {
      printUsage(opts);
      return 0;
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
    }
    return 0;
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("node", opts);
  }

  /**
   * Lists the nodes matching the given node states
   * 
   * @param nodeStates
   * @throws YarnException
   * @throws IOException
   */
  private void listClusterNodes(Set<NodeState> nodeStates) 
            throws YarnException, IOException {
    PrintWriter writer = new PrintWriter(
        new OutputStreamWriter(sysout, Charset.forName("UTF-8")));
    List<NodeReport> nodesReport = client.getNodeReports(
                                       nodeStates.toArray(new NodeState[0]));
    writer.println("Total Nodes:" + nodesReport.size());
    writer.printf(NODES_PATTERN, "Node-Id", "Node-State", "Node-Http-Address",
        "Number-of-Running-Containers");
    for (NodeReport nodeReport : nodesReport) {
      writer.printf(NODES_PATTERN, nodeReport.getNodeId(), nodeReport
          .getNodeState(), nodeReport.getHttpAddress(), nodeReport
          .getNumContainers());
    }
    writer.flush();
  }

  /**
   * Prints the node report for node id.
   * 
   * @param nodeIdStr
   * @throws YarnException
   */
  private void printNodeStatus(String nodeIdStr) throws YarnException,
      IOException {
    NodeId nodeId = ConverterUtils.toNodeId(nodeIdStr);
    List<NodeReport> nodesReport = client.getNodeReports();
    // Use PrintWriter.println, which uses correct platform line ending.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter nodeReportStr = new PrintWriter(
        new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    NodeReport nodeReport = null;
    for (NodeReport report : nodesReport) {
      if (!report.getNodeId().equals(nodeId)) {
        continue;
      }
      nodeReport = report;
      nodeReportStr.println("Node Report : ");
      nodeReportStr.print("\tNode-Id : ");
      nodeReportStr.println(nodeReport.getNodeId());
      nodeReportStr.print("\tRack : ");
      nodeReportStr.println(nodeReport.getRackName());
      nodeReportStr.print("\tNode-State : ");
      nodeReportStr.println(nodeReport.getNodeState());
      nodeReportStr.print("\tNode-Http-Address : ");
      nodeReportStr.println(nodeReport.getHttpAddress());
      nodeReportStr.print("\tLast-Health-Update : ");
      nodeReportStr.println(DateFormatUtils.format(
          new Date(nodeReport.getLastHealthReportTime()),
            "E dd/MMM/yy hh:mm:ss:SSzz"));
      nodeReportStr.print("\tHealth-Report : ");
      nodeReportStr
          .println(nodeReport.getHealthReport());
      nodeReportStr.print("\tContainers : ");
      nodeReportStr.println(nodeReport.getNumContainers());
      nodeReportStr.print("\tMemory-Used : ");
      nodeReportStr.println((nodeReport.getUsed() == null) ? "0MB"
          : (nodeReport.getUsed().getMemory() + "MB"));
      nodeReportStr.print("\tMemory-Capacity : ");
      nodeReportStr.println(nodeReport.getCapability().getMemory() + "MB");
      nodeReportStr.print("\tCPU-Used : ");
      nodeReportStr.println((nodeReport.getUsed() == null) ? "0 vcores"
          : (nodeReport.getUsed().getVirtualCores() + " vcores"));
      nodeReportStr.print("\tCPU-Capacity : ");
      nodeReportStr.println(nodeReport.getCapability().getVirtualCores() + " vcores");
      nodeReportStr.print("\tGPU-Used : ");
      nodeReportStr.println((nodeReport.getUsed() == null) ? "0 gpus"
          : (nodeReport.getUsed().getGPUs() + " gpus"));
      nodeReportStr.print("\tGPU-Capacity : ");
      nodeReportStr.println(nodeReport.getCapability().getGPUs() + " " +
                    "gpus");
      nodeReportStr.print("\tNode-Labels : ");
      
      // Create a List for node labels since we need it get sorted
      List<String> nodeLabelsList =
          new ArrayList<String>(report.getNodeLabels());
      Collections.sort(nodeLabelsList);
      nodeReportStr.println(StringUtils.join(nodeLabelsList.iterator(), ','));
    }

    if (nodeReport == null) {
      nodeReportStr.print("Could not find the node report for node id : "
          + nodeIdStr);
    }
    nodeReportStr.close();
    sysout.println(baos.toString("UTF-8"));
  }
}
