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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;


/**
 * This program starts a mini cluster of data nodes
 * (ie a mini cluster without the name node), all within one address space.
 * It is assumed that the name node has been started separately prior
 * to running this program.
 * <p/>
 * A use case of this is to run a real name node with a large number of
 * simulated data nodes for say a NN benchmark.
 * <p/>
 * Synopisis:
 * DataNodeCluster -n numDatNodes [-racks numRacks] -simulated
 * [-inject startingBlockId numBlocksPerDN]
 * [ -r replicationForInjectedBlocks ]
 * [-d editsLogDirectory]
 * <p/>
 * if -simulated is specified then simulated data nodes are started.
 * if -inject is specified then blocks are injected in each datanode;
 * -inject option is valid only for simulated data nodes.
 * <p/>
 * See Also @link #CreateEditsLog for creating a edits log file to
 * inject a matching set of blocks into into a name node.
 * Typical use of -inject is to inject blocks into a set of datanodes
 * using this DataNodeCLuster command
 * and then to inject the same blocks into a name node using the
 * CreateEditsLog command.
 */

public class DataNodeCluster {
  static final String DATANODE_DIRS = "/tmp/DataNodeCluster";
  static String dataNodeDirs = DATANODE_DIRS;
  static final String USAGE = "Usage: datanodecluster " +
      " -n <numDataNodes> " +
      " -bpid <bpid>" +
      " [-racks <numRacks>] " +
      " [-simulated [<simulatedCapacityPerDn>]] " +
      " [-inject startingBlockId numBlocksPerDN]" +
      " [-r replicationFactorForInjectedBlocks]" +
      " [-d dataNodeDirs]\n" +
      " [-checkDataNodeAddrConfig]\n" +
      "      Default datanode direcory is " + DATANODE_DIRS + "\n" +
      "      Default replication factor for injected blocks is 1\n" +
      "      Defaul rack is used if -racks is not specified\n" +
      "      Data nodes are simulated if -simulated OR conf file specifies simulated\n" +
      "      -checkDataNodeAddrConfig tells DataNodeConf to use data node addresses from conf file, if it is set. If not set, use .localhost'.";
  
  
  static void printUsageExit() {
    System.out.println(USAGE);
    System.exit(-1);
  }

  static void printUsageExit(String err) {
    System.out.println(err);
    printUsageExit();
  }
  
  public static void main(String[] args) throws InterruptedException {
    int numDataNodes = 0;
    int numRacks = 0;
    boolean inject = false;
    long startingBlockId = 1;
    int numBlocksPerDNtoInject = 0;
    int replication = 1;
    boolean checkDataNodeAddrConfig = false;
    long simulatedCapacityPerDn = SimulatedFSDataset.DEFAULT_CAPACITY;
    String bpid = null;
    
    Configuration conf = new HdfsConfiguration();

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equals("-n")) {
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("missing number of nodes");
        }
        numDataNodes = Integer.parseInt(args[i]);
      } else if (args[i].equals("-racks")) {
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("Missing number of racks");
        }
        numRacks = Integer.parseInt(args[i]);
      } else if (args[i].equals("-r")) {
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("Missing replication factor");
        }
        replication = Integer.parseInt(args[i]);
      } else if (args[i].equals("-d")) {
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("Missing datanode dirs parameter");
        }
        dataNodeDirs = args[i];
      } else if (args[i].equals("-simulated")) {
        SimulatedFSDataset.setFactory(conf);
        if ((i+1) < args.length && !args[i+1].startsWith("-")) {
          simulatedCapacityPerDn = Long.parseLong(args[++i]);
        }
      } else if (args[i].equals("-bpid")) {
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("Missing blockpoolid parameter");
        }
        bpid = args[i];
      } else if (args[i].equals("-inject")) {
        if (!FsDatasetSpi.Factory.getFactory(conf).isSimulated()) {
          System.out.print("-inject is valid only for simulated");
          printUsageExit();
        }
        inject = true;
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit(
              "Missing starting block and number of blocks per DN to inject");
        }
        startingBlockId = Integer.parseInt(args[i]);
        if (++i >= args.length || args[i].startsWith("-")) {
          printUsageExit("Missing number of blocks to inject");
        }
        numBlocksPerDNtoInject = Integer.parseInt(args[i]);
      } else if (args[i].equals("-checkDataNodeAddrConfig")) {
        checkDataNodeAddrConfig = true;
      } else {
        printUsageExit();
      }
    }
    if (numDataNodes <= 0 || replication <= 0) {
      printUsageExit(
          "numDataNodes and replication have to be greater than zero");
    }
    if (replication > numDataNodes) {
      printUsageExit("Replication must be less than or equal to numDataNodes");
      
    }
    if (bpid == null) {
      printUsageExit("BlockPoolId must be provided");
    }
    String nameNodeAdr = FileSystem.getDefaultUri(conf).getAuthority();
    if (nameNodeAdr == null) {
      System.out.println("No name node address and port in config");
      System.exit(-1);
    }
    boolean simulated = FsDatasetSpi.Factory.getFactory(conf).isSimulated();
    System.out.println("Starting " + numDataNodes +
        (simulated ? " Simulated " : " ") +
        " Data Nodes that will connect to Name Node at " + nameNodeAdr);

    System.setProperty("test.build.data", dataNodeDirs);

    long simulatedCapacities[] = new long[numDataNodes];
    for (int i = 0; i < numDataNodes; ++i) {
      simulatedCapacities[i] = simulatedCapacityPerDn;
    }

    MiniDFSCluster mc = new MiniDFSCluster();
    try {
      mc.formatDataNodeDirs();
    } catch (IOException e) {
      System.out.println("Error formating data node dirs:" + e);
    }

    String[] rack4DataNode = null;
    if (numRacks > 0) {
      System.out.println("Using " + numRacks + " racks: ");
      String rackPrefix = getUniqueRackPrefix();

      rack4DataNode = new String[numDataNodes];
      for (int i = 0; i < numDataNodes; ++i) {
        //rack4DataNode[i] = racks[i%numRacks];
        rack4DataNode[i] = rackPrefix + "-" + i % numRacks;
        System.out.println("Data Node " + i + " using " + rack4DataNode[i]);
        
        
      }
    }
    try {
      mc.startDataNodes(conf, numDataNodes, true, StartupOption.REGULAR,
          rack4DataNode, null, simulatedCapacities, false, checkDataNodeAddrConfig);
      Thread.sleep(10*1000);   // Give the DN some time to connect to NN and init storage directories.
      if (inject) {
        long blockSize = 10;
        System.out.println("Injecting " + numBlocksPerDNtoInject +
            " blocks in each DN starting at blockId " + startingBlockId +
            " with blocksize of " + blockSize);
        Block[] blocks = new Block[numBlocksPerDNtoInject];
        long blkid = startingBlockId;
        for (int i_dn = 0; i_dn < numDataNodes; ++i_dn) {
          for (int i = 0; i < blocks.length; ++i) {
            blocks[i] =
                new Block(blkid++, blockSize, GenerationStamp.LAST_RESERVED_STAMP
                    //HOP                CreateEditsLog.BLOCK_GENERATION_STAM
                    , Block.NON_EXISTING_BUCKET_ID
                );
          }
          for (int i = 1; i <= replication; ++i) {
            // inject blocks for dn_i into dn_i and replica in dn_i's neighbors 
            mc.injectBlocks((i_dn + i - 1) % numDataNodes,
                Arrays.asList(blocks), bpid);
            System.out.println("Injecting blocks of dn " + i_dn + " into dn" +
                ((i_dn + i - 1) % numDataNodes));
          }
        }
        System.out.println(
            "Created blocks from Bids " + startingBlockId + " to " +
                (blkid - 1));
      }

    } catch (IOException e) {
      System.out.println("Error creating data node:" + e);
    }
  }

  /*
   * There is high probability that the rack id generated here will 
   * not conflict with those of other data node cluster.
   * Not perfect but mostly unique rack ids are good enough
   */
  static private String getUniqueRackPrefix() {

    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      System.out.println("Could not find ip address of \"default\" inteface.");
    }
    
    int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
    return "/Rack-" + rand + "-" + ip + "-" + Time.now();
  }
}
