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
package io.hops.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;

public class IDsMonitor implements Runnable {

  private static final Log LOG = LogFactory.getLog(IDsMonitor.class);
  private static IDsMonitor instance = null;
  private Thread th = null;

  private int checkInterval;
  private IDsMonitor() {
  }

  public static IDsMonitor getInstance() {
    if (instance == null) {
      instance = new IDsMonitor();
    }
    return instance;
  }

  public void setConfiguration(Configuration conf) {
    IDsGeneratorFactory.getInstance().setConfiguration(conf.getInt
            (DFSConfigKeys.DFS_NAMENODE_INODEID_BATCH_SIZE,
                DFSConfigKeys.DFS_NAMENODE_INODEID_BATCH_SIZE_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_ID_BATCH_SIZE,
            DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_ID_BATCH_SIZ_DEFAULT),
        conf.getInt
            (DFSConfigKeys.DFS_NAMENODE_CACHE_DIRECTIVE_ID_BATCH_SIZE,
                DFSConfigKeys.DFS_NAMENODE_CACHE_DIRECTIVE_ID_BATCH_SIZE_DEFAULT),
        conf.getFloat(DFSConfigKeys.DFS_NAMENODE_INODEID_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_INODEID_UPDATE_THRESHOLD_DEFAULT),
        conf.getFloat(DFSConfigKeys.DFS_NAMENODE_BLOCKID_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_BLOCKID_UPDATE_THRESHOLD_DEFAULT),
        conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_ID_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_ID_UPDATE_THRESHOLD_DEFAULT),
        conf.getFloat(DFSConfigKeys.DFS_NAMENODE_CACHE_DIRECTIVE_ID_UPDATE_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_CACHE_DIRECTIVE_ID_UPDATE_THRESHOLD_DEFAULT)
        );

    checkInterval = conf.getInt(DFSConfigKeys.DFS_NAMENODE_IDSMONITOR_CHECK_INTERVAL_IN_MS,
        DFSConfigKeys.DFS_NAMENODE_IDSMONITOR_CHECK_INTERVAL_IN_MS_DEFAULT);
  }



  public void start() {
    getNewIds(); // Avoid race conditions between operations and the first acquisition of ids
    th = new Thread(this, "IDsMonitor");
    th.setDaemon(true);
    th.start();
  }

  @Override
  public void run() {
    while (true) {
      getNewIds();
    }
  }

  private void getNewIds() {
    try {

      IDsGeneratorFactory.getInstance().getNewIDs();

      Thread.sleep(checkInterval);
    } catch (InterruptedException ex) {
      LOG.warn("IDsMonitor interrupted: " + ex);
    } catch (IOException ex) {
      LOG.warn("IDsMonitor got exception: " + ex);
    }
  }
}
