/*
 * Copyright (C) 2019 LogicalClocks.
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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;

public class ProvidedBlocksCacheCleaner extends Thread {

  static final Log LOG = LogFactory.getLog(ProvidedBlocksCacheCleaner.class);

  private final File baseDir;
  private final long checkInterval;
  private final int activationTheshold;
  private final int deleteBatchSize;
  private final int waitBeforeDelete;
  private Map<String, CachedProvidedBlock> cachedFiles = new ConcurrentHashMap<>();
  private ProvidedBlocksCacheDiskUtilization diskUtilization;
  private boolean run = true;

  public ProvidedBlocksCacheCleaner(File baseDir, long checkInterval, int activationTheshold,
                                    int deleteBatchSize, int waitBeforeDelete) {
    this.baseDir = baseDir;
    this.activationTheshold = activationTheshold;
    this.checkInterval = checkInterval;
    this.deleteBatchSize = deleteBatchSize;
    this.diskUtilization = new ProvidedBlocksCacheDiskUtilization(baseDir);
    this.waitBeforeDelete = waitBeforeDelete;

    File[] files = baseDir.listFiles(); //flat dir structure
    for (File file : files) {
      if (file.isFile()) {
        cachedFiles.put(file.getAbsolutePath(), new CachedProvidedBlock(file.getAbsolutePath(),
                System.currentTimeMillis()));
      }
    }
  }

  public void fileAccessed(String file) {
    cachedFiles.put(file, new CachedProvidedBlock(file,
            System.currentTimeMillis()));
    LOG.debug("HopsFS-Cloud. Provided blocks Cache. Added/Updated file: " + file+ " Total cached " +
            "files: "+cachedFiles.size());
  }

  public void fileDeleted(String file) {
    cachedFiles.remove(file);
    LOG.debug("HopsFS-Cloud. Provided blocks Cache. Removed file: " + file);
  }

  @Override
  public void run() {
    try {
      while (run) {
        double percentage = diskUtilization.getDiskUtilization();
        if (percentage >= activationTheshold) {
          LOG.info("HopsFS-Cloud. Disk utilization is " + (long) percentage + ". " +
                  "Freeing up space to make room for new blocks");
          freeUpSpace();
          Thread.sleep(100);
          continue;
        } else {
          LOG.debug("HopsFS-Cloud. Provided bocks cache. No need to free up disk space. " +
                  "Disk utilization: " + percentage + "%");
        }
        Thread.sleep(checkInterval);
      }
    } catch (InterruptedException e) {

    }
  }


  public synchronized void freeUpSpace() {
    LOG.debug("HopsFS-Cloud. Provided blocks cache. Total Files in cache are " + cachedFiles.size());
    List<CachedProvidedBlock> list = new ArrayList<>(cachedFiles.values());
    Collections.sort(list);

    int counter = 0;
    for (CachedProvidedBlock cBlk : list) {
      String filePath = cBlk.path;
      File file = new File(filePath);

      long ts = file.lastModified();
      if ((System.currentTimeMillis() - ts) < waitBeforeDelete) {
        continue;  // do not delete recently downloaded files. It is possible that a clients
        // are currently reading these files
      }

      boolean isFileUnlocked = false;
      boolean deleted = false;
      try {
        org.apache.commons.io.FileUtils.touch(file); //managed to update TS of the file
        // --> file is not open
        isFileUnlocked = true;
      } catch (IOException e) {
        isFileUnlocked = false;
      }

      if (isFileUnlocked) {
        if (file.delete()) {
          fileDeleted(filePath);
          counter++;
          deleted = true;
        }
      }

      if (!deleted) {
        LOG.warn("HopsFS-Cloud. Provided blocks disk cache. Could not delete " + file.getName());
        continue;
      }

      if (counter >= deleteBatchSize) {
        break;
      }
    }
  }

  public void shutdown() {
    run = false;
    interrupt();
  }

  class CachedProvidedBlock implements Comparable<CachedProvidedBlock> {
    private final String path;
    private final long time;

    public CachedProvidedBlock(String path, long time) {
      this.path = path;
      this.time = time;
    }

    public long getTime() {
      return time;
    }

    public String getPath() {
      return path;
    }

    @Override
    public int compareTo(CachedProvidedBlock o) {
      return new Long(time).compareTo(o.time);
    }
  }

  public int getCachedFilesCount() {
    return cachedFiles.size();
  }

  public ProvidedBlocksCacheDiskUtilization getDiskUtilizationCalc() {
    return diskUtilization;
  }

  public void setDiskUtilizationMock(ProvidedBlocksCacheDiskUtilization mock) {
    diskUtilization = mock;
  }
}
