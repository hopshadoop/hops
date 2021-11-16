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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.hdfs.entity.ProjectedINode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FSSTOHelper {
  public static final Log LOG = LogFactory.getLog(FSSTOHelper.class);

  public static void applyAllPendingQuotaInSubTree(final FSNamesystem fsn,
                                                   final AbstractFileTree.FileTree fileTree) throws IOException {
    for (int i = fileTree.getHeight(); i > 0; i--) {
      try {
        Collection<ProjectedINode> dirs = fileTree.getDirsByLevel(i);
        List<Long> dirIDs = new ArrayList<>();
        for (ProjectedINode dir : dirs) {
          dirIDs.add(dir.getId());
        }

        Iterator<Long> itr = dirIDs.iterator();
        synchronized (itr) {
          fsn.getQuotaUpdateManager().addPrioritizedUpdates(itr);
          itr.wait();
        }

      } catch (InterruptedException e) {
        throw new IOException("Operation failed due to an Interrupt");
      }
    }
  }

  public static void applyAllPendingQuotaForDirectory(final FSNamesystem fsn,
                                                      final long inodeID) throws IOException {
    try {
      List<Long> dstInode = new ArrayList<>();
      dstInode.add(inodeID);
      Iterator<Long> itr = dstInode.iterator();
      synchronized (itr) {
        fsn.getQuotaUpdateManager().addPrioritizedUpdates(itr);
        itr.wait();
      }

    } catch (InterruptedException e) {
      throw new IOException("Operation failed due to an Interrupt");
    }
  }
}
