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
package io.hops.transaction.lock;

import io.hops.leader_election.node.ActiveNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.Collection;

public final class SubtreeLockHelper {

  public static boolean isSTOLocked(boolean subtreeLocked, long nameNodeId,
                                    Collection<ActiveNode> activeNamenodes) {
    return subtreeLocked &&
        NameNode.isNameNodeAlive(activeNamenodes, nameNodeId);
  }
}
