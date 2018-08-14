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
package io.hops;

import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;

public class TestUtil {

  /**
   * Get the inodeId for a file/directory.
   *
   * @param nameNode the NameNode
   * @param path the path to the file
   * @return the inodeId
   * @throws IOException
   */
  public static int getINodeId(final NameNode nameNode, final Path path)
      throws IOException {
    return getINode(nameNode, path).getId();
  }
  
  /**
   * Get the inode row for a file/directory.
   *
   * @param nameNode the NameNode
   * @param path the path to the file
   * @return the INode
   * @throws IOException
   */
  public static INode getINode(final NameNode nameNode, final Path path)
      throws IOException {
    final String filePath = path.toUri().getPath();
    return (INode) new HopsTransactionalRequestHandler(
        HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.READ_COMMITTED, TransactionLockTypes.INodeResolveType.PATH, filePath)
            .setNameNodeID(nameNode.getId()).setActiveNameNodes(nameNode.getActiveNameNodes().getActiveNodes());
        locks.add(il);
      }
      
      @Override
      public Object performTask() throws IOException {
        INode targetNode = nameNode.getNamesystem().getINode(filePath);
        return targetNode;
      }
    }.handle();
  }
}
