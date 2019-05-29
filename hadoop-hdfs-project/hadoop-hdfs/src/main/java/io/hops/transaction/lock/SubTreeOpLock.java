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

import io.hops.metadata.hdfs.entity.SubTreeOperation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;

public final class SubTreeOpLock extends Lock {

  private final TransactionLockTypes.LockType lockType;
  private final String pathPrefix;
  public static final Log LOG = LogFactory.getLog(SubTreeOpLock.class);
  
  SubTreeOpLock(TransactionLockTypes.LockType lockType, String pathPrefix) {
    this.lockType = lockType;
    this.pathPrefix = pathPrefix;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    //Inodes for the pathPrefix have already been resolved
    //if the last component of the resolved path is a file then ignore  
    //the lock request as STO operations are only for directories
    
   
    
    
    //remove the / from the end of the path
    String newPath = "";
    if(pathPrefix.endsWith("/") && pathPrefix.length() != 1 /*root*/){
      newPath = pathPrefix.substring(0, pathPrefix.length()-1);
    }else{
      newPath = pathPrefix;
    }
    LOG.debug("Checking if the path \""+newPath+"\" belongs to a file or a dir");
    BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    List<INode> inodes = inodeLock.getPathINodes(newPath);
    if(inodes.get(inodes.size()-1).isDirectory()){
       LOG.debug(newPath+" is a directory so checking the STO table for ongoing operations ");
       acquireLockList(lockType, SubTreeOperation.Finder.ByPathPrefix, pathPrefix);
    }else{
      LOG.debug("The last component of the path is not dir. ignoring the request to look in on " +
              "going STO table");
    }
  }

  @Override
  protected final Type getType() {
    return Type.SubTreePath;
  }

  public TransactionLockTypes.LockType getLockType() {
    return lockType;
  }
}