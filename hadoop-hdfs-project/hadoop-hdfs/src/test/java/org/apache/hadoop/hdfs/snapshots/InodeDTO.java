/*
 * Copyright 2014 Apache Software Foundation.
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
package org.apache.hadoop.hdfs.snapshots;

import com.mysql.clusterj.annotation.*;
import static io.hops.metadata.hdfs.TablesDef.INodeTableDef.*;

/**
 *
 * @author pushparaj
 */
@PersistenceCapable(table = TABLE_NAME)
@PartitionKey(column = PARENT_ID)
@Index(name = "inode_idx")
public interface InodeDTO {
    @Column(name = ID)
    int getId();     // id of the inode

    void setId(int id);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode

    void setName(String name);

    //id of the parent inode
    @PrimaryKey
    @Column(name = PARENT_ID)
    int getParentId();     // id of the inode

    void setParentId(int parentid);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();

    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();

    void setATime(long modificationTime);

    // Inode
    @Column(name = PERMISSION)
    byte[] getPermission();

    void setPermission(byte[] permission);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();

    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();

    void setClientMachine(String clientMachine);

    @Column(name = CLIENT_NODE)
    String getClientNode();

    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = GENERATION_STAMP)
    int getGenerationStamp();

    void setGenerationStamp(int generation_stamp);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();

    void setSymlink(String symlink);

    @Column(name = QUOTA_ENABLED)
    byte getQuotaEnabled();

    void setQuotaEnabled(byte quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    boolean getUnderConstruction();

    void setUnderConstruction(boolean underConstruction);

    @Column(name = SUBTREE_LOCKED)
    byte getSubtreeLocked();

    void setSubtreeLocked(byte locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();

    void setSubtreeLockOwner(long leaderId);

    @Column(name = ISDELETED)
    int getIsDeleted();

    void setIsDeleted(int isdeleted);

    @Column(name = STATUS)
    int getStatus();

    void setStatus(int Status);
}