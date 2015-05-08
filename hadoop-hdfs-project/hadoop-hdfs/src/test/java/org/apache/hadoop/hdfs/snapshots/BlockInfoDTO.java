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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import static io.hops.metadata.hdfs.TablesDef.BlockInfoTableDef.*;
/**
 *
 * @author pushparaj
 */

    @PersistenceCapable(table = "block_infos")
public interface BlockInfoDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int iNodeID);

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int idx);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numbytes);

    @Column(name = GENERATION_STAMP)
    long getGenerationStamp();

    void setGenerationStamp(long genstamp);

    @Column(name = BLOCK_UNDER_CONSTRUCTION_STATE)
    int getBlockUCState();

    void setBlockUCState(int BlockUCState);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long ts);

    @Column(name = PRIMARY_NODE_INDEX)
    int getPrimaryNodeIndex();

    void setPrimaryNodeIndex(int replication);

    @Column(name = BLOCK_RECOVERY_ID)
    long getBlockRecoveryId();

        void setBlockRecoveryId(long recoveryId);
        
        @Column(name=STATUS)
        int getStatus();
        
        void setStatus(int status);
}
