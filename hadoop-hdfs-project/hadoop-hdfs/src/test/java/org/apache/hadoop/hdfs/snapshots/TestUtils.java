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

import java.util.ArrayList;
import java.util.List;

/**
 *This class provides utility methods which are used in TestCases in this package.
 * @author pushparaj
 */
public class TestUtils {
    /**
     * This method converts the given list of InodeDTO objects into a list of INodeRecords.
     * @param results
     * @return
     */
     static ArrayList<INodeRecord> convertToINodeRecords(List<InodeDTO> results){
       
        ArrayList<INodeRecord> records = new ArrayList<INodeRecord>(results.size());
        
        for(InodeDTO row: results){
            records.add(new INodeRecord(row.getId(),row.getParentId(),row.getName(),row.getIsDeleted(),row.getStatus()));
        }
        
        return records;
    }
     
     /**
     * This method converts the given list of InodeDTO objects into a list of INodeRecords.
     * @param results
     * @return
     */
     static ArrayList<BlockInfoRecord> convertToBlockInfoRecords(List<BlockInfoDTO> results){
       
        ArrayList<BlockInfoRecord> records = new ArrayList<BlockInfoRecord>(results.size());
        
        for(BlockInfoDTO row: results){
            records.add(
                    new BlockInfoRecord(row.getBlockId(),row.getINodeId(),row.getStatus())
                    );
        }
        
        return records;
    }
}
