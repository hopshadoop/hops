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

/**
 * This class used in evaluating testcase validations[for BlockInfo table's rows] in this package.
 * @author pushparaj
 */
public class BlockInfoRecord {
    long id;
    int inodeId;
    int status;

    BlockInfoRecord(long id, int inodeId, int status) {
        this.id = id;
        this.inodeId = inodeId;
        this.status = status;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BlockInfoRecord)) {
            return false;
        } else {
            BlockInfoRecord otherRecord = (BlockInfoRecord) other;
            /*
             * It can be done one statement like 
             * return this.id==otherRecord.id&& this.parentId==otherRecord.parentId
             * but to find which comparision failing when step throughing while debugging ,it is done like below.
             */
            if (this.id == otherRecord.id) {
                if (this.inodeId == otherRecord.inodeId) {
                    if (this.status == otherRecord.status) {
                        return true;
                    }

                }
            }

            return false;
        }

    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = (int) (67 * hash + this.id);
        hash = 67 * hash + this.inodeId;
        hash = 67 * hash + this.status;
        return hash;
    }
}
