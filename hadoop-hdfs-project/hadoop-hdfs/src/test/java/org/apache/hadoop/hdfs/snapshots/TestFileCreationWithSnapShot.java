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

import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This class tests various cases during file creation with Snapshots.
 *
 * @author pushparaj
 */
public class TestFileCreationWithSnapShot extends BaseTest{
     /**
     * Test creation of a new directory and a file after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testFileCreationSimple() throws IOException, SQLException {

        Data oldData = getData();
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        Path p1 = new Path("/d1/f2");

        int blocks = 1;
        FSDataOutputStream out = dfs.create(p1);

        int i = 0;
        for (; i < blocks; i++) {
            out.write(i);
        }
        out.close();

        dfs.rollBack("User");

        Data newData =  getData();
        boolean validity = newData.equals(oldData);
        assertTrue(validity);
    }

    /**
     * Test creation of a file in a directory existing before taking snapshot.
     */
    @Test
    public void testFileCreationSimple2() throws IOException, SQLException {

        dfs.mkdirs(new Path("/d1"));

        Data oldData = getData();

        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        Path p1 = new Path("/d1/f2");

        int blocks = 1;
        FSDataOutputStream out = dfs.create(p1);

        int i = 0;
        for (; i < blocks; i++) {
            out.write(i);
        }
        out.close();
        dfs.rollBack("Admin");

        Data newData =  getData();
        boolean validity = newData.equals(oldData);
        assertTrue(validity);
    }
    
    /**
     * Test creation of a file by over-writing a file which is existing before snapshot was taken.
     */
    @Test
    public void testFileCreationSimple3() throws IOException, SQLException {

        //Create a new file in the directory d1
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        int blocks = 1;
        FSDataOutputStream out = dfs.create(p1);
        int i = 0;
        for (; i < blocks; i++) {
            out.write(i);
        }
        out.close();

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        //Over-write the file f2.
        blocks = 1;
        out = dfs.create(p1);
        i = 0;
        for (; i < blocks; i++) {
            out.write(i);
        }
        out.close();

        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataBeforeSnapshot.equals(dataAfterRollBack);
        assertTrue(validity);
    }

}
