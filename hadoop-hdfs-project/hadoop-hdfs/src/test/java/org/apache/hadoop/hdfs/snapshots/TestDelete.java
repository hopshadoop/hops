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

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;

/**
 *
 * @author pushparaj
 */
public class TestDelete extends BaseTest{
    /**
     * Delete a file from a directory which are existing before taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException, SQLException {
        FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];
       //Fille the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d2"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/d2/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p2 = new Path("/d1/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        //Deleted file "f3"
        dfs.delete(p2, false);
        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }

    /**
     * Delete a directory having files , which are existing[including directory] before taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete2() throws IOException, SQLException {

        FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d2"), dirPerm));

        //Create some files on path  "/d1/d2/" and write 1024 bytes into it.
        Path p1 = new Path("/d1/d2/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p2 = new Path("/d1/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        //Deleted directory "d2"
        dfs.delete(new Path("/d1/d2/"), true);
        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }

    /**
     * Delete a directory along with files in it,all of which, created after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete3() throws IOException, SQLException {

        FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];
        byte[] inBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d2"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        Path p1 = new Path("/d1/d2/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p2 = new Path("/d1/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        //Deleted file "f3"
        dfs.delete(p2, false);

        //Deleted directory "d2"

        dfs.delete(new Path("/d1/d2/"), true);
        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }

    /**
     * Delete a directory(A) created before taking snapshot which has some directories created before taking snapshot 
     * and some directories with files in it created after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete4() throws IOException, SQLException {
        FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];
        byte[] inBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d2"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        Path p1 = new Path("/d1/d2/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p2 = new Path("/d1/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d3"), dirPerm));

        Path p3 = new Path("/d1/d3/f4");
        //Write data into the file.
        out = dfs.create(p3);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p4 = new Path("/d1/d3/f5");
        //Write data into the file.
        out = dfs.create(p4);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        dfs.delete(new Path("/d1/"), true);
        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }

    /**
     * Delete a directory(A) created after taking snapshot which has some directories created before taking snapshot[via Move] 
     * and some directories with files in it created after taking snapshot. 
     *  We need move functionality here.
     * @throws IOException
     */
    @Test
    public void testDelete5() throws IOException, SQLException {
          FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];
        byte[] inBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d2"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        Path p1 = new Path("/d1/d2/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p2 = new Path("/d1/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Data dataBeforeSnapshot = getData();
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d3"), dirPerm));

        Path p3 = new Path("/d1/d3/f4");
        //Write data into the file.
        out = dfs.create(p3);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p4 = new Path("/d1/d3/f5");
        //Write data into the file.
        out = dfs.create(p4);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d3/d4"), dirPerm));
        
         Path p5 = new Path("/d1/d3/d4/f6");
        //Write data into the file.
        out = dfs.create(p5);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        Path p6 = new Path("/d1/d3/d4/f7");
        //Write data into the file.
        out = dfs.create(p6);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        dfs.rename(new Path("/d1/d2"),new Path("/d1/d3/d2/"),Options.Rename.NONE);

        dfs.delete(new Path("/d1/d3"), true);
        dfs.rollBack("Admin");

        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }
}
