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
import java.io.IOException;
import java.io.InputStream;
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
public class TestDelete {

    private static final int rootId = 2;
    private static final Log LOG = LogFactory.getLog(TestFileCreationWithSnapShot.class);
    private static Session session;
    private final int BUFFER_SIZE = 1024;
    private final int BLOCK_SIZE = 512;
    private static SessionFactory sessionFactory;
    private DistributedFileSystem dfs;
    private MiniDFSCluster cluster;

    //Initiate loggers.
    {
        //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    }

    @BeforeClass
    public static void initializeNDBConnection() throws ClassNotFoundException, StorageInitializtionException {

        try {
            Properties propertyConf = new Properties();
            InputStream inStream = TestFileCreationWithSnapShot.class.getClassLoader().getResourceAsStream("ndb-config.properties");
            propertyConf.load(inStream);

            LOG.info("Database connect string: " + propertyConf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
            LOG.info("Database name: " + propertyConf.get(Constants.PROPERTY_CLUSTER_DATABASE));
            LOG.info("Max Transactions: " + propertyConf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));

            sessionFactory = ClusterJHelper.getSessionFactory(propertyConf);

            session = sessionFactory.getSession();

        } catch (IOException ex) {
            throw new StorageInitializtionException(ex);
        }
    }

    @AfterClass
    public static void closeSessionFactory() {
        sessionFactory.close();
        session.close();
    }

    @Before
    public void initCluster() throws IOException {
        Configuration conf = new HdfsConfiguration();
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());
    }

    @After
    public void shutDownCluster() {
        cluster.shutdown();
    }

    /**
     * Delete a file from a directory which are existing before taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        FSDataOutputStream out;
        Predicate predicate;
        QueryBuilder queryBuilder=session.getQueryBuilder() ;
        
        QueryDomainType<InodeDTO> queryDomainType=queryBuilder.createQueryDefinition(InodeDTO.class);
        Query<InodeDTO> query;       
        List<InodeDTO> results;
        
        QueryDomainType<BlockInfoDTO> blockInfoQueryDomainType =queryBuilder.createQueryDefinition(BlockInfoDTO.class);
        Query<BlockInfoDTO> blockInfoquery;
        List<BlockInfoDTO> blockInfoResults;
        
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

        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        //Get block info details for the file "f3"
        /*
        ArrayList<BlockInfoRecord> blockRecordsBeforeDeletion ;        
       
        predicate = (blockInfoQueryDomainType.get("inode_id").equal(blockInfoQueryDomainType.param("idParam")));
        blockInfoQueryDomainType.where(predicate);
        blockInfoquery = session.createQuery(blockInfoQueryDomainType);
        blockInfoquery.setParameter("idParam", 6);
        blockInfoResults = blockInfoquery.getResultList();
        
        blockRecordsBeforeDeletion = TestUtils.convertToBlockInfoRecords(blockInfoResults);
        */
        //Deleted file "f3"
        dfs.delete(p2, false);

        //We expect one backup record for d2 and isDeleted set to 1 for "f3"
        ArrayList<INodeRecord> expectedResults = new ArrayList<INodeRecord>();
        //expected record for 'root'
        expectedResults.add(new INodeRecord(2, 1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd1'
        expectedResults.add(new INodeRecord(3, 2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd2'
        expectedResults.add(new INodeRecord(4, 3, "d2", SnapShotConstants.isNotDeleted, SnapShotConstants.Modified));
        //expected record for 'd2' [BackUp INodeRecord or Copy-on Write INodeRecord]
        expectedResults.add(new INodeRecord(-4, -3, "d2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
         //expected record for 'f2'
        expectedResults.add(new INodeRecord(5, 4, "f2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
         //expected record for 'f3'
        expectedResults.add(new INodeRecord(6, 4, "f3", SnapShotConstants.isDeleted, SnapShotConstants.Original));

        //Get all the rows from the inodes.
        predicate = (queryDomainType.get("id").greaterThan(queryDomainType.param("idParam"))).or((queryDomainType.get("id").lessThan(queryDomainType.param("idParam"))));
        queryDomainType.where(predicate);
        query = session.createQuery(queryDomainType);
        query.setParameter("idParam", 0);
        results = query.getResultList();
        
        //Check the size of the return list from query with expected result list
        assertEquals("Size doesn't match. Failed",expectedResults.size(), results.size() );
        
        //Construct record list from results returned from query.
        ArrayList<INodeRecord> inodeRows = TestUtils.convertToINodeRecords(results);
        
        for(INodeRecord inode: expectedResults){
            assertTrue("Expected result not found.Failed",inodeRows.contains(inode) );
        }
       
        //Check block's of "f3" are not modified or deleted.
        /*
        ArrayList<BlockInfoRecord> blockRecordsAfterDeletion;
        blockInfoResults = blockInfoquery.getResultList();
        blockRecordsAfterDeletion = TestUtils.convertToBlockInfoRecords(blockInfoResults);
        
         assertEquals("Size of BlockInfo Records  doesn't match. Failed",blockRecordsBeforeDeletion.size(), blockRecordsAfterDeletion.size());
        
         for(BlockInfoRecord blockInfo: blockRecordsBeforeDeletion){
            assertTrue("Expected result not found.Failed",blockRecordsAfterDeletion.contains(blockInfo) );
        }
         */
    }

    /**
     * Delete a directory having files , which are existing[including directory] before taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete2() throws IOException {

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

        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }

        //Deleted directory "d2"
        dfs.delete(new Path("/d1/d2/"), true);
        
        //Validate results
        QueryBuilder queryBuilder = session.getQueryBuilder();
        QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
        Predicate predicate;
        Query<InodeDTO> query;
        List<InodeDTO> results;
        InodeDTO result;

        ArrayList<INodeRecord> expectedResults = new ArrayList<INodeRecord>();
        //expected record for 'root'
        expectedResults.add(new INodeRecord(2, 1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd1'
        expectedResults.add(new INodeRecord(3, 2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Modified));
         //expected record for 'd1' [BackUp INodeRecord or Copy-on Write INodeRecord]
        expectedResults.add(new INodeRecord(-3, -2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd2'
        expectedResults.add(new INodeRecord(4, 3, "d2", SnapShotConstants.isDeleted, SnapShotConstants.Original));
        //expected record for 'f2'
        expectedResults.add(new INodeRecord(5, 4, "f2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
         //expected record for 'f3'
        expectedResults.add(new INodeRecord(6, 4, "f3", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));

        //Get all the rows from the inodes.
        predicate = (queryDomainType.get("id").greaterThan(queryDomainType.param("idParam"))).or((queryDomainType.get("id").lessThan(queryDomainType.param("idParam"))));
        queryDomainType.where(predicate);
        query = session.createQuery(queryDomainType);
        query.setParameter("idParam", 0);
        results = query.getResultList();
        
        //Check the size of the return list from query with expected result list
        assertEquals("Size doesn't match. Failed", results.size(), expectedResults.size());
        
        //Construct record list from results returned from query.
        ArrayList<INodeRecord> inodeRows = TestUtils.convertToINodeRecords(results);
        
        for(INodeRecord inode: expectedResults){
            assertTrue("Expected result not found.Failed",inodeRows.contains(inode) );
        }
       
    }

    /**
     * Delete a directory along with files in it,all of which, created after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete3() throws IOException {

        FSDataOutputStream out;

        byte[] outBuffer = new byte[BUFFER_SIZE];
        byte[] inBuffer = new byte[BUFFER_SIZE];

        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

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
        
        QueryBuilder queryBuilder = session.getQueryBuilder();
        QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
        Predicate predicate;
        Query<InodeDTO> query;
        List<InodeDTO> results;
        InodeDTO result;

        ArrayList<INodeRecord> expectedResults = new ArrayList<INodeRecord>();
        //expected record for 'root'
        expectedResults.add(new INodeRecord(2, 1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd1'
        expectedResults.add(new INodeRecord(3, 2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Modified));
         //expected record for 'd1' [BackUp INodeRecord or Copy-on Write INodeRecord], since d3 created in it after taking snapshot.
        expectedResults.add(new INodeRecord(-3, -2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd2'
        expectedResults.add(new INodeRecord(4, 3, "d2", SnapShotConstants.isDeleted, SnapShotConstants.New));
        //expected record for 'f2'
        expectedResults.add(new INodeRecord(5, 4, "f2", SnapShotConstants.isNotDeleted, SnapShotConstants.New));
         //expected record for 'f3'
        expectedResults.add(new INodeRecord(6, 4, "f3", SnapShotConstants.isDeleted, SnapShotConstants.New));
        
        
        
        //Get all the rows from the inodes.
        predicate = (queryDomainType.get("id").greaterThan(queryDomainType.param("idParam"))).or((queryDomainType.get("id").lessThan(queryDomainType.param("idParam"))));
        queryDomainType.where(predicate);
        query = session.createQuery(queryDomainType);
        query.setParameter("idParam", 0);
        results = query.getResultList();
        
        //Check the size of the return list from query with expected result list
        assertEquals("Size doesn't match. Failed", results.size(), expectedResults.size());
        
        //Construct record list from results returned from query.
        ArrayList<INodeRecord> inodeRows = TestUtils.convertToINodeRecords(results);
        
        for(INodeRecord inode: expectedResults){
            assertTrue("Expected result not found.Failed",inodeRows.contains(inode) );
        }
        
        
    }

    /**
     * Delete a directory(A) created before taking snapshot which has some directories created before taking snapshot 
     * and some directories with files in it created after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testDelete4() throws IOException {
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
        
        //Validate results
        QueryBuilder queryBuilder = session.getQueryBuilder();
        QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
        Predicate predicate;
        Query<InodeDTO> query;
        List<InodeDTO> results;
        InodeDTO result;

        ArrayList<INodeRecord> expectedResults = new ArrayList<INodeRecord>();
        //expected record for 'root'
        expectedResults.add(new INodeRecord(2, 1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Modified));
        //expected record for 'root'[Back Up record, since d1 is child of root, when child is deleted parent's modification time is changed,we
        // are creating a back-up copy[of root] and saving it.]
        expectedResults.add(new INodeRecord(-2, -1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd1'
        expectedResults.add(new INodeRecord(3, 2, "d1", SnapShotConstants.isDeleted, SnapShotConstants.Modified));
          //expected record for 'd1'
        expectedResults.add(new INodeRecord(-3, -2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
       //expected record for 'd2'
        expectedResults.add(new INodeRecord(4, 3, "d2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
       //expected record for 'd3'
        expectedResults.add(new INodeRecord(7, 3, "d3", SnapShotConstants.isNotDeleted, SnapShotConstants.New));
        //expected record for 'f2'
        expectedResults.add(new INodeRecord(5, 4, "f2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
         //expected record for 'f3'
        expectedResults.add(new INodeRecord(6, 4, "f3", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'f4'
        expectedResults.add(new INodeRecord(8, 7, "f4", SnapShotConstants.isNotDeleted, SnapShotConstants.New));
         //expected record for 'f5'
        expectedResults.add(new INodeRecord(9, 7, "f5", SnapShotConstants.isNotDeleted, SnapShotConstants.New));


        //Get all the rows from the inodes.
        predicate = (queryDomainType.get("id").greaterThan(queryDomainType.param("idParam"))).or((queryDomainType.get("id").lessThan(queryDomainType.param("idParam"))));
        queryDomainType.where(predicate);
        query = session.createQuery(queryDomainType);
        query.setParameter("idParam", 0);
        results = query.getResultList();
        
        //Check the size of the return list from query with expected result list
        assertEquals("Size doesn't match. Failed", results.size(), expectedResults.size());
        
        //Construct record list from results returned from query.
        ArrayList<INodeRecord> inodeRows = TestUtils.convertToINodeRecords(results);
        
        for(INodeRecord inode: expectedResults){
            assertTrue("Expected result not found.Failed",inodeRows.contains(inode) );
        }
        
    }

    /**
     * Delete a directory(A) created after taking snapshot which has some directories created before taking snapshot[via Move] 
     * and some directories with files in it created after taking snapshot. 
     *  We need move functionality here.
     * @throws IOException
     */
    @Test
    public void testDelete5() throws IOException{
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
        
         //Validate results
        QueryBuilder queryBuilder = session.getQueryBuilder();
        QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
        Predicate predicate;
        Query<InodeDTO> query;
        List<InodeDTO> results;
        InodeDTO result;

        ArrayList<INodeRecord> expectedResults = new ArrayList<INodeRecord>();
        //expected record for 'root'
        expectedResults.add(new INodeRecord(2, 1, "", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
       //expected record for 'd1'
        expectedResults.add(new INodeRecord(3, 2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Modified));
         //expected record for 'd1' [BackUp INodeRecord or Copy-on Write INodeRecord]
        expectedResults.add(new INodeRecord(-3, -2, "d1", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
        //expected record for 'd2'
        expectedResults.add(new INodeRecord(4, 7, "d2", SnapShotConstants.isDeleted, SnapShotConstants.Modified));
        //expected back-up record for 'd2'
         expectedResults.add(new INodeRecord(-4,-3, "d2", SnapShotConstants.isNotDeleted, SnapShotConstants.Original));
       //expected record for 'f2'
        expectedResults.add(new INodeRecord(5, 4, "f2", SnapShotConstants.isDeleted, SnapShotConstants.Original));
         //expected record for 'f3'
        expectedResults.add(new INodeRecord(6, 4, "f3", SnapShotConstants.isDeleted, SnapShotConstants.Original));

        //Get all the rows from the inodes.
        predicate = (queryDomainType.get("id").greaterThan(queryDomainType.param("idParam"))).or((queryDomainType.get("id").lessThan(queryDomainType.param("idParam"))));
        queryDomainType.where(predicate);
        query = session.createQuery(queryDomainType);
        query.setParameter("idParam", 0);
        results = query.getResultList();
        
        //Check the size of the return list from query with expected result list
        assertEquals("Size doesn't match. Failed", results.size(), expectedResults.size());
        
        //Construct record list from results returned from query.
        ArrayList<INodeRecord> inodeRows = TestUtils.convertToINodeRecords(results);
        
        for(INodeRecord inode: expectedResults){
            assertTrue("Expected result not found.Failed",inodeRows.contains(inode) );
        }
        
        
    }
}
