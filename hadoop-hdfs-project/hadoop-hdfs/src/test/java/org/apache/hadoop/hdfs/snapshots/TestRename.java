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
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import io.hops.exception.StorageInitializtionException;

/**
 *This class tests various cases of renaming
 * @author pushparaj
 */
public class TestRename {
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

    @Test
    public void testRenameWithOutSnapshot() throws IOException{
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
       //Fill the outBuffer with data.
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
        
        /*  assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1/d3"), dirPerm));
          Path p3 = new Path("/d1/d3/f4");
        //Write data into the file.
        out = dfs.create(p3);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        */
          dfs.rename(new Path("/d1/d2/f2"), new Path("/d1/d2/f3"),Options.Rename.OVERWRITE);

    }
    /* Rename file with in directory, where one of the files created after taking snapshot.
     * 
     */
    @Test
    public void testRenameFile1() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        
           //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d1/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();  
          
        //dfs.rename(new Path("/d1/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);
         dfs.rename(new Path("/d1/f2"), new Path("/d1/f3"),Options.Rename.OVERWRITE);
         
         
         
         
         
         

    }
    /*
     * Rename a file in directory[created after taking snapshot] to a file in directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile2() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        
           //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();  
          
        //dfs.rename(new Path("/d1/f2"), new Path("/d2/f3"),Options.Rename.OVERWRITE);
         dfs.rename(new Path("/d2/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);

    }
    
     /*
     * Rename a file in directory[created after taking snapshot] to a file in other directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile3() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        
           //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();  
          
        dfs.rename(new Path("/d1/f2"), new Path("/d2/f3"),Options.Rename.OVERWRITE);
        // dfs.rename(new Path("/d2/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);

    }
    
     /*
     * Rename a file in directory[created after taking snapshot] to a file in directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile4() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();

        
           //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();  
          
        dfs.rename(new Path("/d1/f2"), new Path("/d2/f3"),Options.Rename.OVERWRITE);
        // dfs.rename(new Path("/d2/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);

    }
    
     /*
     * Rename a directory with in its parent directory.
     */
    @Test
    public void testRenameFile5() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
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
        
           dfs.rename(new Path("/d1/f2"), new Path("/d2/f4"),Options.Rename.OVERWRITE);
          
       // dfs.rename(new Path("/d1/f2"), new Path("/d2/f3"),Options.Rename.OVERWRITE);
        // dfs.rename(new Path("/d2/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);

    }
    
    
    /*
     * Move directory to a directory created before taking snapshot.
     */
    @Test
    public void testRenameDirectory1() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
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
          
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
        

    }
    
    /*
     *Move directory created before taking snapshot to a directory created after taking snapshot.
     */
    @Test
    public void testRenameDirectory2() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close(); 
          
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.NONE);
     }
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory3() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
         assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d2/d1"), dirPerm));
                  
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
     }
    
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created before taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory4() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
         assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d2/d1"), dirPerm));
                  
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
            dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
     }
    
     /*
     *Move directory[A] created after taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory5() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
         assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d2/d1"), dirPerm));
                  
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
     }
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory6() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
     
         assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d2/d1"), dirPerm));
        
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
         //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
     }
    
    /*
     *Move directory to a directory, both are created after taking snapshot.
     */
    @Test
    public void testRenameDirectory7() throws Exception{
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
       //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        
             //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        
        //Create directory in the fileSystem.
        FsPermission dirPerm = new FsPermission((short) 0777);
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path("/d1"), dirPerm));

        //create some files on path  "/d1/d2/" and write 1024 bytes into it.
        
        Path p1 = new Path("/d1/f2");
        //Write data into the file.
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
        
        //Create a new file named 'f3'
                
        Path p2 = new Path("/d2/f3");
        //Write data into the file.
        out = dfs.create(p2);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close(); 
          
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.NONE);
     }
    
    
}
