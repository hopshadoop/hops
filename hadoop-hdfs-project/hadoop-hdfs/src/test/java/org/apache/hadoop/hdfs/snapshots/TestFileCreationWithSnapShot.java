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
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;

/**
 * This class tests various cases during file creation with Snapshots.
 *
 * @author pushparaj
 */
public class TestFileCreationWithSnapShot {
    

    static final int rootId = INodeDirectory.ROOT_ID;
    static final Log LOG = LogFactory.getLog(TestFileCreationWithSnapShot.class);
    static Session session;
    static SessionFactory sessionFactory;

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

   

    /**
     * Test creation of a new directory and a file after taking snapshot.
     *
     * @throws IOException
     */
    @Test
    public void testFileCreationSimple() throws IOException {

        Configuration conf = new HdfsConfiguration();
        final int BYTES_PER_CHECKSUM = 1;
        final int PACKET_SIZE = BYTES_PER_CHECKSUM;
        final int BLOCK_SIZE = 1 * PACKET_SIZE;
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());

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


//Check whether inode rows inserted into the database as expected.
        try {
            //Contsruct the query to get the results given a inode name.
            QueryBuilder queryBuilder = session.getQueryBuilder();
            QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
            Predicate predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam"));

            queryDomainType.where(predicate);

            Query<InodeDTO> query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");

            List<InodeDTO> results = query.getResultList();

            assertTrue("Failed", results.size() == 1);

            InodeDTO result = results.get(0);
            int directoryId = result.getId();

            assertTrue("Failed", result.getStatus() == SnapShotConstants.New && result.getParentId() == rootId);

            query.setParameter("nameParam", "f2");

            results = query.getResultList();

            assertTrue("Failed", results.size() == 1);

            result = results.get(0);

            assertTrue("Failed", result.getStatus() == SnapShotConstants.New && result.getParentId() == directoryId);



        } catch (Exception e) {
            throw new StorageException(e);
        }


    }

    /**
     * Test creation of a file in a directory existing before taking snapshot.
     */
    @Test
    public void testFileCreation2() throws IOException {
        Configuration conf = new HdfsConfiguration();
        final int BYTES_PER_CHECKSUM = 1;
        final int PACKET_SIZE = BYTES_PER_CHECKSUM;
        final int BLOCK_SIZE = 1 * PACKET_SIZE;
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());
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

        //Check whether inode rows inserted into the database as expected.
        try {
            //Contsruct the query to get the results given a inode name.
            QueryBuilder queryBuilder = session.getQueryBuilder();
            QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
            Predicate predicate;
            Query<InodeDTO> query;
            List<InodeDTO> results;
            InodeDTO result;
            int directoryId;
            int backUp_inode_record_id;
            int backUp_inode_record_Parent_id;

            //There will be two rows for "d1" inode , one is saved record before adding the file to it, which changes its modification time and other record which is latest.
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam"));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            results = query.getResultList();
            assertTrue("Failed", results.size() == 2);


            //Check the modified record
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).and(queryDomainType.get("status").equal(queryDomainType.param("stsParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            query.setParameter("stsParam", SnapShotConstants.Modified);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            directoryId = result.getId();
            assertTrue("Failed", result.getParentId() == rootId);

            //Check the backup record
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).and(queryDomainType.get("status").equal(queryDomainType.param("stsParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            query.setParameter("stsParam", SnapShotConstants.Original);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            assertTrue("Failed", result.getParentId() == -rootId && result.getId() == -directoryId);

            //Check the newly created inode[file,"f2"]
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam"));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "f2");
            //There will be two rows for "d1" inode , one is saved record before adding the file to it, which changes its modification time and other record which is latest.
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            assertTrue("Failed", result.getStatus() == SnapShotConstants.New && result.getParentId() == directoryId);



        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    /**
     * Test creation of a file by over-writing a file which is existing before snapshot was taken.
     */
    @Test
    public void testFileCreation3() throws IOException {
        Configuration conf = new HdfsConfiguration();
        final int BYTES_PER_CHECKSUM = 1;
        final int PACKET_SIZE = BYTES_PER_CHECKSUM;
        final int BLOCK_SIZE = 1 * PACKET_SIZE;
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());

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

        try {
            //Contsruct the query to get the results given a inode name.
            QueryBuilder queryBuilder = session.getQueryBuilder();
            QueryDomainType<InodeDTO> queryDomainType = queryBuilder.createQueryDefinition(InodeDTO.class);
            Predicate predicate;
            Query<InodeDTO> query;
            List<InodeDTO> results;
            InodeDTO result;
            int directoryId;
            int backUp_inode_record_id;
            int backUp_inode_record_Parent_id;


            //We expect one back-up record for "d1" and other modified record[since we are over-writing a file in this directory, its modification time will be chaged
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam"));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            results = query.getResultList();
            assertTrue("Failed", results.size() == 2);
            
             //Check the modified record for "d1"
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).
                    and(queryDomainType.get("status").equal(queryDomainType.param("stsParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            query.setParameter("stsParam", SnapShotConstants.Modified);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            directoryId = result.getId();
            assertTrue("Failed", result.getParentId() == rootId);

            //Check the backup record for "d1"
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).
                    and(queryDomainType.get("status").equal(queryDomainType.param("stsParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "d1");
            query.setParameter("stsParam", SnapShotConstants.Original);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            assertTrue("Failed", result.getParentId() == -rootId && result.getId() == -directoryId);
            
            //Check the record for "f2" which represents the deleted file.[with isDeleted=true] since this has been overwritten.
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).
                    and(queryDomainType.get("status").equal(queryDomainType.param("stsParam"))).
                    and(queryDomainType.get("isDeleted").equal(queryDomainType.param("isDeltedParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "f2");
            query.setParameter("stsParam", SnapShotConstants.Original);
            query.setParameter("isDeltedParam", SnapShotConstants.isDeleted);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            assertTrue("Failed", result.getParentId() == directoryId);
            
            //Check the record for "f2" which is a new file after over-writing the existing file.
            predicate = queryDomainType.get("name").equal(queryDomainType.param("nameParam")).
                    and(queryDomainType.get("status").equal(queryDomainType.param("stsParam"))).
                    and(queryDomainType.get("isDeleted").equal(queryDomainType.param("isDeltedParam")));
            queryDomainType.where(predicate);
            query = session.createQuery(queryDomainType);
            query.setParameter("nameParam", "f2");
            query.setParameter("stsParam", SnapShotConstants.New);
            query.setParameter("isDeltedParam", SnapShotConstants.isNotDeleted);
            results = query.getResultList();
            assertTrue("Failed", results.size() == 1);
            result = results.get(0);
            assertTrue("Failed", result.getParentId() == directoryId);
            
        } catch (Exception e) {
            throw new StorageException(e);
        }


    }

}
