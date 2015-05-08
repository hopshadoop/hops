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
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.logging.Log;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
/*
 *
 * @author pushparaj
 */
public class TestFileAppend {

     static final int rootId = 2;
    static final Log LOG = LogFactory.getLog(TestFileCreationWithSnapShot.class);
    static Session session;
    static SessionFactory sessionFactory;
    static MysqlServerConnectorForTest mysqlConnector;

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
            MysqlServerConnectorForTest.getInstance().setConfiguration(propertyConf);

        } catch (IOException ex) {
            throw new StorageInitializtionException(ex);
        }
    }

    @AfterClass
    public static void closeSessionFactory() throws StorageException {
        sessionFactory.close();
        session.close();
        MysqlServerConnectorForTest.getInstance().closeSession();
    }
    
    @Test
    public void testFileAppend() throws IOException, SQLException{
         Configuration conf = new HdfsConfiguration();
        final int BYTES_PER_CHECKSUM = 1;
        final int PACKET_SIZE = 4;
        final int BLOCK_SIZE = 8 * PACKET_SIZE;
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);

        MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());
        
        
        //Create a file f2 with block size 8 bytes.
        Path p1 = new Path("/d1/f2");
        //Write 4 bytes[integer size] into the file.
       
        FSDataOutputStream out = dfs.create(p1);
        int i = 0xFFFFFFFF;
        //for (; i < blocks; i++) {
            out.write(i);
       // }
        out.close();
        
        
        //Take root level snapshot
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
        
        //Create append to the exisiting block.
        
         out = dfs.append(p1);
        out.write(i);
        out.close();
//      
//        QueryBuilder queryBuilder = session.getQueryBuilder();
//        QueryDomainType<InodeDTO> inodeQueryQDT = queryBuilder.createQueryDefinition(InodeDTO.class);
//        QueryDomainType<BlockInfoDTO> blockQueryQDT = queryBuilder.createQueryDefinition(BlockInfoDTO.class);
//        Predicate predicate;
//        Query<InodeDTO> inodeQuery;
//        Query<BlockInfoDTO> blockQuery;
//        List<InodeDTO> inodeResults;
//        List<BlockInfoDTO> blockResults;
//        int inodeId;
//
//        //Check backup record for inode
//        predicate = inodeQueryQDT.get("name").equal(inodeQueryQDT.param("nameParam")).and(inodeQueryQDT.get("status").equal(inodeQueryQDT.param("statusParam")));
//        inodeQueryQDT.where(predicate);
//        inodeQuery = session.createQuery(inodeQueryQDT);
//        inodeQuery.setParameter("nameParam", "f2");
//        inodeQuery.setParameter("statusParam", SnapShotConstants.Modified);
//        inodeResults = inodeQuery.getResultList();
//        assertTrue("Failed", inodeResults.size() == 1);
//        inodeId = inodeResults.get(0).getId();
//        //check backup record for inode
//        predicate = blockQueryQDT.get("inode_id").equal(blockQueryQDT.param("inodeIdParam")).and(blockQueryQDT.get("block_id").equal(blockQueryQDT.param("blockIdParam")));
//        blockQueryQDT.where(predicate);
//        blockQuery = session.createQuery(blockQueryQDT);
//        blockQuery.setParameter("inodeIdParam", inodeId);
//        blockQuery.setParameter("blockIdParam", 1);
//        blockResults = blockQuery.getResultList();
//        assertTrue("Failed", blockResults.size()==2);
//        int status[] = new int[2];
//        status[0] = blockResults.get(0).getStatus();
//        status[1] = blockResults.get(1).getStatus();
//        assertTrue("Failed",
//                (
//                (status[0]==SnapShotConstants.Modified&&status[1]==SnapShotConstants.Original) ||
//                (status[1]==SnapShotConstants.Modified&&status[0]==SnapShotConstants.Original)
//                )
//                );
                
        
       ResultSet inodes = executeQuery("select id from inodes where name='f2' and id>0");
       int inodeId=-1;
       if(inodes.next()){
           inodeId = inodes.getInt(1);
       }
       
       int blockId=-1;
       //Check the record for modified blockrow.
       ResultSet blocks  = executeQuery("select block_id, status from block_infos where inode_id="+inodeId);
          if(blocks.next()){
              blockId = blocks.getInt(1);
           assertTrue("failed",blocks.getInt(2)==SnapShotConstants.Modified);
       }
       //check the backUp row for the block above.
     blocks  = executeQuery("select block_id, status from block_infos where inode_id="+-inodeId);
          if(blocks.next()){
             
           assertTrue("failed",blocks.getInt(1)==-blockId&&blocks.getInt(2)==SnapShotConstants.Original);
       }     
          
    }
    
    ResultSet executeQuery(String query) throws StorageException{
        try {
            MysqlServerConnectorForTest connector = MysqlServerConnectorForTest.getInstance();
            Connection conn = connector.obtainSession();
            PreparedStatement s = conn.prepareStatement(query);
            ResultSet results = s.executeQuery();
           return results;
        } catch (SQLException ex) {
            throw new StorageException(ex);
        } 
    }
    
}
