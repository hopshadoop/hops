package org.apache.hadoop.hdfs.snapshots;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.mysql.clusterj.*;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Created by pushparaj.motamari on 27/02/16.
 */
public class BaseTest {

    protected static Session session;
    protected static SessionFactory sessionFactory;
    protected static Log LOG = LogFactory.getLog(BaseTest.class);
    protected DistributedFileSystem dfs;
    protected MiniDFSCluster cluster;
    protected static final int BUFFER_SIZE = 1;
    protected static byte[] outBuffer = new byte[BUFFER_SIZE];
    protected FsPermission dirPerm = new FsPermission((short) 0777);
    protected static Configuration conf;
    protected static Connection mySqlConnection;
    protected static MysqlServerConnectorForTest mysqlServerConnector;

   static {
        String path = System.getProperty("java.library.path");
        //System.setProperty("java.library.path", "/Users/pushparaj.motamari/Desktop/ubuntu/github/mysql-cluster-gpl-7.4.4-osx10.9-x86_64/lib");
        System.setProperty("java.library.path", "/Users/pushparaj.motamari/Desktop/ubuntu/github/mysql-cluster-gpl-7.4.11-osx10.11-x86_64/lib");
        try {
            //http://blog.cedarsoft.com/2010/11/setting-java-library-path-programmatically/
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible(true);
            fieldSysPath.set(null, null);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        System.loadLibrary("ndbclient");
    }

    @BeforeClass
    public  static void initializeNDBConnection() throws ClassNotFoundException, StorageInitializtionException {

        try {
            addToClassPath(DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT);
            addToClassPath("/Users/pushparaj.motamari/.m2/repository/com/mysql/ndb/clusterj-part-key-fix/7.4.7/clusterj-part-key-fix-7.4.7.jar");
            Session s1;
            Properties propertyConf = new Properties();
            propertyConf.load(ClassLoader.getSystemResourceAsStream(DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT));

            LOG.info("Database connect string: " + propertyConf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
            LOG.info("Database name: " + propertyConf.get(Constants.PROPERTY_CLUSTER_DATABASE));
            LOG.info("Max Transactions: " + propertyConf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));

            sessionFactory = ClusterJHelper.getSessionFactory(propertyConf);

            session = sessionFactory.getSession();

            mysqlServerConnector = MysqlServerConnectorForTest.getInstance();
            mysqlServerConnector.setConfiguration(propertyConf);
            mySqlConnection = mysqlServerConnector.obtainConnection();

        } catch (IOException ex) {
            throw new StorageInitializtionException(ex);
        }

        //Fill the outBuffer with data.
        for (int i = 0; i < BUFFER_SIZE; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }

        //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
        ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);

        conf = new HdfsConfiguration();
        final int BYTES_PER_CHECKSUM = 1;
        final int PACKET_SIZE = BYTES_PER_CHECKSUM;
        final int BLOCK_SIZE = 1 * PACKET_SIZE;
        conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BYTES_PER_CHECKSUM);
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, PACKET_SIZE);
    }


    private static void addToClassPath(String s)
            throws StorageInitializtionException {
        try {
            File f = new File(s);
            URL u = f.toURI().toURL();
            URLClassLoader urlClassLoader =
                    (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class urlClass = URLClassLoader.class;
            Method method =
                    urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            method.invoke(urlClassLoader, new Object[]{u});
        } catch (MalformedURLException ex) {
            throw new StorageInitializtionException(ex);
        } catch (IllegalAccessException ex) {
            throw new StorageInitializtionException(ex);
        } catch (IllegalArgumentException ex) {
            throw new StorageInitializtionException(ex);
        } catch (InvocationTargetException ex) {
            throw new StorageInitializtionException(ex);
        } catch (NoSuchMethodException ex) {
            throw new StorageInitializtionException(ex);
        } catch (SecurityException ex) {
            throw new StorageInitializtionException(ex);
        }
    }

    @AfterClass
    public static void closeSessionFactory() throws StorageException {
        sessionFactory.close();
        session.close();
        mysqlServerConnector.closeConnection();
    }


    @Before
    public void initCluster() throws IOException {
        cluster = new MiniDFSCluster.Builder(conf).format(true).build();
        FileSystem fs = cluster.getFileSystem();
        dfs = (DistributedFileSystem) FileSystem.newInstance(fs.getUri(), fs.getConf());

    }

    @After
    public void shutDownCluster() {
        cluster.shutdown();
    }

    protected void takeSnapshotOnRoot() throws IOException{
        boolean isSnapShottingSuccess = dfs.takeRootLevelSnapshot("Admin");

        if (!isSnapShottingSuccess) {
            throw new IOException("Taking Snapshot at root failed");
        } else {
            System.out.println("Snapshotting was success");
        }
    }

    protected  Data getData() throws SQLException {
        Data data = new Data();
        data.setInodeList(getINodes());
        data.setInodeAttributeList(getINodeAttributes());
        data.setBlockInfoList(getBlockInfos());
        data.setBlockInfoList(getBlockInfos());

        return data;
    }

    private Map<String,inode> getINodes() throws SQLException {
        String inodesQuery =
                "SELECT id,parent_id,isdeleted,status,name FROM hdfs_inodes ";
        ResultSet resultSet = getResultSet(inodesQuery);
        Map<String,inode> inodes  = new HashMap<String,inode>();
        while(resultSet.next()){
            inodes.put(resultSet.getInt(2) +","+resultSet.getString(5),new inode(resultSet.getInt(1),resultSet.getInt(2), resultSet.getString(5), resultSet.getInt(3),resultSet.getInt(4)));
        }
        return inodes;

    }

    private Map<String,inodeAttribute> getINodeAttributes() throws SQLException {
        String inodeAttributesQuery =
                "SELECT inodeId,nscount, nsquota,dsquota,status FROM hdfs_inode_attributes";
        ResultSet resultSet = getResultSet(inodeAttributesQuery);
        Map<String,inodeAttribute> inodeAttributes  = new HashMap<String,inodeAttribute>();
        while(resultSet.next()){
            inodeAttributes.put(String.valueOf(resultSet.getInt(1)),new inodeAttribute(resultSet.getInt(1),resultSet.getLong(2),resultSet.getLong(3),resultSet.getLong(4),resultSet.getInt(5)));
        }
        return inodeAttributes;
    }

    private Map<String,blockInfo> getBlockInfos() throws SQLException {
        String blockInfosQuery =
                "SELECT block_id,inode_id,status FROM hdfs_block_infos ";
        ResultSet resultSet = getResultSet(blockInfosQuery);
        Map<String,blockInfo> blockInfos  = new HashMap<String,blockInfo>();
        while(resultSet.next()){
            blockInfos.put(resultSet.getInt(2)+","+resultSet.getInt(1),new blockInfo(resultSet.getLong(1),resultSet.getInt(2),resultSet.getInt(3)));
        }
        return blockInfos;
    }

    private ResultSet getResultSet(String query){
        try {
            PreparedStatement s = mySqlConnection.prepareStatement(query);
            ResultSet result = s.executeQuery();
            return  result;
        } catch (SQLException e) {
           throw new RuntimeException(e);
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, SQLException {
        /*BaseTest.initializeNDBConnection();
        BaseTest test = new BaseTest();
        test.initCluster();
        Data OldData = test.getData();*/

    }

    private class inode {
        private int id,parent_id,isdeleted,status;
        private String name;

        public inode(int id, int parent_id, String name, int isdeleted, int status) {
            this.id = id;
            this.parent_id = parent_id;
            this.isdeleted = isdeleted;
            this.status = status;
            this.name = name;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            inode inode = (inode) o;
            return id == inode.id &&
                    parent_id == inode.parent_id &&
                    isdeleted == inode.isdeleted &&
                    status == inode.status &&
                    Objects.equal(name, inode.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, parent_id, isdeleted, status, name);
        }
    }

    private class inodeAttribute {
        private  int inodeId,status;
        private long nsCount,nsQuota,dsQuota;

        public inodeAttribute(int inodeId, long nsCount, long nsQuota, long dsQuota, int status) {
            this.inodeId = inodeId;

            this.status = status;
            this.nsCount = nsCount;
            this.nsQuota = nsQuota;
            this.dsQuota = dsQuota;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            inodeAttribute that = (inodeAttribute) o;
            return inodeId == that.inodeId &&
                    status == that.status &&
                    nsCount == that.nsCount &&
                    nsQuota == that.nsQuota &&
                    dsQuota == that.dsQuota;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(inodeId, status, nsCount, nsQuota, dsQuota);
        }


    }
    private class blockInfo {
        private long block_id;
        private int inode_id,status;

        public blockInfo(long block_id, int inode_id, int status) {
            this.block_id = block_id;
            this.inode_id = inode_id;
            this.status = status;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            blockInfo blockInfo = (blockInfo) o;
            return block_id == blockInfo.block_id &&
                    inode_id == blockInfo.inode_id &&
                    status == blockInfo.status;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(block_id, inode_id, status);
        }
    }
    protected static class Data{
        private Map<String,inode> inodeList;
        private Map<String,inodeAttribute> inodeAttributeList;
        private Map<String,blockInfo> blockInfoList;

        public Map<String,inode> getInodeList() {
            return inodeList;
        }

        public void setInodeList(Map<String,inode> inodeList) {
            this.inodeList = inodeList;
        }


        public Map<String,inodeAttribute> getInodeAttributeList() {
            return inodeAttributeList;
        }

        public void setInodeAttributeList(Map<String,inodeAttribute> inodeAttributeList) {
            this.inodeAttributeList = inodeAttributeList;
        }

        public Map<String,blockInfo> getBlockInfoList() {
            return blockInfoList;
        }

        public void setBlockInfoList(Map<String,blockInfo> blockInfoList) {
            this.blockInfoList = blockInfoList;
        }

        @Override
        public boolean equals(Object o) {
            if(o!= null &&  (o instanceof Data )){
                Data newData = (Data)o;

                return compareInodes(newData.getInodeList()) &&
                        compareInodeAttributes(newData.getInodeAttributeList()) &&
                        compareBlockInfos(newData.getBlockInfoList());


            }else{
                return false;
            }

        }

        boolean compareInodes(Map<String,inode> newInodes){
            return this.inodeList.equals(newInodes);

        }
        boolean compareInodeAttributes(Map<String,inodeAttribute> newInodeAttributes){
            return this.inodeAttributeList.equals(newInodeAttributes);

        }
        boolean compareBlockInfos(Map<String,blockInfo> newBlockInfos){
            return  this.blockInfoList.equals(newBlockInfos);

        }

        public int getInodeId(String path) {
            String regEx = "/([^/]+)";
            Pattern pattern = Pattern.compile(regEx);
            Matcher matcher = pattern.matcher(path);
            int inodeId= INodeDirectory.ROOT_ID;
            while(matcher.find()){
                inodeId= inodeList.get(String.valueOf(inodeId)+","+matcher.group(1)).id;
            }
            return inodeId;
        }
    }

    protected boolean fetchFreshDataAndValidate(Data dataTobeValidted) throws SQLException {
        Data freshlyFetchedData = getData();
        boolean validity = freshlyFetchedData.equals(dataTobeValidted);
        return validity;
    }

    protected void createDirectory(String dirName, FsPermission permission) throws IOException{
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path(dirName), permission));
    }

    protected void createDirectory(String dirName) throws IOException{
        assertTrue("Creation of a directory failed", dfs.mkdirs(new Path(dirName), dirPerm));
    }

    protected void createAndWriteToFile(String fileName) throws IOException{
        FSDataOutputStream out;
        Path p1 = new Path(fileName);
        out = dfs.create(p1);
        out.write(outBuffer, 0, BUFFER_SIZE);
        out.close();
    }
    protected static class Constraint{
        protected enum Operation {Modified,Deleted} ;
        private Operation operation;
        private String path;
        protected Constraint(String path,Operation operation ){
            this.path = path;
            this.operation =operation;

        }
        protected Operation getOperation() {
            return operation;
        }

        protected void setOperation(Operation operation) {
            this.operation = operation;
        }

        protected String getPath() {
            return path;
        }

        protected void setPath(String path) {
            this.path = path;
        }
    }
    protected void validateConstraints(Data baseData, Data newData, List<Constraint> constraintList){
        for(Constraint constraint: constraintList){
            validateConstraint(baseData,newData,constraint);
        }
    }
    protected void validateConstraint(Data baseData, Data newData, Constraint constraint){
        String path = constraint.getPath();
        String regEx = "/([^/]+)/?\\z";
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(path);
        String inodeName = matcher.group(1);
        Constraint.Operation operation = constraint.getOperation();
        int inodeId = baseData.getInodeId(path);

        switch (operation){
            case Modified:

                break;
            case Deleted:
                break;
            default:
                throw new RuntimeException("Illegal Operation");
        }
    }
}
