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


import com.mysql.clusterj.Constants;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.exception.StorageException;

/**
 * This class presents a singleton connector to Mysql Server.
 * It creates connections to Mysql Server and loads the driver.
 * 
 * @author hooman
 */
public class MysqlServerConnectorForTest  {

  private static MysqlServerConnectorForTest instance;
  private Log log;
  private String protocol;
  private String user;
  private String password;
  private ThreadLocal<Connection> connectionPool = new ThreadLocal<Connection>();
  public static final String DRIVER = "com.mysql.jdbc.Driver";

  private MysqlServerConnectorForTest() {
    log = LogFactory.getLog(MysqlServerConnectorForTest.class);
  }

  public static MysqlServerConnectorForTest getInstance(){
    if(instance == null){
      instance = new MysqlServerConnectorForTest();
    }
    return instance;
  }

  public void setConfiguration(Properties conf) throws StorageException {
    this.protocol = conf.getProperty(Constants.PROPERTY_JDBC_URL);
    this.user = conf.getProperty(Constants.PROPERTY_JDBC_USERNAME);
    this.password = conf.getProperty(Constants.PROPERTY_JDBC_PASSWORD);
    loadDriver();
  }

  private void loadDriver() throws StorageException {
    try {
      Class.forName(DRIVER).newInstance();
      log.info("Loaded Mysql driver.");
    } catch (ClassNotFoundException cnfe) {
      log.error("\nUnable to load the JDBC driver " + DRIVER, cnfe);
      throw new StorageException(cnfe);
    } catch (InstantiationException ie) {
      log.error("\nUnable to instantiate the JDBC driver " + DRIVER, ie);
      throw new StorageException(ie);
    } catch (IllegalAccessException iae) {
      log.error("\nNot allowed to access the JDBC driver " + DRIVER, iae);
      throw new StorageException(iae);
    }
  }

  
  public Connection obtainSession() throws StorageException {
    Connection conn = connectionPool.get();
    if (conn == null) {
      try {
        conn = DriverManager.getConnection(protocol, user, password);
        connectionPool.set(conn);
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
    return conn;
  }
  
  public void closeSession() throws StorageException
  {
    Connection conn = connectionPool.get();
    if (conn != null) {
      try {
        conn.close();
        connectionPool.remove();
      } catch (SQLException ex) {
        throw new StorageException(ex);
      }
    }
  }
  
  public static void truncateTable(String tableName) throws StorageException, SQLException{
    MysqlServerConnectorForTest connector = MysqlServerConnectorForTest.getInstance();
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement("delete from "+tableName);
      s.executeUpdate();
    } finally {
      connector.closeSession();
    }
  }

  
}

