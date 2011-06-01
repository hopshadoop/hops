/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file 
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestWriteRead {
  
  // junit test settings
  private static final int WR_NTIMES = 350;
  private static final int WR_CHUNK_SIZE = 10000;

  
  private static final int BUFFER_SIZE = 8192  * 100;
  private static final String ROOT_DIR = "/tmp/";
      
  // command-line options
  String filenameOption = ROOT_DIR + "fileX1";
  int chunkSizeOption = 10000;
  int loopOption = 10;
 
  
  private MiniDFSCluster cluster;
  private Configuration conf;   // = new HdfsConfiguration();
  private FileSystem mfs;       // = cluster.getFileSystem();
  private FileContext mfc;      // = FileContext.getFileContext();
  
   // configuration
  final boolean positionRead = false;   // position read vs sequential read
  private boolean useFCOption = false;  // use either FileSystem or FileContext
  private boolean verboseOption = true;

  static private Log LOG = LogFactory.getLog(TestWriteRead.class);

  @Before
  public void initJunitModeTest() throws Exception {
    LOG.info("initJunitModeTest");
   
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1024 * 100); //100K blocksize

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
   
    mfs = cluster.getFileSystem();
    mfc = FileContext.getFileContext();

    Path rootdir = new Path(ROOT_DIR);
    mfs.mkdirs(rootdir);
  }

  @After
  public void shutdown() {
    cluster.shutdown();
  }

  // Equivalence of @Before for cluster mode testing.
  private void initClusterModeTest() throws IOException {
    
    LOG = LogFactory.getLog(TestWriteRead.class);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.INFO);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.INFO);
    LOG.info("initClusterModeTest");

    conf = new Configuration();
    mfc = FileContext.getFileContext();
    mfs = FileSystem.get(conf);
  }

  /** Junit Test reading while writing. */
  @Test
  public void TestWriteRead1() throws IOException {
    String fname = filenameOption;
 
    // need to run long enough to fail: takes 25 to 35 seec on Mac
    int stat = testWriteAndRead(fname, WR_NTIMES, WR_CHUNK_SIZE);
    Assert.assertTrue(stat == 0);
  }

  // equivalent of TestWriteRead1
  private int clusterTestWriteRead1() throws IOException {
    int stat =   testWriteAndRead(filenameOption, loopOption, chunkSizeOption);
    return stat;
  }
  
  /**
   * Open the file to read from begin to end. Then close the file.
   * Return number of bytes read.
   * Support both sequential read and position read.
   */
  private long readData(String fname, byte[] buffer, long byteExpected)
  throws IOException {
    long totalByteRead = 0;
    long beginPosition = 0;
    Path path = getFullyQualifiedPath(fname);

    FSDataInputStream in = null;
    try {
      in = openInputStream(path);

      long visibleLenFromReadStream = getVisibleFileLength(in);

      totalByteRead = readUntilEnd(in, buffer, buffer.length, fname,
          beginPosition, visibleLenFromReadStream, positionRead);
      in.close();

      return  totalByteRead + beginPosition;

    } catch (IOException e) {
      throw new IOException("##### Caught Exception in readData. "
          + "Total Byte Read so far = " + totalByteRead 
          + " beginPosition = " + beginPosition, e);  
    } finally {
      if (in != null) 
        in.close();
    }
  }

  /**
   * read chunks into buffer repeatedly until total of VisibleLen byte are read 
   * Return total number of bytes read
   */
  private long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,String fname, 
      long pos, long visibleLen, boolean positionRead) throws IOException {

    if (pos >= visibleLen || visibleLen <= 0 )
      return 0;
    
    int chunkNumber = 0;
    long totalByteRead = 0;
    long currentPosition = pos;
    int byteRead = 0;
    long byteLeftToRead = visibleLen - pos;
    int byteToReadThisRound = 0;
    
    if (!positionRead){
      in.seek(pos);
      currentPosition = in.getPos();
    } 
    if (verboseOption)
      LOG.info("reader begin: position: " + pos
          + " ; currentOffset = " + currentPosition + " ; bufferSize ="
          + buffer.length + " ; Filename = " + fname);
    try {
      while (byteLeftToRead > 0 && currentPosition < visibleLen ) {
        byteToReadThisRound = (int) (byteLeftToRead >=  buffer.length ? 
            buffer.length : byteLeftToRead);
        if (positionRead) {
          byteRead = in.read(currentPosition, buffer, 0, byteToReadThisRound);
        } else {
          byteRead = in.read(buffer, 0, byteToReadThisRound);  
        }
        if (byteRead <= 0)
          break;
        chunkNumber++;
        totalByteRead += byteRead;
        currentPosition += byteRead;
        byteLeftToRead -= byteRead;
        
        if (verboseOption) {
          LOG.info("reader: Number of byte read: " + byteRead
              + " ; toatlByteRead = " + totalByteRead + " ; currentPosition="
              + currentPosition + " ; chunkNumber =" + chunkNumber
              + "; File name = " + fname);
        }
      }
    } catch (IOException e) {
      throw new IOException(
          "#### Exception caught in readUntilEnd: reader  currentOffset = "
          + currentPosition + " ; totalByteRead =" + totalByteRead
          + " ; latest byteRead = " + byteRead + "; visibleLen= "
          + visibleLen + " ; bufferLen = " + buffer.length
          + " ; Filename = " + fname, e);
    }

    if (verboseOption)
      LOG.info("reader end:   position: " + pos
          + " ; currentOffset = " + currentPosition + " ; totalByteRead ="
          + totalByteRead + " ; Filename = " + fname);

    return totalByteRead;
  }

  private int writeData(FSDataOutputStream out, byte[] buffer, int length)
      throws IOException {

    int totalByteWritten = 0;
    int remainToWrite = length;

    while (remainToWrite > 0) {
      int toWriteThisRound = remainToWrite > buffer.length ? buffer.length
          : remainToWrite;
      out.write(buffer, 0, toWriteThisRound);
      totalByteWritten += toWriteThisRound;
      remainToWrite -= toWriteThisRound;
    }
    return totalByteWritten;
  }

  /** 
   * Common routine to do position read while open the file for write.
   * After each iteration of write, do a read of the file from begin to end.
   * Return 0 on success, else number of failure.
   */
  private int testWriteAndRead(String fname, int loopN, int chunkSize)
      throws IOException {
    
    int countOfFailures = 0;
    long byteVisibleToRead = 0;
    FSDataOutputStream out = null;

    byte[] outBuffer = new byte[BUFFER_SIZE];
    byte[] inBuffer = new byte[BUFFER_SIZE];
    
    for (int i = 0; i < BUFFER_SIZE; i++) {
      outBuffer[i] = (byte) (i & 0x00ff);
    }

    try {
      Path path = getFullyQualifiedPath(fname);

      out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.CREATE)) : 
           mfs.create(path);

      long totalByteWritten = 0;
      long totalByteVisible = 0;
      long totalByteWrittenButNotVisible = 0;
      int byteWrittenThisTime;

      boolean toFlush;
      for (int i = 0; i < loopN; i++) {
        toFlush = (i % 2) == 0;

        byteWrittenThisTime = writeData(out, outBuffer, chunkSize);

        totalByteWritten += byteWrittenThisTime;

        if (toFlush) {
          out.hflush();
          totalByteVisible += byteWrittenThisTime
              + totalByteWrittenButNotVisible;
          totalByteWrittenButNotVisible = 0;
        } else {
          totalByteWrittenButNotVisible += byteWrittenThisTime;
        }

        if (verboseOption) {
         LOG.info("TestReadWrite - Written " + byteWrittenThisTime
              + ". Total written = " + totalByteWritten
              + ". TotalByteVisible = " + totalByteVisible + " to file "
              + fname);
        }
        byteVisibleToRead = readData(fname, inBuffer, totalByteVisible); 
        
        String readmsg;
         
        if (byteVisibleToRead >= totalByteVisible
            && byteVisibleToRead <= totalByteWritten) {
          readmsg = "pass: reader sees expected number of visible byte " 
              + byteVisibleToRead + " of file " + fname  + " [pass]";
        } else {
          countOfFailures++;
          readmsg = "fail: reader does not see expected number of visible byte " 
            + byteVisibleToRead + " of file " + fname  + " [fail]";
      }
       LOG.info(readmsg);
      }

      // test the automatic flush after close
      writeData(out, outBuffer, chunkSize);
      totalByteWritten += chunkSize;
      totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
      totalByteWrittenButNotVisible += 0;

      out.close();

      byteVisibleToRead = readData(fname, inBuffer, totalByteVisible);
      long lenFromFc = getFileLengthFromNN(path);

      String readmsg;
      if (byteVisibleToRead == totalByteVisible) {
        readmsg = "PASS: reader sees expected size of file " + fname
            + " after close. File Length from NN: " + lenFromFc + " [Pass]"; 
      } else {
        countOfFailures++;
        readmsg = "FAIL: reader sees is different size of file " + fname
            + " after close. File Length from NN: " + lenFromFc + " [Fail]"; 
      }
     LOG.info(readmsg);

    } catch (IOException e) {
      throw new IOException(
          "##### Caught Exception in testAppendWriteAndRead. Close file. " 
              + "Total Byte Read so far = " + byteVisibleToRead, e);
    } finally {
      if (out != null)
        out.close();
    }
    return -countOfFailures;
  }

  // //////////////////////////////////////////////////////////////////////
  // // helper function:
  // /////////////////////////////////////////////////////////////////////
  private FSDataInputStream openInputStream(Path path) throws IOException {
    FSDataInputStream in = useFCOption ? mfc.open(path) : mfs.open(path);
    return in;
  }

  // length of a file (path name) from NN.
  private long getFileLengthFromNN(Path path) throws IOException {
    FileStatus fileStatus = useFCOption ? 
        mfc.getFileStatus(path) : mfs.getFileStatus(path);
    return fileStatus.getLen();
  }

  private long getVisibleFileLength(FSDataInputStream in) throws IOException {
    DFSClient.DFSDataInputStream din = (DFSClient.DFSDataInputStream) in;
    return din.getVisibleLength();
  }

  private boolean ifExists(Path path) throws IOException {
    return useFCOption ? mfc.util().exists(path) : mfs.exists(path);
  }

  private Path getFullyQualifiedPath(String pathString) {
    return useFCOption ?  
      mfc.makeQualified(new Path(ROOT_DIR, pathString)) :
      mfs.makeQualified(new Path(ROOT_DIR, pathString));
  }

  private void usage(){
    System.out.println("Usage: -chunkSize nn -loop ntime  -f filename");
    System.exit(0);
  }
  
  private void getCmdLineOption(String[] args){
    for (int i = 0; i < args.length; i++){
      if (args[i].equals("-f")) {
        filenameOption = args[++i];
      } else if (args[i].equals("-chunkSize")){
        chunkSizeOption = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-loop")){
        loopOption = Integer.parseInt(args[++i]);
      } else { 
        usage();
      }
    }
    return;
  }

  /**
   * Entry point of the test when using a real cluster.
   * Usage: [-loop ntimes] [-chunkSize nn]  [-f filename]
   * -loop: iterate ntimes: each iteration consists of a write, then a read
   * -chunkSize: number of byte for each write
   * -f filename: filename to write and read
   * Default: ntimes = 10; chunkSize = 10000; filename = /tmp/fileX1
   */
  public static void main(String[] args) {
    try {
      TestWriteRead trw = new TestWriteRead();
      trw.initClusterModeTest();
      trw.getCmdLineOption(args);
      int stat = trw.clusterTestWriteRead1();
      
      if (stat == 0){
        System.out.println("Status: clusterTestWriteRead1 test PASS"); 
      } else {
        System.out.println("Status: clusterTestWriteRead1 test FAIL");    
      }
      System.exit(stat);
    } catch (IOException e) {
     LOG.info("#### Exception in Main");
      e.printStackTrace();
      System.exit(-2);
    }
  }
}
