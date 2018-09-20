/*
 * Copyright (C) 2015 hops.io.
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.hadoop.hdfs.server.namenode.TestHAFileCreation.LOG;

class Writer extends Thread {

  FileSystem fs;
  final Path threadDir;
  final Path filepath;
  boolean running = true;
  FSDataOutputStream outputStream = null;
  long datawrote = 0;
  boolean waitFileisClosed = true;
  long fileCloseWaitTile = -1;

  Writer(FileSystem fs, String fileName, boolean writeInSameDir, Path baseDir,
      boolean waitFileisClosed, long fileCloseWaitTile) {
    super(Writer.class.getSimpleName() + ":" + fileName + "_dir/" + fileName);
    this.waitFileisClosed = waitFileisClosed;
    this.fileCloseWaitTile = fileCloseWaitTile;
    this.fs = fs;
    if (writeInSameDir) {
      this.threadDir = baseDir;
    } else {
      this.threadDir = new Path(baseDir, fileName + "_dir");
    }
    this.filepath = new Path(threadDir, fileName);


    // creating the file here
    try {
      fs.mkdirs(threadDir);
      outputStream = this.fs.create(filepath);
    } catch (Exception ex) {
      LOG.info(getName() + " unable to create file [" + filepath + "]" + ex,
          ex);
      if (outputStream != null) {
        IOUtils.closeStream(outputStream);
        outputStream = null;
      }
    }
  }

  public void run() {

    int i = 0;
    if (outputStream != null) {
      try {
        for (; running; i++) {
          outputStream.writeInt(i);
          outputStream.flush();
          datawrote++;

          sleep(10);
        }
      } catch (InterruptedException e) {
      } catch (Exception e) {
        fail(getName() + " dies: e=" + e);
        LOG.info(getName() + " dies: e=" + e, e);
      } finally {
        IOUtils.closeStream(outputStream);
        if (!checkFileClosed()) {
          LOG.debug("File " + filepath + " close FAILED");
          fail("File " + filepath + " close FAILED");
        }
        LOG.debug("File " + filepath + " closed");
      }//end-finally
    }// end-outcheck
    else {
      LOG.info(getName() + " outstream was null for file  [" + filepath + "]");
    }
  }//end-run        

  private boolean checkFileClosed() {
    if (!waitFileisClosed) {
      return true;
    }
    int timePassed = 50;
    do {
      try {
        long len = fs.getFileStatus(filepath).getLen();
        if (len == datawrote * 4) {
          return true;
        }
      } catch (IOException ex) {
        return false; //incase of exception return false
      }
      timePassed *= 2;
    } while (timePassed <= fileCloseWaitTile);

    return false;
  }

  public Path getFilePath() {
    return filepath;
  }
  
  
  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  static void startWriters(Writer[] writers) {
    for (Writer writer : writers) {
      writer.start();
    }
  }

  static void stopWriters(Writer[] writers) throws InterruptedException {
    for (Writer writer1 : writers) {
      if (writer1 != null) {
        writer1.running = false;
        writer1.interrupt();
      }
    }
    for (Writer writer : writers) {
      if (writer != null) {
        writer.join();
      }
    }
  }

  // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  static void verifyFile(Writer[] writers, FileSystem fs) throws IOException {
    LOG.info("Verify the file");
    for (Writer writer : writers) {
      LOG.info(writer.filepath + ": length=" +
          fs.getFileStatus(writer.filepath).getLen());
      FSDataInputStream in = null;
      try {
        in = fs.open(writer.filepath);
        boolean eof = false;
        int j = 0, x = 0;
        long dataRead = 0;
        while (!eof) {
          try {
            x = in.readInt();
            dataRead++;
            assertEquals(j, x);
            j++;
          } catch (EOFException ex) {
            eof = true; // finished reading file
          }
        }
        if (writer.datawrote != dataRead) {
          LOG.debug("File length read lenght is not consistant. wrote " +
              writer.datawrote + " data read " + dataRead + " file path " +
              writer.filepath);
          fail("File length read lenght is not consistant. wrote " +
              writer.datawrote + " data read " + dataRead + " file path " +
              writer.filepath);
        }
      } catch (Exception ex) {
        fail("File varification failed for file: " + writer.filepath +
            " exception " + ex);
      } finally {
        IOUtils.closeStream(in);
      
      }
    }
  }

  public static void waitReplication(FileSystem fs, Writer[] writers,
      short replicationFactor, long timeout)
      throws IOException, TimeoutException {
  
    for (Writer writer : writers) {
      try {
        // increasing timeout to take into consideration 'ping' time with failed namenodes
        // if the client fetches for block locations from a dead NN, it would need to retry many times and eventually this time would cause a timeout
        // to avoid this, we set a larger timeout
        long expectedRetyTime = 20000; // 20seconds
        timeout = timeout + expectedRetyTime;
        DFSTestUtil.waitReplicationWithTimeout(fs, writer.getFilePath(),
            replicationFactor, timeout);
      
      } catch (ConnectException ex) {
        LOG.warn("Received Connect Exception (expected due to failure of NN)");
        ex.printStackTrace();
      }
    }
  }
}
