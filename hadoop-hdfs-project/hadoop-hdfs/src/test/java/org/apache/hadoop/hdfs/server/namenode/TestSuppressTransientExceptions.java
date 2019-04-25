/*
 * Copyright (C) 2019 hops.io.
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
import io.hops.exception.OutOfDBExtentsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.transaction.handler.RequestHandler;
import io.hops.transaction.handler.TransactionalRequestHandler;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import io.hops.exception.LockUpgradeException;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestSuppressTransientExceptions {

  private static final Log LOG = LogFactory.getLog(TestSuppressTransientExceptions.class);

  @Test
  public void TestSuppressTransientExceptions() throws IOException {
    try {
      final Logger logger = Logger.getRootLogger();
      Logger.getLogger(RequestHandler.class).setLevel(Level.ALL);
      Logger.getLogger(TestSuppressTransientExceptions.class).setLevel(Level.ALL);

      final int MAX_RETRY = 2;
      Configuration conf = new HdfsConfiguration();
      RequestHandler.setRetryCount(MAX_RETRY);
      HdfsStorageFactory.resetDALInitialized();
      HdfsStorageFactory.setConfiguration(conf);
      HdfsStorageFactory.formatStorage();


      LOG.info("******* Test 1 ********");
      final LogVerificationAppender appender = new LogVerificationAppender();
      logger.addAppender(appender);
      IOException e = new TransientStorageException("TransientStorageException");
      runWithErrors(1, e, HDFSOperationType.TEST);
      assertTrue(getExceptionCount(appender.getLog(),e.getClass()) == 0);


      LOG.info("******* Test 2 ********");
      final LogVerificationAppender appender2 = new LogVerificationAppender();
      logger.addAppender(appender2);
      e = new TransientStorageException("TransientStorageException");
      runWithErrors(2, e, HDFSOperationType.TEST);
      assertTrue(getExceptionCount(appender2.getLog(),e.getClass()) == 0);


      LOG.info("******* Test 3 ********");
      int failures = 3;
      final LogVerificationAppender appender3 = new LogVerificationAppender();
      logger.addAppender(appender3);
      try {
        e = new TransientStorageException("TransientStorageException");
        runWithErrors(failures, e, HDFSOperationType.TEST);
        fail("Expecting Exception");
      }catch (Exception ex){
      }
      assertTrue(getExceptionCount(appender3.getLog(),e.getClass()) == failures);


      //Testing nontransient exceptions, i.e. OutOfDBExtentsException, LockUpgradeException
      LOG.info("******* Test 4 ********");
      failures = 100;
      final LogVerificationAppender appender4 = new LogVerificationAppender();
      logger.addAppender(appender4);
      try {
        e = new OutOfDBExtentsException("OutOfDBExtentsException");
        runWithErrors(failures, e, HDFSOperationType.TEST);
        fail();
      }catch (OutOfDBExtentsException ex){
      }
      assertTrue(getExceptionCount(appender4.getLog(),e.getClass()) == 1);  // for
      // HDFSOperationType.TEST the exception will not be suppressed


      LOG.info("******* Test 5 ********");
      failures = 100;
      final LogVerificationAppender appender5 = new LogVerificationAppender();
      logger.addAppender(appender5);
      try {
        e = new OutOfDBExtentsException("OutOfDBExtentsException");
        runWithErrors(failures, e, HDFSOperationType.COMPLETE_FILE);
        fail();
      }catch (OutOfDBExtentsException ex){
      }
      assertTrue(getExceptionCount(appender5.getLog(),e.getClass()) == 0);

      LOG.info("******* Test 6 ********");
      failures = 100;
      final LogVerificationAppender appender6 = new LogVerificationAppender();
      logger.addAppender(appender6);
      try {
        e = new LockUpgradeException("LockUpgradeException");
        runWithErrors(failures, e, HDFSOperationType.TEST);
        fail();
      }catch (LockUpgradeException ex){
      }
      assertTrue(getExceptionCount(appender6.getLog(),e.getClass()) == 1);  // for
      // HDFSOperationType.TEST the exception will not be suppressed


      LOG.info("******* Test 7 ********");
      failures = 100;
      final LogVerificationAppender appender7 = new LogVerificationAppender();
      logger.addAppender(appender7);
      try {
        e = new LockUpgradeException("LockUpgradeException");
        runWithErrors(failures, e, HDFSOperationType.GET_BLOCK_LOCATIONS);
        fail();
      }catch (LockUpgradeException ex){
      }
      assertTrue(getExceptionCount(appender7.getLog(),e.getClass()) == 0);


    } finally {
    }
  }

  int getExceptionCount(List<LoggingEvent> log, Class e){
    int count = 0;
    for (int i = 0; i < log.size(); i++) {
      if (log.get(i).getMessage().toString().contains(e.getCanonicalName())) {
        count++;
      }
    }
    return count;
  }

  void runWithErrors(final int failures, final IOException e, HDFSOperationType type) throws IOException {
    final AtomicInteger count = new AtomicInteger(0);
    HopsTransactionalRequestHandler completeFileHandler =
            new HopsTransactionalRequestHandler(type) {
              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
              }

              @Override
              public Object performTask() throws IOException {
                if (count.get() >= failures) {
                  return null;
                } else {
                  count.incrementAndGet();
                  throw e;
                }
              }
            };

    completeFileHandler.handle(this);
  }
}
