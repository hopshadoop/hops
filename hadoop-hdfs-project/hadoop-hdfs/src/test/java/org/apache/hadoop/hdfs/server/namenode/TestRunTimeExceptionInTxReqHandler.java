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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import io.hops.transaction.handler.TransactionalRequestHandler;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestRunTimeExceptionInTxReqHandler {

  private static final Log LOG = LogFactory.getLog(TestRunTimeExceptionInTxReqHandler.class);

  {
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger(TestRunTimeExceptionInTxReqHandler.class).setLevel(Level.DEBUG);
    Logger.getLogger(TransactionalRequestHandler.class).setLevel(Level.TRACE);
    Logger.getLogger(RequestHandler.class).setLevel(Level.TRACE);
  }

  @Test
  public void TestSuppressTransientExceptions() throws IOException {
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.reset();
    HdfsStorageFactory.setConfiguration(conf);

    //Exception in setup phase
    HopsTransactionalRequestHandler txHandler =
            new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
              @Override
              public void setUp() throws IOException {
                throw new RuntimeException("Test");
              }

              @Override
              public Object performTask() throws IOException {
                return null;
              }

              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
              }
            };
    try {
      txHandler.handle();
      fail();
    } catch (RuntimeException e) {
      assert e.getMessage().compareToIgnoreCase("Test") == 0;
    }

    //Exception in perform phase
    txHandler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void setUp() throws IOException {
      }

      @Override
      public Object performTask() throws IOException {
        throw new RuntimeException("Test");
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
      }
    };
    try {
      txHandler.handle();
      fail();
    } catch (RuntimeException e) {
      assert e.getMessage().compareToIgnoreCase("Test") == 0;
    }

    //Exception in lock phase
    txHandler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void setUp() throws IOException {
      }

      @Override
      public Object performTask() throws IOException {
        return null;
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        throw new RuntimeException("Test");
      }
    };
    try {
      txHandler.handle();
      fail();
    } catch (RuntimeException e) {
      assert e.getMessage().compareToIgnoreCase("Test") == 0;
    }
  }
}
