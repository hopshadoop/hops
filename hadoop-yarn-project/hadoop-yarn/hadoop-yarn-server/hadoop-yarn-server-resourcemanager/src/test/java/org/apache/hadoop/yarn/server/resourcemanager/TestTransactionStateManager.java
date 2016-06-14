/*
 * Copyright 2015 Apache Software Foundation.
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

package org.apache.hadoop.yarn.server.resourcemanager;

import io.hops.ha.common.*;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

public class TestTransactionStateManager {

  @Before
  public void setUp() throws Exception {
    Configuration conf = new YarnConfiguration();

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
  }
  /*
   * when the scheduler loose is status of leader it stop all activities and
   * some rpc wile not finish to be handled. In this case the
   * TransactionStateManager should be reseted in order to accecp new RPCs and 
   * not stay stuck on the non finishing RPCs
   */
  @Test
  public void testFailOver() throws Exception{
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.HOPS_BATCH_MAX_SIZE, 10);
    conf.setInt(YarnConfiguration.HOPS_BATCH_MAX_DURATION, 60000);
    MockRM rm = new MockRM(conf);
    rm.start();
    Assert.assertTrue("the transactionStateManager should be running", 
            rm.getRMContext().getTransactionStateManager().isRunning());
    //fill the resouceTrackerService with RPCs.
    for(int i=0;i<10;i++){
      rm.getRMContext().getTransactionStateManager().
              getCurrentTransactionStatePriority(i, "test");
    }
    Assert.assertTrue("the transactionStateManager should block new RPCs", 
            rm.getRMContext().getTransactionStateManager().isFullBatch());
    
    //loose leader status
    rm.transitionToStandby(true);
    Assert.assertTrue("the transactionStateManager should be running", 
            rm.getRMContext().getTransactionStateManager().isRunning());
    Assert.assertFalse("the transactionStateManager should now be empty", 
            rm.getRMContext().getTransactionStateManager().isFullBatch());
  }
}
