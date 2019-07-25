/*
 * Copyright 2016 Apache Software Foundation.
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
package io.hops.util;

import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.security.UsersGroups;
import io.hops.transaction.handler.AsyncLightWeightRequestHandler;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.transaction.handler.LightWeightRequestHandler;

public class DBUtility {

  private static final Log LOG = LogFactory.getLog(DBUtility.class);
    
  public static boolean InitializeDB() throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        boolean success = connector.formatStorage();
        UsersGroups.createSyncRow();
        LOG.debug("HOP :: Format storage has been completed: " + success);
        return success;
      }
    };
    return (boolean) setRMDTMasterKeyHandler.handle();
  }
}
