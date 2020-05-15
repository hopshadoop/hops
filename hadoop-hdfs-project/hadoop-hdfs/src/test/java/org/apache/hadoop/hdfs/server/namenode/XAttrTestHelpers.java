/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.security.SecureRandom;

public class XAttrTestHelpers {
  
  public static int getXAttrTableRowCount()
      throws IOException {
    return (Integer) new LightWeightRequestHandler(
        HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        XAttrDataAccess da = (XAttrDataAccess)
            HdfsStorageFactory.getDataAccess(XAttrDataAccess.class);
        return da.count();
      }
    }.handle();
  }
  
  public static String generateRandomXAttrName(int size){
    return RandomStringUtils.randomAlphanumeric(size);
  }
  
  public static byte[] generateRandomByteArrayWithRandomSize(int MAX_SIZE){
    int size = RandomUtils.nextInt(1, MAX_SIZE + 1);
    return generateRandomByteArray(size);
  }
  
  public static byte[] generateRandomByteArray(int size){
    SecureRandom random = new SecureRandom();
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }
}
