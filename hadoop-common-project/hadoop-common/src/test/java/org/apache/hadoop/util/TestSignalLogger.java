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

package org.apache.hadoop.util;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSignalLogger {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestSignalLogger.class);
  
  @Test(timeout=60000)
  public void testInstall() throws Exception {
    Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
    SignalLogger.INSTANCE.register(LogAdapter.create(LOG));
    try {
      SignalLogger.INSTANCE.register(LogAdapter.create(LOG));
      Assert.fail("expected IllegalStateException from double registration");
    } catch (IllegalStateException e) {
      // fall through
    }
  }
}
