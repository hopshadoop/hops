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
package org.apache.hadoop.mapreduce.util;

import java.io.File;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;

/**
 * A test for MRAsyncDiskService.
 */
public class TestMRAsyncDiskService extends TestCase {
  
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString();

  /**
   * This test creates one empty directory, and one directory with content, 
   * and then removes them through MRAsyncDiskService. 
   * @throws Throwable
   */
  public void testMRAsyncDiskService() throws Throwable {
  
    FileSystem localFileSystem = FileSystem.getLocal(new Configuration());
    String[] vols = new String[]{TEST_ROOT_DIR + "/0",
        TEST_ROOT_DIR + "/1"};
    MRAsyncDiskService service = new MRAsyncDiskService(
        localFileSystem, vols);
    
    String a = "a";
    String b = "b";
    String c = "b/c";
    
    File fa = new File(vols[0], a);
    File fb = new File(vols[1], b);
    File fc = new File(vols[1], c);
    
    // Create the directories
    fa.mkdirs();
    fb.mkdirs();
    fc.mkdirs();
    
    assertTrue(fa.exists());
    assertTrue(fb.exists());
    assertTrue(fc.exists());
    
    // Move and delete them
    service.moveAndDelete(vols[0], a);
    assertFalse(fa.exists());
    service.moveAndDelete(vols[1], b);
    assertFalse(fb.exists());
    assertFalse(fc.exists());
    
    // Sleep at most 5 seconds to make sure the deleted items are all gone.
    service.shutdown();
    if (!service.awaitTermination(5000)) {
      fail("MRAsyncDiskService is still not shutdown in 5 seconds!");
    }
    
    // All contents should be gone by now.
    for (int i = 0; i < 2; i++) {
      File toBeDeletedDir = new File(vols[0], MRAsyncDiskService.SUBDIR);
      String[] content = toBeDeletedDir.list();
      assertNotNull("Cannot find " + toBeDeletedDir, content);
      assertEquals("" + toBeDeletedDir + " should be empty now.", 
          0, content.length);
    }
  }
}
