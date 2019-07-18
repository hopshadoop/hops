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

package org.apache.hadoop.yarn.util.resource;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Test class to verify all resource utility methods.
 */
public class TestResourceUtils {

  private File nodeResourcesFile;
  private File resourceTypesFile;

  static class ResourceFileInformation {
    String filename;
    int resourceCount;
    Map<String, String> resourceNameUnitsMap;

    public ResourceFileInformation(String name, int count) {
      filename = name;
      resourceCount = count;
      resourceNameUnitsMap = new HashMap<>();
    }
  }

  public static void addNewTypesToResources(String... resourceTypes) {
    // Initialize resource map
    Map<String, ResourceInformation> riMap = new HashMap<>();

    // Initialize mandatory resources
    riMap.put(ResourceInformation.MEMORY_URI, ResourceInformation.MEMORY_MB);
    riMap.put(ResourceInformation.VCORES_URI, ResourceInformation.VCORES);

    for (String newResource : resourceTypes) {
      riMap.put(newResource, ResourceInformation
          .newInstance(newResource, "", 0, ResourceTypes.COUNTABLE, 0,
              Integer.MAX_VALUE));
    }

    ResourceUtils.initializeResourcesFromResourceInformationMap(riMap);
  }

  @Before
  public void setup() {
    ResourceUtils.resetResourceTypes();
  }

  @After
  public void teardown() {
    if(nodeResourcesFile != null && nodeResourcesFile.exists()) {
      nodeResourcesFile.delete();
    }
    if(resourceTypesFile != null && resourceTypesFile.exists()) {
      resourceTypesFile.delete();
    }
  }

  private void testMemoryAndVcores(Map<String, ResourceInformation> res) {
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();
    Assert.assertTrue("Resource 'memory' missing", res.containsKey(memory));
    Assert.assertEquals("'memory' units incorrect",
        ResourceInformation.MEMORY_MB.getUnits(), res.get(memory).getUnits());
    Assert.assertEquals("'memory' types incorrect",
        ResourceInformation.MEMORY_MB.getResourceType(),
        res.get(memory).getResourceType());
    Assert.assertTrue("Resource 'vcores' missing", res.containsKey(vcores));
    Assert.assertEquals("'vcores' units incorrect",
        ResourceInformation.VCORES.getUnits(), res.get(vcores).getUnits());
    Assert.assertEquals("'vcores' type incorrect",
        ResourceInformation.VCORES.getResourceType(),
        res.get(vcores).getResourceType());
  }

  @Test
  public void testGetResourceTypes() throws Exception {

    Map<String, ResourceInformation> res = ResourceUtils.getResourceTypes();
    Assert.assertEquals(2, res.size());
    testMemoryAndVcores(res);
  }

  @Test
  public void testGetResourceTypesConfigs() throws Exception {

    Configuration conf = new YarnConfiguration();

    ResourceFileInformation testFile1 =
        new ResourceFileInformation("resource-types-1.xml", 2);
    ResourceFileInformation testFile2 =
        new ResourceFileInformation("resource-types-2.xml", 3);
    testFile2.resourceNameUnitsMap.put("resource1", "G");
    ResourceFileInformation testFile3 =
        new ResourceFileInformation("resource-types-3.xml", 3);
    testFile3.resourceNameUnitsMap.put("resource2", "");
    ResourceFileInformation testFile4 =
        new ResourceFileInformation("resource-types-4.xml", 5);
    testFile4.resourceNameUnitsMap.put("resource1", "G");
    testFile4.resourceNameUnitsMap.put("resource2", "m");
    testFile4.resourceNameUnitsMap.put("yarn.io/gpu", "");

    ResourceFileInformation[] tests = {testFile1, testFile2, testFile3,
        testFile4};
    Map<String, ResourceInformation> res;
    for (ResourceFileInformation testInformation : tests) {
      ResourceUtils.resetResourceTypes();
      File source = new File(
          conf.getClassLoader().getResource(testInformation.filename)
              .getFile());
      resourceTypesFile = new File(source.getParent(), "resource-types.xml");
      FileUtils.copyFile(source, resourceTypesFile);
      res = ResourceUtils.getResourceTypes();
      testMemoryAndVcores(res);
      Assert.assertEquals(testInformation.resourceCount, res.size());
      for (Map.Entry<String, String> entry : testInformation.resourceNameUnitsMap
          .entrySet()) {
        String resourceName = entry.getKey();
        Assert.assertTrue("Missing key " + resourceName,
            res.containsKey(resourceName));
        Assert.assertEquals(entry.getValue(), res.get(resourceName).getUnits());
      }
    }
  }

  @Test
  public void testGetResourceTypesConfigErrors() throws Exception {
    Configuration conf = new YarnConfiguration();

    String[] resourceFiles = {"resource-types-error-1.xml",
        "resource-types-error-2.xml", "resource-types-error-3.xml",
        "resource-types-error-4.xml"};
    for (String resourceFile : resourceFiles) {
      ResourceUtils.resetResourceTypes();
      try {
        File source =
            new File(conf.getClassLoader().getResource(resourceFile).getFile());
        resourceTypesFile = new File(source.getParent(), "resource-types.xml");
        FileUtils.copyFile(source, resourceTypesFile);
        ResourceUtils.getResourceTypes();
        Assert.fail("Expected error with file " + resourceFile);
      } catch (NullPointerException ne) {
        throw ne;
      } catch (Exception e) {
        //Test passed
      }
    }
  }

  @Test
  public void testInitializeResourcesMap() throws Exception {
    String[] empty = {"", ""};
    String[] res1 = {"resource1", "m"};
    String[] res2 = {"resource2", "G"};
    String[][] test1 = {empty};
    String[][] test2 = {res1};
    String[][] test3 = {res2};
    String[][] test4 = {res1, res2};

    String[][][] allTests = {test1, test2, test3, test4};

    for (String[][] test : allTests) {

      Configuration conf = new YarnConfiguration();
      String resSt = "";
      for (String[] resources : test) {
        resSt += (resources[0] + ",");
      }
      resSt = resSt.substring(0, resSt.length() - 1);
      conf.set(YarnConfiguration.RESOURCE_TYPES, resSt);
      for (String[] resources : test) {
        String name =
            YarnConfiguration.RESOURCE_TYPES + "." + resources[0] + ".units";
        conf.set(name, resources[1]);
      }
      Map<String, ResourceInformation> ret =
          ResourceUtils.resetResourceTypes(conf);

      // for test1, 4 - length will be 1, 4
      // for the others, len will be 3
      int len = 3;
      if (test == test1) {
        len = 2;
      } else if (test == test4) {
        len = 4;
      }

      Assert.assertEquals(len, ret.size());
      for (String[] resources : test) {
        if (resources[0].length() == 0) {
          continue;
        }
        Assert.assertTrue(ret.containsKey(resources[0]));
        ResourceInformation resInfo = ret.get(resources[0]);
        Assert.assertEquals(resources[1], resInfo.getUnits());
        Assert.assertEquals(ResourceTypes.COUNTABLE, resInfo.getResourceType());
      }
      // we must always have memory and vcores with their fixed units
      Assert.assertTrue(ret.containsKey("memory-mb"));
      ResourceInformation memInfo = ret.get("memory-mb");
      Assert.assertEquals("Mi", memInfo.getUnits());
      Assert.assertEquals(ResourceTypes.COUNTABLE, memInfo.getResourceType());
      Assert.assertTrue(ret.containsKey("vcores"));
      ResourceInformation vcoresInfo = ret.get("vcores");
      Assert.assertEquals("", vcoresInfo.getUnits());
      Assert
          .assertEquals(ResourceTypes.COUNTABLE, vcoresInfo.getResourceType());
    }
  }

  @Test
  public void testInitializeResourcesMapErrors() throws Exception {

    String[] mem1 = {"memory-mb", ""};
    String[] vcores1 = {"vcores", "M"};

    String[] mem2 = {"memory-mb", "m"};
    String[] vcores2 = {"vcores", "G"};

    String[] mem3 = {"memory", ""};

    String[][] test1 = {mem1, vcores1};
    String[][] test2 = {mem2, vcores2};
    String[][] test3 = {mem3};

    String[][][] allTests = {test1, test2, test3};

    for (String[][] test : allTests) {

      Configuration conf = new YarnConfiguration();
      String resSt = "";
      for (String[] resources : test) {
        resSt += (resources[0] + ",");
      }
      resSt = resSt.substring(0, resSt.length() - 1);
      conf.set(YarnConfiguration.RESOURCE_TYPES, resSt);
      for (String[] resources : test) {
        String name =
            YarnConfiguration.RESOURCE_TYPES + "." + resources[0] + ".units";
        conf.set(name, resources[1]);
      }
      try {
        ResourceUtils.initializeResourcesMap(conf);
        Assert.fail("resource map initialization should fail");
      } catch (Exception e) {
        //Test passed
      }
    }
  }

  @Test
  public void testGetResourceInformation() throws Exception {

    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypes(conf, "resource-types-4.xml");
    // testRun.put("node-resources-1.xml", Resource.newInstance(1024, 1));
    Resource test3Resources = Resource.newInstance(0, 0);
    test3Resources.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "Gi", 5L));
    test3Resources.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "m", 2L));
    test3Resources.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 1));
    testRun.put("node-resources-2.xml", test3Resources);

    for (Map.Entry<String, Resource> entry : testRun.entrySet()) {
      String resourceFile = entry.getKey();
      ResourceUtils.resetNodeResources();
      File source = new File(
          conf.getClassLoader().getResource(resourceFile).getFile());
      nodeResourcesFile = new File(source.getParent(), "node-resources.xml");
      FileUtils.copyFile(source, nodeResourcesFile);
      Map<String, ResourceInformation> actual = ResourceUtils
          .getNodeResourceInformation(conf);
      Assert.assertEquals(actual.size(),
          entry.getValue().getResources().length);
      for (ResourceInformation resInfo : entry.getValue().getResources()) {
        Assert.assertEquals(resInfo, actual.get(resInfo.getName()));
      }
    }
  }

  @Test
  public void testGetNodeResourcesConfigErrors() throws Exception {
    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypes(conf, "resource-types-4.xml");
    String invalidNodeResFiles[] = { "node-resources-error-1.xml"};

    for (String resourceFile : invalidNodeResFiles) {
      ResourceUtils.resetNodeResources();
      try {
        File source = new File(conf.getClassLoader().getResource(resourceFile).getFile());
        nodeResourcesFile = new File(source.getParent(), "node-resources.xml");
        FileUtils.copyFile(source, nodeResourcesFile);
        Map<String, ResourceInformation> actual = ResourceUtils.getNodeResourceInformation(conf);
        Assert.fail("Expected error with file " + resourceFile);
      } catch (NullPointerException ne) {
        throw ne;
      } catch (Exception e) {
        //Test passed
      }
    }
  }

  @Test
  public void testResourceNameFormatValidation() throws Exception {
    String[] validNames = new String[] {
        "yarn.io/gpu",
        "gpu",
        "g_1_2",
        "123.io/gpu",
        "prefix/resource_1",
        "a___-3",
        "a....b",
    };

    String[] invalidNames = new String[] {
        "asd/resource/-name",
        "prefix/-resource_1",
        "prefix/0123resource",
        "0123resource",
        "-resource_1",
        "........abc"
    };

    for (String validName : validNames) {
      ResourceUtils.validateNameOfResourceNameAndThrowException(validName);
    }

    for (String invalidName : invalidNames) {
      try {
        ResourceUtils.validateNameOfResourceNameAndThrowException(invalidName);
        Assert.fail("Expected to fail name check, the name=" + invalidName
            + " is illegal.");
      } catch (YarnRuntimeException e) {
        // Expected
      }
    }
  }

  @Test
  public void testGetResourceInformationWithDiffUnits() throws Exception {

    Configuration conf = new YarnConfiguration();
    Map<String, Resource> testRun = new HashMap<>();
    setupResourceTypes(conf, "resource-types-4.xml");
    Resource test3Resources = Resource.newInstance(0, 0);

    //Resource 'resource1' has been passed as 5T
    //5T should be converted to 5000G
    test3Resources.setResourceInformation("resource1",
        ResourceInformation.newInstance("resource1", "T", 5L));

    //Resource 'resource2' has been passed as 2M
    //2M should be converted to 2000000000m
    test3Resources.setResourceInformation("resource2",
        ResourceInformation.newInstance("resource2", "M", 2L));
    test3Resources.setResourceInformation("yarn.io/gpu",
        ResourceInformation.newInstance("yarn.io/gpu", "", 1));
    testRun.put("node-resources-3.xml", test3Resources);

    for (Map.Entry<String, Resource> entry : testRun.entrySet()) {
      String resourceFile = entry.getKey();
      ResourceUtils.resetNodeResources();
      File source = new File(
          conf.getClassLoader().getResource(resourceFile).getFile());
      nodeResourcesFile = new File(source.getParent(), "node-resources.xml");
      FileUtils.copyFile(source, nodeResourcesFile);
      Map<String, ResourceInformation> actual = ResourceUtils
          .getNodeResourceInformation(conf);
      Assert.assertEquals(actual.size(),
          entry.getValue().getResources().length);
      for (ResourceInformation resInfo : entry.getValue().getResources()) {
        Assert.assertEquals(resInfo, actual.get(resInfo.getName()));
      }
    }
  }

  public static String setupResourceTypes(Configuration conf, String filename)
      throws Exception {
    File source = new File(
        conf.getClassLoader().getResource(filename).getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    FileUtils.copyFile(source, dest);
    ResourceUtils.getResourceTypes();
    return dest.getAbsolutePath();
  }
}
