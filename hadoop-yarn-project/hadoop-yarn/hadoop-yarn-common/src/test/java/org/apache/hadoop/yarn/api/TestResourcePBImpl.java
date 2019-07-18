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

package org.apache.hadoop.yarn.api;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class to handle various proto related tests for resources.
 */
public class TestResourcePBImpl {

  @Before
  public void setup() throws Exception {
    ResourceUtils.resetResourceTypes();

    String resourceTypesFile = "resource-types-4.xml";
    Configuration conf = new YarnConfiguration();
    TestResourceUtils.setupResourceTypes(conf, resourceTypesFile);
  }

  @After
  public void teardown() {
    Configuration conf = new YarnConfiguration();
    File source = new File(
        conf.getClassLoader().getResource("resource-types-4.xml").getFile());
    File dest = new File(source.getParent(), "resource-types.xml");
    if (dest.exists()) {
      dest.delete();
    }
  }

  @Test
  public void testEmptyResourcePBInit() throws Exception {
    Resource res = new ResourcePBImpl();
    // Assert to check it sets resource value and unit to default.
    Assert.assertEquals(0, res.getMemorySize());
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    Assert.assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }

  @Test
  public void testResourcePBInitFromOldPB() throws Exception {
    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).setVirtualCores(3)
            .build();
    // Assert to check it sets resource value and unit to default.
    Resource res = new ResourcePBImpl(proto);
    Assert.assertEquals(1024, res.getMemorySize());
    Assert.assertEquals(3, res.getVirtualCores());
    Assert.assertEquals(ResourceInformation.MEMORY_MB.getUnits(),
        res.getResourceInformation(ResourceInformation.MEMORY_MB.getName())
            .getUnits());
    Assert.assertEquals(ResourceInformation.VCORES.getUnits(),
        res.getResourceInformation(ResourceInformation.VCORES.getName())
            .getUnits());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGetMemory() {
    Resource res = new ResourcePBImpl();
    long memorySize = Integer.MAX_VALUE + 1L;
    res.setMemorySize(memorySize);

    assertEquals("No need to cast if both are long", memorySize,
        res.getMemorySize());
    assertEquals("Cast to Integer.MAX_VALUE if the long is greater than "
            + "Integer.MAX_VALUE", Integer.MAX_VALUE, res.getMemory());
  }

  @Test
  public void testGetVirtualCores() {
    Resource res = new ResourcePBImpl();
    long vcores = Integer.MAX_VALUE + 1L;
    res.getResourceInformation("vcores").setValue(vcores);

    assertEquals("No need to cast if both are long", vcores,
        res.getResourceInformation("vcores").getValue());
    assertEquals("Cast to Integer.MAX_VALUE if the long is greater than "
        + "Integer.MAX_VALUE", Integer.MAX_VALUE, res.getVirtualCores());
  }

  @Test
  public void testResourcePBWithExtraResources() throws Exception {

    //Resource 'resource1' has been passed as 4T
    //4T should be converted to 4000G
    YarnProtos.ResourceInformationProto riProto =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
            setName("resource1").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
        setValue(4).setUnits("T").setKey("resource1").build();

    YarnProtos.ResourceProto proto =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
        setVirtualCores(3).addResourceValueMap(riProto).build();
    Resource res = new ResourcePBImpl(proto);

    Assert.assertEquals(4000,
        res.getResourceInformation("resource1").getValue());
    Assert.assertEquals("G",
        res.getResourceInformation("resource1").getUnits());

    //Resource 'resource2' has been passed as 4M
    //4M should be converted to 4000000000m
    YarnProtos.ResourceInformationProto riProto1 =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
            setName("resource2").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
        setValue(4).setUnits("M").setKey("resource2").build();

    YarnProtos.ResourceProto proto1 =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
        setVirtualCores(3).addResourceValueMap(riProto1).build();
    Resource res1 = new ResourcePBImpl(proto1);

    Assert.assertEquals(4000000000L,
        res1.getResourceInformation("resource2").getValue());
    Assert.assertEquals("m",
        res1.getResourceInformation("resource2").getUnits());

    //Resource 'resource1' has been passed as 3M
    //3M should be converted to 0G
    YarnProtos.ResourceInformationProto riProto2 =
        YarnProtos.ResourceInformationProto.newBuilder().setType(
            YarnProtos.ResourceTypeInfoProto.newBuilder().
            setName("resource1").setType(
                YarnProtos.ResourceTypesProto.COUNTABLE).getType()).
        setValue(3).setUnits("M").setKey("resource1").build();

    YarnProtos.ResourceProto proto2 =
        YarnProtos.ResourceProto.newBuilder().setMemory(1024).
        setVirtualCores(3).addResourceValueMap(riProto2).build();
    Resource res2 = new ResourcePBImpl(proto2);

    Assert.assertEquals(0,
        res2.getResourceInformation("resource1").getValue());
    Assert.assertEquals("G",
        res2.getResourceInformation("resource1").getUnits());
  }
}
