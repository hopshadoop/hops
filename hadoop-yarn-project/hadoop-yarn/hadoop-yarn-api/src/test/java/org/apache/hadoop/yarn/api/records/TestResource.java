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
package org.apache.hadoop.yarn.api.records;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The class to test {@link Resource}.
 */
public class TestResource {

  @Test
  public void testCastToIntSafely() {
    assertEquals(0, Resource.castToIntSafely(0));
    assertEquals(1, Resource.castToIntSafely(1));
    assertEquals(Integer.MAX_VALUE,
        Resource.castToIntSafely(Integer.MAX_VALUE));

    assertEquals("Cast to Integer.MAX_VALUE if the long is greater than "
            + "Integer.MAX_VALUE", Integer.MAX_VALUE,
        Resource.castToIntSafely(Integer.MAX_VALUE + 1L));
    assertEquals("Cast to Integer.MAX_VALUE if the long is greater than "
            + "Integer.MAX_VALUE", Integer.MAX_VALUE,
        Resource.castToIntSafely(Long.MAX_VALUE));
  }
}
