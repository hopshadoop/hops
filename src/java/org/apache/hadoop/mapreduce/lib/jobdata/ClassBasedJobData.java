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

package org.apache.hadoop.mapreduce.lib.jobdata;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.SerializationBase;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Methods that configure the use of class-based serialization mechanisms
 * for intermediate and output types.
 *
 * This is the base for configuration of Writable, Java, AvroReflect,
 * and AvroSpecific serialization mechanisms.
 */
public class ClassBasedJobData {
  protected ClassBasedJobData() { }

  /**
   * Resolve the class which is a mapper output type.
   * Works for both key and value types; the exact parameters
   * to check are specified by the caller.
   *
   * We first check whether the class was set via the serialization
   * metadata API. If not, then fall back to the deprecated class-specifying
   * serialization API. If that's not set, then intermediate types are not
   * specified for this job; use the job's output types -- which may be the
   * default type, if those were not specified by the user either.
   *
   * @param conf The configuration to check.
   * @param metadataMapName the key identifying the serialization metadata map.
   * @param deprecatedClassKey the deprecated conf key identifying the class
   *   as set in the pre-metadata serialization API.
   * @param jobOutputClassKey the conf key identifying the job output class
   *   as set via the pre-metadata serialization API.
   * @param defaultValue the class to return if no return type can
   *   be found in the configuration.
   * @throws RuntimeException if the class could not be found.
   * @return the class which is the mapper output type.
   */
  private static Class<?> getMapOutputClass(Configuration conf,
      String metadataMapName, String deprecatedClassKey,
      String jobOutputClassKey, Class<?> defaultValue) {

    Map<String, String> metadata = conf.getMap(metadataMapName);
    String className = metadata.get(SerializationBase.CLASS_KEY);
    if (null == className) {
      // Not stored in a map. Might be in the deprecated parameter.
      className = conf.get(deprecatedClassKey);
    }

    if (null == className) {
      // Not set through either mechanism. Use the output value class.
      className = conf.get(jobOutputClassKey);
    }

    // Resolve this to a Class object.
    if (null == className) {
      return defaultValue; // Return default value class.
    } else {
      try {
        return conf.getClassByName(className);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(cnfe);
      }
    }
  }


  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * key class.
   *
   * @param theClass the map output key class.
   */
  public static void setMapOutputKeyClass(Configuration conf,
      Class<?> theClass) {
    Map<String, String> metadata =
        SerializationBase.getMetadataFromClass(theClass);
    conf.setMap(JobContext.MAP_OUTPUT_KEY_METADATA, metadata);
    conf.setBoolean(JobContext.MAP_OUTPUT_KEY_METADATA_SET, true);
  }

  /**
   * Get the key class for the map output data.
   * @return the map output key class name.
   */
  public static Class<?> getMapOutputKeyClass(Configuration conf) {
    return getMapOutputClass(conf, JobContext.MAP_OUTPUT_KEY_METADATA,
        JobContext.MAP_OUTPUT_KEY_CLASS,
        JobContext.OUTPUT_KEY_CLASS,
        LongWritable.class);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   *
   * @param theClass the map output value class.
   */
  public static void setMapOutputValueClass(Configuration conf,
      Class<?> theClass) {
    Map<String, String> metadata =
        SerializationBase.getMetadataFromClass(theClass);
    conf.setMap(JobContext.MAP_OUTPUT_VALUE_METADATA, metadata);
    conf.setBoolean(JobContext.MAP_OUTPUT_VALUE_METADATA_SET, true);
  }

  /**
   * Get the value class for the map output data.
   * @return the map output value class.
   */
  public static Class<?> getMapOutputValueClass(Configuration conf) {
    return getMapOutputClass(conf, JobContext.MAP_OUTPUT_VALUE_METADATA,
        JobContext.MAP_OUTPUT_VALUE_CLASS,
        JobContext.OUTPUT_VALUE_CLASS,
        Text.class);
  }
}
