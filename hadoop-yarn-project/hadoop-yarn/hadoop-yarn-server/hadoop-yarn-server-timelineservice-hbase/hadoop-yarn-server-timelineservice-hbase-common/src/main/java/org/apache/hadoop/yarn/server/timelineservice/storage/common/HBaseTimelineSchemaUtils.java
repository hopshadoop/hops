/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

import java.text.NumberFormat;

/**
 * A bunch of utility functions used in HBase TimelineService common module.
 */
public final class HBaseTimelineSchemaUtils {
  /** milliseconds in one day. */
  public static final long MILLIS_ONE_DAY = 86400000L;

  private static final ThreadLocal<NumberFormat> APP_ID_FORMAT =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(4);
          return fmt;
        }
      };

  private HBaseTimelineSchemaUtils() {
  }

  /**
   * Combines the input array of attributes and the input aggregation operation
   * into a new array of attributes.
   *
   * @param attributes Attributes to be combined.
   * @param aggOp Aggregation operation.
   * @return array of combined attributes.
   */
  public static Attribute[] combineAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    int newLength = getNewLengthCombinedAttributes(attributes, aggOp);
    Attribute[] combinedAttributes = new Attribute[newLength];

    if (attributes != null) {
      System.arraycopy(attributes, 0, combinedAttributes, 0, attributes.length);
    }

    if (aggOp != null) {
      Attribute a2 = aggOp.getAttribute();
      combinedAttributes[newLength - 1] = a2;
    }
    return combinedAttributes;
  }

  /**
   * Returns a number for the new array size. The new array is the combination
   * of input array of attributes and the input aggregation operation.
   *
   * @param attributes Attributes.
   * @param aggOp Aggregation operation.
   * @return the size for the new array
   */
  private static int getNewLengthCombinedAttributes(Attribute[] attributes,
      AggregationOperation aggOp) {
    int oldLength = getAttributesLength(attributes);
    int aggLength = getAppOpLength(aggOp);
    return oldLength + aggLength;
  }

  private static int getAppOpLength(AggregationOperation aggOp) {
    if (aggOp != null) {
      return 1;
    }
    return 0;
  }

  private static int getAttributesLength(Attribute[] attributes) {
    if (attributes != null) {
      return attributes.length;
    }
    return 0;
  }

  /**
   * Converts an int into it's inverse int to be used in (row) keys
   * where we want to have the largest int value in the top of the table
   * (scans start at the largest int first).
   *
   * @param key value to be inverted so that the latest version will be first in
   *          a scan.
   * @return inverted int
   */
  public static int invertInt(int key) {
    return Integer.MAX_VALUE - key;
  }

  /**
   * returns the timestamp of that day's start (which is midnight 00:00:00 AM)
   * for a given input timestamp.
   *
   * @param ts Timestamp.
   * @return timestamp of that day's beginning (midnight)
   */
  public static long getTopOfTheDayTimestamp(long ts) {
    long dayTimestamp = ts - (ts % MILLIS_ONE_DAY);
    return dayTimestamp;
  }

  /**
   * Checks if passed object is of integral type(Short/Integer/Long).
   *
   * @param obj Object to be checked.
   * @return true if object passed is of type Short or Integer or Long, false
   * otherwise.
   */
  public static boolean isIntegralValue(Object obj) {
    return (obj instanceof Short) || (obj instanceof Integer) ||
        (obj instanceof Long);
  }

  /**
   * A utility method that converts ApplicationId to string without using
   * FastNumberFormat in order to avoid the incompatibility issue caused
   * by mixing hadoop-common 2.5.1 and hadoop-yarn-api 3.0 in this module.
   * This is a work-around implementation as discussed in YARN-6905.
   *
   * @param appId application id
   * @return the string representation of the given application id
   *
   */
  public static String convertApplicationIdToString(ApplicationId appId) {
    StringBuilder sb = new StringBuilder(64);
    sb.append(ApplicationId.appIdStrPrefix);
    sb.append("_");
    sb.append(appId.getClusterTimestamp());
    sb.append('_');
    sb.append(APP_ID_FORMAT.get().format(appId.getId()));
    return sb.toString();
  }
}
