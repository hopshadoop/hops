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

import java.lang.reflect.Array;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility methods for dealing with Java Generics. 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GenericsUtil {

  /**
   * Returns the Class object (of type <code>Class&lt;T&gt;</code>) of the  
   * argument of type <code>T</code>. 
   * @param <T> The type of the argument
   * @param t the object to get it class
   * @return <code>Class&lt;T&gt;</code>
   */
  public static <T> Class<T> getClass(T t) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)t.getClass();
    return clazz;
  }

  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>.
   * @param c the Class object of the items in the list
   * @param list the list to convert
   */
  public static <T> T[] toArray(Class<T> c, List<T> list)
  {
    @SuppressWarnings("unchecked")
    T[] ta= (T[])Array.newInstance(c, list.size());

    for (int i= 0; i<list.size(); i++)
      ta[i]= list.get(i);
    return ta;
  }


  /**
   * Converts the given <code>List&lt;T&gt;</code> to a an array of 
   * <code>T[]</code>. 
   * @param list the list to convert
   * @throws ArrayIndexOutOfBoundsException if the list is empty. 
   * Use {@link #toArray(Class, List)} if the list may be empty.
   */
  public static <T> T[] toArray(List<T> list) {
    return toArray(getClass(list.get(0)), list);
  }

  /**
   * Determine whether the log of <code>clazz</code> is Log4j implementation.
   * @param clazz a class to be determined
   * @return true if the log of <code>clazz</code> is Log4j implementation.
   */
  public static boolean isLog4jLogger(Class<?> clazz) {
    if (clazz == null) {
      return false;
    }
    Logger log = LoggerFactory.getLogger(clazz);
    try {
      Class log4jClass = Class.forName("org.slf4j.impl.Log4jLoggerAdapter");
      return log4jClass.isInstance(log);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
