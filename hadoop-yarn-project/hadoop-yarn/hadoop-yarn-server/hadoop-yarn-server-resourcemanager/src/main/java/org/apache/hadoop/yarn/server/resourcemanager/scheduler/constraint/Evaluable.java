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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint;

import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * A class implements Evaluable interface represents the internal state
 * of the class can be changed against a given target.
 * @param <T> a target to evaluate against
 */
public interface Evaluable<T> {

  /**
   * Evaluate against a given target, this process changes the internal state
   * of current class.
   *
   * @param target a generic type target that impacts this evaluation.
   * @throws YarnException
   */
  void evaluate(T target) throws YarnException;
}
