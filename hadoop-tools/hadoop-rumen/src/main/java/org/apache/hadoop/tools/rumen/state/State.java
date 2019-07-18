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
package org.apache.hadoop.tools.rumen.state;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents a state. This state is managed by {@link StatePool}.
 * 
 * Note that a {@link State} objects should be persistable. Currently, the 
 * {@link State} objects are persisted using the Jackson JSON library. Hence the
 * implementors of the {@link State} interface should be careful while defining 
 * their public setter and getter APIs.  
 */
public interface State {
  /**
   * Returns true if the state is updated since creation (or reload).
   */
  @JsonIgnore
  boolean isUpdated();
  
  /**
   * Get the name of the state.
   */
  public String getName();
  
  /**
   * Set the name of the state.
   */
  public void setName(String name);
}
