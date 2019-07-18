/*
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

/**
 * Basic services for the YARN registry
 * <ul>
 *   <li>
 *     The {@link org.apache.hadoop.registry.server.services.RegistryAdminService}
 *     extends the shared YARN Registry client with registry setup and
 *     (potentially asynchronous) administrative actions.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.registry.server.services.MicroZookeeperService}
 *     is a transient Zookeeper instance bound to the YARN service lifecycle.
 *     It is suitable for testing.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.registry.server.services.AddingCompositeService}
 *     extends the standard YARN composite service by making its add and remove
 *     methods public. It is a utility service used in parts of the codebase
 *   </li>
 * </ul>
 */
package org.apache.hadoop.registry.server.services;
