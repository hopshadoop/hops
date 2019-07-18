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

export default {
  initMockNodesData: function(ref) {
    var data = [];
    for (var i = 0; i < 3; i++) {
      for (var j = 0; j < 38; j++) {
        var node = ref.get('targetObject.store').createRecord('YarnRmNode', {
          rack: "/rack-" + i,
          nodeHostName: "hadoop-" + ["centos6", "ubuntu7", "win"][i % 3] + "-" + ["web", "etl", "dm"][j % 3] + "-" + j,
          usedMemoryMB: Math.abs(Math.random() * 10000),
          availMemoryMB: Math.abs(Math.random() * 10000)
        });
        data.push(node);
      }
    }

    ref.set("model", data);
  }
};