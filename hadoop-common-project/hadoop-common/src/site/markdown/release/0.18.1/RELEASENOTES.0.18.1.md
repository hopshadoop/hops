
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop  0.18.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-3995](https://issues.apache.org/jira/browse/HADOOP-3995) | *Blocker* | **renameTo(src, dst) does not restore src name in case of quota failure.**

In case of quota failure on HDFS, rename does not restore source filename.


---

* [HADOOP-4060](https://issues.apache.org/jira/browse/HADOOP-4060) | *Blocker* | **[HOD] Make HOD to roll log files on the client**

HOD client was modified to roll over client logs being written to the cluster directory. A new configuration parameter, hod.log-rollover-count, was introduced to specify how many rollover log files to retain.



