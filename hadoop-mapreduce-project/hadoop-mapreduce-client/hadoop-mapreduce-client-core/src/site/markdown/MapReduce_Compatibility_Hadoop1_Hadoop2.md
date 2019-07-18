<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Apache Hadoop MapReduce - Migrating from Apache Hadoop 1.x to Apache Hadoop 2.x
===============================================================================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Introduction
------------

This document provides information for users to migrate their Apache Hadoop MapReduce applications from Apache Hadoop 1.x to Apache Hadoop 2.x.

In Apache Hadoop 2.x we have spun off resource management capabilities into Apache Hadoop YARN, a general purpose, distributed application management framework while Apache Hadoop MapReduce (aka MRv2) remains as a pure distributed computation framework.

In general, the previous MapReduce runtime (aka MRv1) has been reused and no major surgery has been conducted on it. Therefore, MRv2 is able to ensure satisfactory compatibility with MRv1 applications. However, due to some improvements and code refactorings, a few APIs have been rendered backward-incompatible.

The remainder of this page will discuss the scope and the level of backward compatibility that we support in Apache Hadoop MapReduce 2.x (MRv2).

Binary Compatibility
--------------------

First, we ensure binary compatibility to the applications that use old **mapred** APIs. This means that applications which were built against MRv1 **mapred** APIs can run directly on YARN without recompilation, merely by pointing them to an Apache Hadoop 2.x cluster via configuration.

Source Compatibility
--------------------

We cannot ensure complete binary compatibility with the applications that use **mapreduce** APIs, as these APIs have evolved a lot since MRv1. However, we ensure source compatibility for **mapreduce** APIs that break binary compatibility. In other words, users should recompile their applications that use **mapreduce** APIs against MRv2 jars. One notable binary incompatibility break is Counter and CounterGroup.

Not Supported
-------------

MRAdmin has been removed in MRv2 because because `mradmin` commands no longer exist. They have been replaced by the commands in `rmadmin`. We neither support binary compatibility nor source compatibility for the applications that use this class directly.

Tradeoffs between MRv1 Users and Early MRv2 Adopters
----------------------------------------------------

Unfortunately, maintaining binary compatibility for MRv1 applications may lead to binary incompatibility issues for early MRv2 adopters, in particular Hadoop 0.23 users. For **mapred** APIs, we have chosen to be compatible with MRv1 applications, which have a larger user base. For **mapreduce** APIs, if they don't significantly break Hadoop 0.23 applications, we still change them to be compatible with MRv1 applications. Below is the list of MapReduce APIs which are incompatible with Hadoop 0.23.

| **Problematic Function** | **Incompatibility Issue** |
|:---- |:---- |
| `org.apache.hadoop.util.ProgramDriver#drive` | Return type changes from `void` to `int` |
| `org.apache.hadoop.mapred.jobcontrol.Job#getMapredJobID` | Return type changes from `String` to `JobID` |
| `org.apache.hadoop.mapred.TaskReport#getTaskId` | Return type changes from `String` to `TaskID` |
| `org.apache.hadoop.mapred.ClusterStatus#UNINITIALIZED_MEMORY_VALUE` | Data type changes from `long` to `int` |
| `org.apache.hadoop.mapreduce.filecache.DistributedCache#getArchiveTimestamps` | Return type changes from `long[]` to `String[]` |
| `org.apache.hadoop.mapreduce.filecache.DistributedCache#getFileTimestamps` | Return type changes from `long[]` to `String[]` |
| `org.apache.hadoop.mapreduce.Job#failTask` | Return type changes from `void` to `boolean` |
| `org.apache.hadoop.mapreduce.Job#killTask` | Return type changes from `void` to `boolean` |
| `org.apache.hadoop.mapreduce.Job#getTaskCompletionEvents` | Return type changes from `o.a.h.mapred.TaskCompletionEvent[]` to `o.a.h.mapreduce.TaskCompletionEvent[]` |

Malicious
---------

For the users who are going to try `hadoop-examples-1.x.x.jar` on YARN, please note that `hadoop -jar hadoop-examples-1.x.x.jar` will still use `hadoop-mapreduce-examples-2.x.x.jar`, which is installed together with other MRv2 jars. By default Hadoop framework jars appear before the users' jars in the classpath, such that the classes from the 2.x.x jar will still be picked. Users should remove `hadoop-mapreduce-examples-2.x.x.jar` from the classpath of all the nodes in a cluster. Otherwise, users need to set `HADOOP_USER_CLASSPATH_FIRST=true` and `HADOOP_CLASSPATH=...:hadoop-examples-1.x.x.jar` to run their target examples jar, and add the following configuration in `mapred-site.xml` to make the processes in YARN containers pick this jar as well.

        <property>
            <name>mapreduce.job.user.classpath.first</name>
            <value>true</value>
        </property>
