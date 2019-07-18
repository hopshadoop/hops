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

* [Hadoop OpenStack Support: Swift Object Store](#Hadoop_OpenStack_Support:_Swift_Object_Store)
    * [Introduction](#Introduction)
    * [Features](#Features)
    * [Using the Hadoop Swift Filesystem Client](#Using_the_Hadoop_Swift_Filesystem_Client)
        * [Concepts: services and containers](#Concepts:_services_and_containers)
        * [Containers and Objects](#Containers_and_Objects)
        * [Eventual Consistency](#Eventual_Consistency)
        * [Non-atomic "directory" operations.](#Non-atomic_directory_operations.)
    * [Working with Swift Object Stores in Hadoop](#Working_with_Swift_Object_Stores_in_Hadoop)
        * [Swift Filesystem URIs](#Swift_Filesystem_URIs)
        * [Installing](#Installing)
        * [Configuring](#Configuring)
            * [Example: Rackspace US, in-cluster access using API key](#Example:_Rackspace_US_in-cluster_access_using_API_key)
            * [Example: Rackspace UK: remote access with password authentication](#Example:_Rackspace_UK:_remote_access_with_password_authentication)
            * [Example: HP cloud service definition](#Example:_HP_cloud_service_definition)
        * [General Swift Filesystem configuration options](#General_Swift_Filesystem_configuration_options)
            * [Blocksize fs.swift.blocksize](#Blocksize_fs.swift.blocksize)
            * [Partition size fs.swift.partsize](#Partition_size_fs.swift.partsize)
            * [Request size fs.swift.requestsize](#Request_size_fs.swift.requestsize)
            * [Connection timeout fs.swift.connect.timeout](#Connection_timeout_fs.swift.connect.timeout)
            * [Connection timeout fs.swift.socket.timeout](#Connection_timeout_fs.swift.socket.timeout)
            * [Connection Retry Count fs.swift.connect.retry.count](#Connection_Retry_Count_fs.swift.connect.retry.count)
            * [Connection Throttle Delay fs.swift.connect.throttle.delay](#Connection_Throttle_Delay_fs.swift.connect.throttle.delay)
            * [HTTP Proxy](#HTTP_Proxy)
        * [Troubleshooting](#Troubleshooting)
            * [ClassNotFoundException](#ClassNotFoundException)
            * [Failure to Authenticate](#Failure_to_Authenticate)
            * [Timeout connecting to the Swift Service](#Timeout_connecting_to_the_Swift_Service)
        * [Warnings](#Warnings)
        * [Limits](#Limits)
        * [Testing the hadoop-openstack module](#Testing_the_hadoop-openstack_module)

Hadoop OpenStack Support: Swift Object Store
============================================

Introduction
------------

[OpenStack](http://www.openstack.org/) is an open source cloud infrastructure which can be accessed from multiple public IaaS providers, and deployed privately. It offers infrastructure services such as VM hosting (Nova), authentication (Keystone) and storage of binary objects (Swift).

This module enables Apache Hadoop applications -including MapReduce jobs, read and write data to and from instances of the [OpenStack Swift object store](http://www.openstack.org/software/openstack-storage/).

To make it part of Apache Hadoop's default classpath, simply make sure that
HADOOP_OPTIONAL_TOOLS in hadoop-env.sh has 'hadoop-openstack' in the list.

Features
--------

* Read and write of data stored in a Swift object store

* Support of a pseudo-hierachical file system (directories, subdirectories and
  files)

* Standard filesystem operations: `create`, `delete`, `mkdir`,
  `ls`, `mv`, `stat`.

* Can act as a source of data in a MapReduce job, or a sink.

* Support for multiple OpenStack services, and multiple containers from a
  single service.

* Supports in-cluster and remote access to Swift data.

* Supports OpenStack Keystone authentication with password or token.

* Released under the Apache Software License

* Tested against the Hadoop 3.x and 1.x branches, against multiple public
  OpenStack clusters: Rackspace US, Rackspace UK, HP Cloud.

* Tested against private OpenStack clusters, including scalability tests of
  large file uploads.

Using the Hadoop Swift Filesystem Client
----------------------------------------

### Concepts: services and containers

OpenStack swift is an *Object Store*; also known as a *blobstore*. It stores arbitrary binary objects by name in a *container*.

The Hadoop Swift filesystem library adds another concept, the *service*, which defines which Swift blobstore hosts a container -and how to connect to it.

### Containers and Objects

*   Containers are created by users with accounts on the Swift filestore, and hold
    *objects*.

*   Objects can be zero bytes long, or they can contain data.

*   Objects in the container can be up to 5GB; there is a special support for
    larger files than this, which merges multiple objects in to one.

*   Each object is referenced by it's *name*; there is no notion of directories.

*   You can use any characters in an object name that can be 'URL-encoded'; the
    maximum length of a name is 1034 characters -after URL encoding.

*   Names can have `/` characters in them, which are used to create the illusion of
    a directory structure. For example `dir/dir2/name`. Even though this looks
    like a directory, *it is still just a name*. There is no requirement to have
    any entries in the container called `dir` or `dir/dir2`

*   That said. if the container has zero-byte objects that look like directory
    names above other objects, they can pretend to be directories. Continuing the
    example, a 0-byte object called `dir` would tell clients that it is a
    directory while `dir/dir2` or `dir/dir2/name` were present. This creates an
    illusion of containers holding a filesystem.

Client applications talk to Swift over HTTP or HTTPS, reading, writing and deleting objects using standard HTTP operations (GET, PUT and DELETE, respectively). There is also a COPY operation, that creates a new object in the container, with a new name, containing the old data. There is no rename operation itself, objects need to be copied -then the original entry deleted.

### Eventual Consistency

The Swift Filesystem is \*eventually consistent\*: an operation on an object may not be immediately visible to that client, or other clients. This is a consequence of the goal of the filesystem: to span a set of machines, across multiple datacenters, in such a way that the data can still be available when many of them fail. (In contrast, the Hadoop HDFS filesystem is \*immediately consistent\*, but it does not span datacenters.)

Eventual consistency can cause surprises for client applications that expect immediate consistency: after an object is deleted or overwritten, the object may still be visible -or the old data still retrievable. The Swift Filesystem client for Apache Hadoop attempts to handle this, in conjunction with the MapReduce engine, but there may be still be occasions when eventual consistency causes surprises.

### Non-atomic "directory" operations.

Hadoop expects some operations to be atomic, especially `rename()`, which is something the MapReduce layer relies on to commit the output of a job, renaming data from a temp directory to the final path. Because a rename is implemented as a copy of every blob under the directory's path, followed by a delete of the originals, the intermediate state of the operation will be visible to other clients. If two Reducer tasks to rename their temp directory to the final path, both operations may succeed, with the result that output directory contains mixed data. This can happen if MapReduce jobs are being run with *speculation* enabled and Swift used as the direct output of the MR job (it can also happen against Amazon S3).

Other consequences of the non-atomic operations are:

1.  If a program is looking for the presence of the directory before acting
    on the data -it may start prematurely. This can be avoided by using
    other mechanisms to co-ordinate the programs, such as the presence of a file
    that is written *after* any bulk directory operations.

2.  A `rename()` or `delete()` operation may include files added under
    the source directory tree during the operation, may unintentionally delete
    it, or delete the 0-byte swift entries that mimic directories and act
    as parents for the files. Try to avoid doing this.

The best ways to avoid all these problems is not using Swift as the filesystem between MapReduce jobs or other Hadoop workflows. It can act as a source of data, and a final destination, but it doesn't meet all of Hadoop's expectations of what a filesystem is -it's a *blobstore*.

Working with Swift Object Stores in Hadoop
------------------------------------------

Once installed, the Swift FileSystem client can be used by any Hadoop application to read from or write to data stored in a Swift container.

Data stored in Swift can be used as the direct input to a MapReduce job -simply use the `swift:` URL (see below) to declare the source of the data.

This Swift Filesystem client is designed to work with multiple Swift object stores, both public and private. This allows the client to work with different clusters, reading and writing data to and from either of them.

It can also work with the same object stores using multiple login details.

These features are achieved by one basic concept: using a service name in the URI referring to a swift filesystem, and looking up all the connection and login details for that specific service. Different service names can be defined in the Hadoop XML configuration file, so defining different clusters, or providing different login details for the same object store(s).

### Swift Filesystem URIs

Hadoop uses URIs to refer to files within a filesystem. Some common examples are:

        local://etc/hosts
        hdfs://cluster1/users/example/data/set1
        hdfs://cluster2.example.org:8020/users/example/data/set1

The Swift Filesystem Client adds a new URL type `swift`. In a Swift Filesystem URL, the hostname part of a URL identifies the container and the service to work with; the path the name of the object. Here are some examples

        swift://container.rackspace/my-object.csv
        swift://data.hpcloud/data/set1
        swift://dmitry.privatecloud/out/results

In the last two examples, the paths look like directories: it is not, they are simply the objects named `data/set1` and `out/results` respectively.

### Installing

The `hadoop-openstack` JAR must be on the classpath of the Hadoop program trying to talk to the Swift service. If installed in the classpath of the Hadoop MapReduce service, then all programs started by the MR engine will pick up the JAR automatically. This is the easiest way to give all Hadoop jobs access to Swift.

Alternatively, the JAR can be included as one of the JAR files that an application uses. This lets the Hadoop jobs work with a Swift object store even if the Hadoop cluster is not pre-configured for this.

The library also depends upon the Apache HttpComponents library, which must also be on the classpath.

### Configuring

To talk to a swift service, the user must must provide:

1.  The URL defining the container and the service.

2.  In the cluster/job configuration, the login details of that service.

Multiple service definitions can co-exist in the same configuration file: just use different names for them.

#### Example: Rackspace US, in-cluster access using API key

This service definition is for use in a Hadoop cluster deployed within Rackspace's US infrastructure.

        <property>
          <name>fs.swift.service.rackspace.auth.url</name>
          <value>https://auth.api.rackspacecloud.com/v2.0/tokens</value>
          <description>Rackspace US (multiregion)</description>
        </property>

        <property>
          <name>fs.swift.service.rackspace.username</name>
          <value>user4</value>
        </property>

        <property>
          <name>fs.swift.service.rackspace.region</name>
          <value>DFW</value>
        </property>

        <property>
          <name>fs.swift.service.rackspace.apikey</name>
          <value>fe806aa86dfffe2f6ed8</value>
        </property>

Here the API key visible in the account settings API keys page is used to log in. No property for public/private access -the default is to use the private endpoint for Swift operations.

This configuration also selects one of the regions, DFW, for its data.

A reference to this service would use the `rackspace` service name:

        swift://hadoop-container.rackspace/

#### Example: Rackspace UK: remote access with password authentication

This connects to Rackspace's UK ("LON") datacenter.

        <property>
          <name>fs.swift.service.rackspaceuk.auth.url</name>
          <value>https://lon.identity.api.rackspacecloud.com/v2.0/tokens</value>
          <description>Rackspace UK</description>
        </property>

        <property>
          <name>fs.swift.service.rackspaceuk.username</name>
          <value>user4</value>
        </property>

        <property>
          <name>fs.swift.service.rackspaceuk.password</name>
          <value>insert-password-here/value>
        </property>

        <property>
          <name>fs.swift.service.rackspace.public</name>
          <value>true</value>
        </property>

This is a public access point connection, using a password over an API key.

A reference to this service would use the `rackspaceuk` service name:

        swift://hadoop-container.rackspaceuk/

Because the public endpoint is used, if this service definition is used within the London datacenter, all accesses will be billed at the public upload/download rates, *irrespective of where the Hadoop cluster is*.

#### Example: HP cloud service definition

Here is an example that connects to the HP Cloud object store.

        <property>
          <name>fs.swift.service.hpcloud.auth.url</name>
          <value>https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/tokens
          </value>
          <description>HP Cloud</description>
        </property>

        <property>
          <name>fs.swift.service.hpcloud.tenant</name>
          <value>FE806AA86</value>
        </property>

        <property>
          <name>fs.swift.service.hpcloud.username</name>
          <value>FE806AA86DFFFE2F6ED8</value>
        </property>

        <property>
          <name>fs.swift.service.hpcloud.password</name>
          <value>secret-password-goes-here</value>
        </property>

        <property>
          <name>fs.swift.service.hpcloud.public</name>
          <value>true</value>
        </property>

A reference to this service would use the `hpcloud` service name:

        swift://hadoop-container.hpcloud/

### General Swift Filesystem configuration options

Some configuration options apply to the Swift client, independent of the specific Swift filesystem chosen.

#### Blocksize fs.swift.blocksize

Swift does not break up files into blocks, except in the special case of files over 5GB in length. Accordingly, there isn't a notion of a "block size" to define where the data is kept.

Hadoop's MapReduce layer depends on files declaring their block size, so that it knows how to partition work. Too small a blocksize means that many mappers work on small pieces of data; too large a block size means that only a few mappers get started.

The block size value reported by Swift, therefore, controls the basic workload partioning of the MapReduce engine -and can be an important parameter to tune for performance of the cluster.

The property has a unit of kilobytes; the default value is `32*1024`: 32 MB

        <property>
          <name>fs.swift.blocksize</name>
          <value>32768</value>
        </property>

This blocksize has no influence on how files are stored in Swift; it only controls what the reported size of blocks are - a value used in Hadoop MapReduce to divide work.

Note that the MapReduce engine's split logic can be tuned independently by setting the `mapred.min.split.size` and `mapred.max.split.size` properties, which can be done in specific job configurations.

        <property>
          <name>mapred.min.split.size</name>
          <value>524288</value>
        </property>

        <property>
          <name>mapred.max.split.size</name>
          <value>1048576</value>
        </property>

In an Apache Pig script, these properties would be set as:

        mapred.min.split.size 524288
        mapred.max.split.size 1048576

#### Partition size fs.swift.partsize

The Swift filesystem client breaks very large files into partitioned files, uploading each as it progresses, and writing any remaning data and an XML manifest when a partitioned file is closed.

The partition size defaults to 4608 MB; 4.5GB, the maximum filesize that Swift can support.

It is possible to set a smaller partition size, in the `fs.swift.partsize` option. This takes a value in KB.

        <property>
          <name>fs.swift.partsize</name>
          <value>1024</value>
          <description>upload every MB</description>
        </property>

When should this value be changed from its default?

While there is no need to ever change it for basic operation of the Swift filesystem client, it can be tuned

*   If a Swift filesystem is location aware, then breaking a file up into
    smaller partitions scatters the data round the cluster. For best performance,
    the property `fs.swift.blocksize` should be set to a smaller value than the
    partition size of files.

*   When writing to an unpartitioned file, the entire write is done in the
    `close()` operation. When a file is partitioned, the outstanding data to
    be written whenever the outstanding amount of data is greater than the
    partition size. This means that data will be written more incrementally

#### Request size fs.swift.requestsize

The Swift filesystem client reads files in HTTP GET operations, asking for a block of data at a time.

The default value is 64KB. A larger value may be more efficient over faster networks, as it reduces the overhead of setting up the HTTP operation.

However, if the file is read with many random accesses, requests for data will be made from different parts of the file -discarding some of the previously requested data. The benefits of larger request sizes may be wasted.

The property `fs.swift.requestsize` sets the request size in KB.

        <property>
          <name>fs.swift.requestsize</name>
          <value>128</value>
        </property>

#### Connection timeout fs.swift.connect.timeout

This sets the timeout in milliseconds to connect to a Swift service.

        <property>
          <name>fs.swift.connect.timeout</name>
          <value>15000</value>
        </property>

A shorter timeout means that connection failures are raised faster -but may trigger more false alarms. A longer timeout is more resilient to network problems -and may be needed when talking to remote filesystems.

#### Connection timeout fs.swift.socket.timeout

This sets the timeout in milliseconds to wait for data from a connected socket.

        <property>
          <name>fs.swift.socket.timeout</name>
          <value>60000</value>
        </property>

A shorter timeout means that connection failures are raised faster -but may trigger more false alarms. A longer timeout is more resilient to network problems -and may be needed when talking to remote filesystems.

#### Connection Retry Count fs.swift.connect.retry.count

This sets the number of times to try to connect to a service whenever an HTTP request is made.

        <property>
          <name>fs.swift.connect.retry.count</name>
          <value>3</value>
        </property>

The more retries, the more resilient it is to transient outages -and the less rapid it is at detecting and reporting server connectivity problems.

#### Connection Throttle Delay fs.swift.connect.throttle.delay

This property adds a delay between bulk file copy and delete operations, to prevent requests being throttled or blocked by the remote service

        <property>
          <name>fs.swift.connect.throttle.delay</name>
          <value>0</value>
        </property>

It is measured in milliseconds; "0" means do not add any delay.

Throttling is enabled on the public endpoints of some Swift services. If `rename()` or `delete()` operations fail with `SwiftThrottledRequestException` exceptions, try setting this property.

#### HTTP Proxy

If the client can only access the Swift filesystem via a web proxy server, the client configuration must specify the proxy via the `fs.swift.connect.proxy.host` and `fs.swift.connect.proxy.port` properties.

        <property>
          <name>fs.swift.proxy.host</name>
          <value>web-proxy</value>
        </property>

        <property>
          <name>fs.swift.proxy.port</name>
          <value>8088</value>
        </property>

If the host is declared, the proxy port must be set to a valid integer value.

### Troubleshooting

#### ClassNotFoundException

The `hadoop-openstack` JAR -or any dependencies- may not be on your classpath.

Make sure that the:
* JAR is installed on the servers in the cluster.
* 'hadoop-openstack' is on the HADOOP_OPTIONAL_TOOLS entry in hadoop-env.sh or that the job submission process uploads the JAR file to the distributed cache.

#### Failure to Authenticate

A `SwiftAuthenticationFailedException` is thrown when the client cannot authenticate with the OpenStack keystone server. This could be because the URL in the service definition is wrong, or because the supplied credentials are invalid.

1.  Check the authentication URL through `curl` or your browser

2.  Use a Swift client such as CyberDuck to validate your credentials

3.  If you have included a tenant ID, try leaving it out. Similarly,
    try adding it if you had not included it.

4.  Try switching from API key authentication to password-based authentication,
    by setting the password.

5.  Change your credentials. As with Amazon AWS clients, some credentials
    don't seem to like going over the network.

#### Timeout connecting to the Swift Service

This happens if the client application is running outside an OpenStack cluster, where it does not have access to the private hostname/IP address for filesystem operations. Set the `public` flag to true -but remember to set it to false for use in-cluster.

### Warnings

1.  Do not share your login details with anyone, which means do not log the
    details, or check the XML configuration files into any revision control system
    to which you do not have exclusive access.

2.  Similarly, do not use your real account details in any
    documentation \*or any bug reports submitted online\*

3.  Prefer the apikey authentication over passwords as it is easier
    to revoke a key -and some service providers allow you to set
    an automatic expiry date on a key when issued.

4.  Do not use the public service endpoint from within a public OpenStack
    cluster, as it will run up large bills.

5.  Remember: it's not a real filesystem or hierarchical directory structure.
    Some operations (directory rename and delete) take time and are not atomic or
    isolated from other operations taking place.

6.  Append is not supported.

7.  Unix-style permissions are not supported. All accounts with write access to
    a repository have unlimited access; the same goes for those with read access.

8.  In the public clouds, do not make the containers public unless you are happy
    with anyone reading your data, and are prepared to pay the costs of their
    downloads.

### Limits

*   Maximum length of an object path: 1024 characters

*   Maximum size of a binary object: no absolute limit. Files \> 5GB are
    partitioned into separate files in the native filesystem, and merged during
    retrieval. *Warning:* the partitioned/large file support is the
    most complex part of the Hadoop/Swift FS integration, and, along with
    authentication, the most troublesome to support.

### Testing the hadoop-openstack module

The `hadoop-openstack` can be remotely tested against any public or private cloud infrastructure which supports the OpenStack Keystone authentication mechanism. It can also be tested against private OpenStack clusters. OpenStack Development teams are strongly encouraged to test the Hadoop swift filesystem client against any version of Swift that they are developing or deploying, to stress their cluster and to identify bugs early.

The module comes with a large suite of JUnit tests -tests that are only executed if the source tree includes credentials to test against a specific cluster.

After checking out the Hadoop source tree, create the file:

      hadoop-tools/hadoop-openstack/src/test/resources/auth-keys.xml

Into this file, insert the credentials needed to bond to the test filesystem, as decribed above.

Next set the property `test.fs.swift.name` to the URL of a swift container to test against. The tests expect exclusive access to this container -do not keep any other data on it, or expect it to be preserved.

        <property>
          <name>test.fs.swift.name</name>
          <value>swift://test.myswift/</value>
        </property>

In the base hadoop directory, run:

       mvn clean install -DskipTests

This builds a set of Hadoop JARs consistent with the `hadoop-openstack` module that is about to be tested.

In the `hadoop-tools/hadoop-openstack` directory run

       mvn test -Dtest=TestSwiftRestClient

This runs some simple tests which include authenticating against the remote swift service. If these tests fail, so will all the rest. If it does fail: check your authentication.

Once this test succeeds, you can run the full test suite

        mvn test

Be advised that these tests can take an hour or more, especially against a remote Swift service -or one that throttles bulk operations.

Once the `auth-keys.xml` file is in place, the `mvn test` runs from the Hadoop source base directory will automatically run these OpenStack tests While this ensures that no regressions have occurred, it can also add significant time to test runs, and may run up bills, depending on who is providingthe Swift storage service. We recommend having a separate source tree set up purely for the Swift tests, and running it manually or by the CI tooling at a lower frequency than normal test runs.

Finally: Apache Hadoop is an open source project. Contributions of code -including more tests- are very welcome.
