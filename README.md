Hops
===

Hadoop Open Platform-as-a-Service (Hops) is a new distribution of Apache Hadoop with scalable, highly available, customizable metadata.
<ul>
<li><a href="https://twitter.com/hopshadoop">Follow our Twitter account.</a></li>
<li><a href="https://gitter.im/hopshadoop">Chat with Hops developers in Gitter.</a></li>
<li><a href="https://groups.google.com/forum/#!forum/hopshadoop">Join our developer mailing list.</a></li>
<li><a href="https://cloud17.sics.se/jenkins/view/develop/">Checkout the current build status.</a></li>
</ul>

Introduction
====
Hops consists internally of two main sub projects, Hops-Fs and Hops-Yarn. Hops-FS is a new implementation of the the Hadoop Filesystem (HDFS), that supports multiple stateless NameNodes, where the metadata is stored in an in-memory distributed database (MySQL Cluster). Hops-FS enables more scalable clusters than Apache HDFS (up to ten times larger clusters), and enables NameNode metadata to be both customized and analyzed, because it can now be easily accessed via a SQL API. Hops-YARN introduces a distributed stateless Resource Manager, whose state is migrated to MySQL Cluster, a replicated, partitioned, in-memory NewSQL database. This enables our YARN architecture to have no down-time, with failover of a ResourceManager happening in a few seconds. 

For the latest information about Hops, please visit our website at:

   http://hops.io

For the latest information about Hadoop, please visit their website at:

  http://hadoop.apache.org/core/

and their wiki, at:

   http://wiki.apache.org/hadoop/

This distribution includes cryptographic software.  The country in
which you currently reside may have restrictions on the import,
possession, use, and/or re-export to another country, of
encryption software.  BEFORE using any encryption software, please
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to
see if this is permitted.  See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and
Security (BIS), has classified this software as Export Commodity
Control Number (ECCN) 5D002.C.1, which includes information security
software using or performing cryptographic functions with asymmetric
algorithms.  The form and manner of this Apache Software Foundation
distribution makes it eligible for export under the License Exception
ENC Technology Software Unrestricted (TSU) exception (see the BIS
Export Administration Regulations, Section 740.13) for both object
code and source code.

The following provides more details on the included cryptographic
software:
  Hadoop Core uses the SSL libraries from the Jetty project written
by mortbay.org.

===============================================================================

How to build
===
```
sudo  aptitude install cmake libprotobuf-dev libprotobuf-c0-dev

mvn clean generate-sources

cd hadoop-maven-plugins
mvn install
```
build the [hop-metadata-dal](https://github.com/hopshadoop/hops-metadata-dal) project first, then build the associated metadata-dal implementation which for now is [hop-metadata-dal-ndb-impl](https://github.com/hopshadoop/hops-metadata-dal-impl-ndb)

```
cd ..
mvn package -Pdist -DskipTests -Dtar
```
for native deployment, you need to install these libraries first
```
sudo aptitude install zlib1g-dev libssl-dev
cd ..
mvn package -Pdist,native -DskipTests -Dtar
```

How to add RPC messages
===
Add first a protocol buffer msg and rpc to the .proto file.
Then add a wrapper class.

Finally, run:
mvn generate-sources
to generate the java classes from the protocol buffer files.

===============================================================================

# License

Hops is released under an [Apache 2.0 license](LICENSE.txt).

