# Hops

[![Join the chat at https://gitter.im/hopshadoop/hops](https://badges.gitter.im/hopshadoop/services.png)](https://gitter.im/hopshadoop/hops)

<a href=""><img src="http://www.hops.io/sites/default/files/hops-50x50.png" align="left" hspace="10" vspace="6"></a>

**Hops** (<b>H</b>adoop <b>O</b>pen <b>P</b>latform-as-a-<b>S</b>ervice) is a next generation distribution of [Apache Hadoop](http://hadoop.apache.org/core/) with scalable, highly available, customizable metadata. Hops consists internally of two main sub projects, HopsFs and HopsYarn. <b>HopsFS</b> is a new implementation of the the Hadoop Filesystem (HDFS), that supports multiple stateless NameNodes, where the metadata is stored in [MySQL Cluster](https://www.mysql.com/products/cluster/), an in-memory distributed database. HopsFS enables more scalable clusters than Apache HDFS (up to ten times larger clusters), and enables NameNode metadata to be both customized and analyzed, because it can now be easily accessed via a SQL API. <b>HopsYARN</b> introduces a distributed stateless Resource Manager, whose state is migrated to MySQL Cluster. This enables our YARN architecture to have no down-time, with failover of a ResourceManager happening in a few seconds. Together, HopsFS and HopsYARN enable Hadoop clusters to scale to larger volumes and higher throughput.


# Online Documentation
You can find the latest Hops documentation, including a programming guide, on the project [web page](http://www.hops.io). This README file only contains basic setup instructions.




## How to build

Build the [hop-metadata-dal](https://github.com/hopshadoop/hops-metadata-dal) project first, then build the associated metadata-dal implementation driver which for now is [hop-metadata-dal-ndb-impl](https://github.com/hopshadoop/hops-metadata-dal-impl-ndb).

Building Hops Hadoop Distribution
```
sudo  aptitude install cmake libprotobuf-dev libprotobuf-c0-dev

mvn clean generate-sources

cd hadoop-maven-plugins

mvn install

cd ..

mvn package -Pdist -DskipTests -Dtar
```

For native deployment, you need to install these libraries first

```
sudo aptitude install zlib1g-dev libssl-dev

cd ..

mvn package -Pdist,native -DskipTests -Dtar
```


# Export Control

This distribution includes cryptographic software.  The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software.  BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted.  See <http://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms.  The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software: Hadoop Core uses the SSL libraries from the Jetty project written by mortbay.org.

#Contact 

<ul>
<li><a href="https://twitter.com/hopshadoop">Follow our Twitter account.</a></li>
<li><a href="https://gitter.im/hopshadoop/hops">Chat with Hops developers in Gitter.</a></li>
<li><a href="https://groups.google.com/forum/#!forum/hopshadoop">Join our developer mailing list.</a></li>
<li><a href="https://cloud17.sics.se/jenkins/view/develop/">Checkout the current build status.</a></li>
</ul>

# License

Hops is released under an [Apache 2.0 license](LICENSE.txt).

