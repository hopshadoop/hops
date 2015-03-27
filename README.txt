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

sudo  aptitude install cmake libprotobuf-dev libprotobuf-c0-dev

mvn clean generate-sources

cd hadoop-maven-plugins
mvn install

cd ..
mvn package -Pdist -DskipTests -Dtar

- for native deployment, you need to install these libraries first

sudo aptitude install zlib1g-dev libssl-dev

cd ..
mvn package -Pdist,native -DskipTests -Dtar

How to add RPC messages
===
Add first a protocol buffer msg and rpc to the .proto file.
Then add a wrapper class.

Finally, run:
mvn generate-sources
to generate the java classes from the protocol buffer files.

===============================================================================

Memcache Setup
==================

1- add ndbmemcache schema to mysql cluster

Ex:
/usr/local/mysql/bin/mysql -S /tmp/mysql.sock < /usr/local/mysql/share/memcache-api/ndb_memcache_metadata.sql

2- insert the following rows to the ndbmemcache database


use ndbmemcache;
INSERT INTO containers VALUES ('path_cnt', 'hop_mahmoud','path_memcached', 'path', 'inodeids', 0, NULL, NULL, NULL, NULL);
INSERT INTO key_prefixes VALUES (3, 'p:', 0,'caching', 'path_cnt');


3- use the memcached command associated with the mysql cluster on your namenode

Ex:
/home/mahmoud/opt/mysql-cluster/bin/memcached -E /home/mahmoud/opt/mysql-cluster/lib/ndb_engine.so -e "connectstring=cloud11.sics.se:1186;role=ndb-caching" -p 11212 -U 11212 -v

4- In DFSConfigKeys.java update the Memcache config parameters 

NOTE: don't forget to change hop_mahmoud to your database name

