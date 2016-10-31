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
sudo aptitude install cmake libprotobuf-dev libprotobuf-c0-dev

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

##Hops Installation
The Hops stack includes a number of services also requires a number of third-party distributed services:
<ul>
<li>Java 1.7 (OpenJDK or Oracle JRE/JDK)</li>
<li>NDB 7.4+ (MySQL Cluster)</li>
<li>J2EE7 web application server (default: Glassfish)</li>
<li>ElasticSearch 1.7+</li>
</ul>

Due to the complexity of installing and configuring all Hops’ services, we recommend installing Hops using the automated installer [Karamel/Chef](http://www.karamel.io). Detailed documentation on the steps for installing and configuring all services in Hops is not discussed here. Instead, Chef cookbooks contain all the installation and configuration steps needed to install and configure Hops. The Chef cookbooks are available at: https://github.com/hopshadoop.

###Installing on Cloud Platforms (AWS, GCE, OPenStack)
1. Download and install Karamel (http://www.karamel.io).
2. Run Karamel.
3. Click on the “Load Cluster Definition” menu item in Karamel. You are now prompted to select a cluster definition YAML file. Go to the examples/stable directory, and select a cluster definition file for your target cloud platform for one of the following cluster types:
    * Amazon Web Services EC2 (AWS)
    * Google Compute Engine (GCE)
    * OpenStack
    * On-premises (bare metal)

For more information on how to configure cloud-based installations,  go to help documentation at http://www.karamel.io. For on-premises installations, we provide some additional installation details and tips later in this section.

###On-Premises (baremetal) Installation
For on-premises (bare-metal) installations, you will need to prepare for installation by:

1. Identifying a master host, from which you will run Karamel;
    * the master must have a display for Karamel’s user interface;
    * the master must be able to ping (and connect using ssh) to all of the target hosts.
  
2. Identifying a set of target hosts, on which the Hops software and 3rd party services will be installed.
  * the target nodes should have http access to the open Internet to be able to download software during the installation process. (Cookbooks can be configured to download software from within the private network, but this requires a good bit of configuration work for Chef attributes, changing all download URLs).

The master must be able to connect using SSH to all the target nodes, on which the software will be installed. If you have not already copied the master’s public key to the .ssh/authorized_keys file of all target hosts, you can do so by preparing the machines as follows:

1. Create an openssh public/private key pair on the master host for your user account. On Linux, you can use the ssh-keygen utility program to generate the keys, which will by default be stored in the $HOME/.ssh/id_rsa and $HOME/.ssh/id_rsa.pub files. If you decided to enter a password for the ssh keypair, you will need to enter it again in Karamel when you reach the ssh dialog, part of Karamel’s Launch step. We recommend no password (passwordless) for the ssh keypair.
2. Create a user account USER on the all the target machines with full sudo privileges (root privileges) and the same password on all target machines.
3. Copy the $HOME/.ssh/id_rsa.pub file on the master to the /tmp folder of all the target hosts. A good way to do this is to use pscp utility along with a file ( hosts.txt ) containing the line-separated hostnames (or IP addresss) for all the target machines. You may need to install the pssh utility programs ( pssh ), first.

```
sudo apt-get install pssh
or
yum install pssh
vim hosts.txt

# Enter the row-separated IP addresses of all target nodes in hosts.txt

128.112.152.122
18.31.0.190
128.232.103.201
.....

pscp -h hosts.txt -P PASSWORD -i USER ~/.ssh/id_rsa.pub /tmp
pssh -h hosts.txt -i USER -P PASSWORD mkdir -p /home/USER/.ssh
pssh -h hosts.txt -i USER -P PASSWORD cat /tmp/id_rsa.pub >> /home/USER/.ssh/authorized_keys
```

Update your Karamel cluster definition file to include the IP addresses of the target machines and the USER account name. After you have clicked on the launch menu item, you will come to a ssh dialog. On the ssh dialog, you need to open the advanced section. Here, you will need to enter the password for the USER account on the target machines ( sudo password text input box). If your ssh keypair is password protected, you will also need to enter it again here in the keypair password text input box. 

###Vagrant (Virtualbox)
You can install Hops on your laptop/desktop with Vagrant. You will need to have the following software packages installed:

* chef-dk, version >0.5+ (but not >0.8+)
* git
* vagrant
* vagrant omnibus plugin
* virtualbox

You can now run vagrant, using:

```
sudo apt-get install virtualbox vagrant
vagrant plugin install vagrant-omnibus
git clone https://github.com/hopshadoop/hopsworks-chef.git
cd hopsworks-chef
berks vendor cookbooks
vagrant up
```

# Export Control

This distribution includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption software, please check your country's laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted. See <http://www.wassenaar.org/> for more information. 

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

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

