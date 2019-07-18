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

Web Application Proxy
=====================

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->


Overview
---------

### Introduction 

 The Web Application Proxy is part of YARN. By default it will run as part of the Resource Manager(RM), but can be configured to run in stand alone mode. The reason for the proxy is to reduce the possibility of web based attacks through YARN.

 In YARN the Application Master(AM) has the responsibility to provide a web UI and to send that link to the RM. This opens up a number of potential issues. The RM runs as a trusted user, and people visiting that web address will treat it, and links it provides to them as trusted, when in reality the AM is running as a non-trusted user, and the links it gives to the RM could point to anything malicious or otherwise. The Web Application Proxy mitigates this risk by warning users that do not own the given application that they are connecting to an untrusted site.

 In addition to this the proxy also tries to reduce the impact that a malicious AM could have on a user. It primarily does this by stripping out cookies from the user, and replacing them with a single cookie providing the user name of the logged in user. This is because most web based authentication systems will identify a user based off of a cookie. By providing this cookie to an untrusted application it opens up the potential for an exploit. If the cookie is designed properly that potential should be fairly minimal, but this is just to reduce that potential attack vector. 
 
### Current Status
 
 The current proxy implementation does nothing to prevent the AM from providing links to malicious external sites, nor does it do anything to prevent malicious javascript code from running as well. In fact javascript can be used to get the cookies, so stripping the cookies from the request has minimal benefit at this time. In the future we hope to address the attack vectors described above and make attaching to an AM's web UI safer.


Deployment
----------

###Configurations

| Configuration Property | Description |
|:---- |:---- |
| `yarn.web-proxy.address` | The address for the web proxy as HOST:PORT, if this is not given then the proxy will run as part of the RM. |
| `yarn.web-proxy.keytab` | Keytab for WebAppProxy, if the proxy is not running as part of the RM. |
| `yarn.web-proxy.principal` | The kerberos principal for the proxy, if the proxy is not running as part of the RM. |


### Running Web Application Proxy

  Standalone Web application proxy server can be launched with the following command. 

```
  $ yarn proxyserver
```

  Or users can start the stand alone Web Application Proxy server as a daemon, with the following command

```
  $ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver
```
