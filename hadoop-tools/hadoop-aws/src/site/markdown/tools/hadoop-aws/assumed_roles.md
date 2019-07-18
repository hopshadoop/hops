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

# Working with IAM Assumed Roles

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

AWS ["IAM Assumed Roles"](http://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
allows applications to change the AWS role with which to authenticate with AWS services.
The assumed roles can have different rights from the main user login.

The S3A connector supports assumed roles for authentication with AWS.
A full set of login credentials must be provided, which will be used
to obtain the assumed role and refresh it regularly.
By using per-filesystem configuration, it is possible to use different
assumed roles for different buckets.

*IAM Assumed Roles are unlikely to be supported by third-party systems
supporting the S3 APIs.*

## <a name="using_assumed_roles"></a> Using IAM Assumed Roles

### Before You Begin

This document assumes you know about IAM Assumed roles, what they
are, how to configure their policies, etc.

* You need a role to assume, and know its "ARN".
* You need a pair of long-lived IAM User credentials, not the root account set.
* Have the AWS CLI installed, and test that it works there.
* Give the role access to S3, and, if using S3Guard, to DynamoDB.
* For working with data encrypted with SSE-KMS, the role must
have access to the appropriate KMS keys.

Trying to learn how IAM Assumed Roles work by debugging stack traces from
the S3A client is "suboptimal".

### <a name="how_it_works"></a> How the S3A connector support IAM Assumed Roles.

To use assumed roles, the client must be configured to use the
*Assumed Role Credential Provider*, `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider`,
in the configuration option `fs.s3a.aws.credentials.provider`.

This AWS Credential provider will read in the `fs.s3a.assumed.role` options needed to connect to the
Security Token Service [Assumed Role API](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html),
first authenticating with the full credentials, then assuming the specific role
specified. It will then refresh this login at the configured rate of
`fs.s3a.assumed.role.session.duration`

To authenticate with the  [AWS STS service](https://docs.aws.amazon.com/STS/latest/APIReference/Welcome.html)
both for the initial credential retrieval
and for background refreshes, a different credential provider must be
created, one which uses long-lived credentials (secret keys, environment variables).
Short lived credentials (e.g other session tokens, EC2 instance credentials) cannot be used.

A list of providers can be set in `s.s3a.assumed.role.credentials.provider`;
if unset the standard `BasicAWSCredentialsProvider` credential provider is used,
which uses `fs.s3a.access.key` and `fs.s3a.secret.key`.

Note: although you can list other AWS credential providers in  to the
Assumed Role Credential Provider, it can only cause confusion.

### <a name="using"></a> Configuring Assumed Roles

To use assumed roles, the S3A client credentials provider must be set to
the `AssumedRoleCredentialProvider`, and `fs.s3a.assumed.role.arn` to
the previously created ARN.

```xml
<property>
  <name>fs.s3a.aws.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider</value>
</property>

<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value>arn:aws:iam::90066806600238:role/s3-restricted</value>
</property>
```

The STS service itself needs the caller to be authenticated, *which can
only be done with a set of long-lived credentials*.
This means the normal `fs.s3a.access.key` and `fs.s3a.secret.key`
pair, environment variables, or some other supplier of long-lived secrets.

The default is the `fs.s3a.access.key` and `fs.s3a.secret.key` pair.
If you wish to use a different authentication mechanism, set it in the property
`fs.s3a.assumed.role.credentials.provider`.

```xml
<property>
  <name>fs.s3a.assumed.role.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
</property>
```

Requirements for long-lived credentials notwithstanding, this option takes the
same values as `fs.s3a.aws.credentials.provider`.

The safest way to manage AWS secrets is via
[Hadoop Credential Providers](index.html#hadoop_credential_providers).

### <a name="configuration"></a>Assumed Role Configuration Options

Here are the full set of configuration options.

```xml
<property>
  <name>fs.s3a.assumed.role.arn</name>
  <value />
  <description>
    AWS ARN for the role to be assumed.
    Required if the fs.s3a.aws.credentials.provider contains
    org.apache.hadoop.fs.s3a.AssumedRoleCredentialProvider
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.session.name</name>
  <value />
  <description>
    Session name for the assumed role, must be valid characters according to
    the AWS APIs.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
    If not set, one is generated from the current Hadoop/Kerberos username.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.policy</name>
  <value/>
  <description>
    JSON policy to apply to the role.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.session.duration</name>
  <value>30m</value>
  <description>
    Duration of assumed roles before a refresh is attempted.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
    Range: 15m to 1h
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.sts.endpoint</name>
  <value/>
  <description>
    AWS Security Token Service Endpoint. If unset, uses the default endpoint.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.sts.endpoint.region</name>
  <value>us-west-1</value>
  <description>
    AWS Security Token Service Endpoint's region;
    Needed if fs.s3a.assumed.role.sts.endpoint points to an endpoint
    other than the default one and the v4 signature is used.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
  </description>
</property>

<property>
  <name>fs.s3a.assumed.role.credentials.provider</name>
  <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  <description>
    List of credential providers to authenticate with the STS endpoint and
    retrieve short-lived role credentials.
    Only used if AssumedRoleCredentialProvider is the AWS credential provider.
    If unset, uses "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".
  </description>
</property>
```

## <a name="polices"></a> Restricting S3A operations through AWS Policies

The S3A client needs to be granted specific permissions in order
to work with a bucket.
Here is a non-normative list of the permissions which must be granted
for FileSystem operations to work.

*Disclaimer* The specific set of actions which the S3A connector needs
will change over time.

As more operations are added to the S3A connector, and as the
means by which existing operations are implemented change, the
AWS actions which are required by the client will change.

These lists represent the minimum actions to which the client's principal
must have in order to work with a bucket.


### <a name="read-permissions"></a> Read Access Permissions

Permissions which must be granted when reading from a bucket:


```
s3:Get*
s3:ListBucket
```

When using S3Guard, the client needs the appropriate
<a href="s3guard-permissions">DynamoDB access permissions</a>

To use SSE-KMS encryption, the client needs the
<a href="sse-kms-permissions">SSE-KMS Permissions</a> to access the
KMS key(s).

### <a name="write-permissions"></a> Write Access Permissions

These permissions must all be granted for write access:

```
s3:Get*
s3:Delete*
s3:Put*
s3:ListBucket
s3:ListBucketMultipartUploads
s3:AbortMultipartUpload
```

### <a name="sse-kms-permissions"></a> SSE-KMS Permissions

When to read data encrypted using SSE-KMS, the client must have
 `kms:Decrypt` permission for the specific key a file was encrypted with.

```
kms:Decrypt
```

To write data using SSE-KMS, the client must have all the following permissions.

```
kms:Decrypt
kms:GenerateDataKey
```

This includes renaming: renamed files are encrypted with the encryption key
of the current S3A client; it must decrypt the source file first.

If the caller doesn't have these permissions, the operation will fail with an
`AccessDeniedException`: the S3 Store does not provide the specifics of
the cause of the failure.

### <a name="s3guard-permissions"></a> S3Guard Permissions

To use S3Guard, all clients must have a subset of the
[AWS DynamoDB Permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/api-permissions-reference.html).

To work with buckets protected with S3Guard, the client must have
all the following rights on the DynamoDB Table used to protect that bucket.

```
dynamodb:BatchGetItem
dynamodb:BatchWriteItem
dynamodb:DeleteItem
dynamodb:DescribeTable
dynamodb:GetItem
dynamodb:PutItem
dynamodb:Query
dynamodb:UpdateItem
```

This is true, *even if the client only has read access to the data*.

For the `hadoop s3guard` table management commands, _extra_ permissions are required:

```
dynamodb:CreateTable
dynamodb:DescribeLimits
dynamodb:DeleteTable
dynamodb:Scan
dynamodb:TagResource
dynamodb:UntagResource
dynamodb:UpdateTable
```

Without these permissions, tables cannot be created, destroyed or have their IO capacity
changed through the `s3guard set-capacity` call.
The `dynamodb:Scan` permission is needed for `s3guard prune`

The `dynamodb:CreateTable` permission is needed by a client it tries to
create the DynamoDB table on startup, that is
`fs.s3a.s3guard.ddb.table.create` is `true` and the table does not already exist.

### <a name="mixed-permissions"></a> Mixed Permissions in a single S3 Bucket

Mixing permissions down the "directory tree" is limited
only to the extent of supporting writeable directories under
read-only parent paths.

*Disclaimer:* When a client lacks write access up the entire
directory tree, there are no guarantees of consistent filesystem
views or operations.

Particular troublespots are "directory markers" and
failures of non-atomic operations, particularly `rename()` and `delete()`.

A directory marker such as `/users/` will not be deleted if the user `alice`
creates a directory `/users/alice` *and* she only has access to `/users/alice`.

When a path or directory is deleted, the parent directory may not exist afterwards.
In the example above, if `alice` deletes `/users/alice` and there are no
other entries under `/users/alice`, then the directory marker `/users/` cannot
be created. The directory `/users` will not exist in listings,
`getFileStatus("/users")` or similar.

Rename will fail if it cannot delete the items it has just copied, that is
`rename(read-only-source, writeable-dest)` will fail &mdash;but only after
performing the COPY of the data.
Even though the operation failed, for a single file copy, the destination
file will exist.
For a directory copy, only a partial copy of the source data may take place
before the permission failure is raised.


*S3Guard*: if [S3Guard](s3guard.html) is used to manage the directory listings,
then after partial failures of rename/copy the DynamoDB tables can get out of sync.

### Example: Read access to the base, R/W to the path underneath

This example has the base bucket read only, and a directory underneath,
`/users/alice/` granted full R/W access.

```json
{
  "Version" : "2012-10-17",
  "Statement" : [ {
    "Sid" : "4",
    "Effect" : "Allow",
    "Action" : [
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:Get*"
      ],
    "Resource" : "arn:aws:s3:::example-bucket/*"
  }, {
    "Sid" : "5",
    "Effect" : "Allow",
    "Action" : [
      "s3:Get*",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts" ],
    "Resource" : [
      "arn:aws:s3:::example-bucket/users/alice/*",
      "arn:aws:s3:::example-bucket/users/alice",
      "arn:aws:s3:::example-bucket/users/alice/"
      ]
  } ]
}
```

Note how three resources are provided to represent the path `/users/alice`

|  Path | Matches |
|-------|----------|
| `/users/alice` |  Any file `alice` created under `/users` |
| `/users/alice/` |  The directory marker `alice/` created under `/users` |
| `/users/alice/*` |  All files and directories under the path `/users/alice` |

Note that the resource `arn:aws:s3:::example-bucket/users/alice*` cannot
be used to refer to all of these paths, because it would also cover
adjacent paths like `/users/alice2` and `/users/alicebob`.


## <a name="troubleshooting"></a> Troubleshooting Assumed Roles

1. Make sure the role works and the user trying to enter it can do so from AWS
the command line before trying to use the S3A client.
1. Try to access the S3 bucket with reads and writes from the AWS CLI.
1. With the Hadoop configuration set too use the role,
 try to read data from the `hadoop fs` CLI:
`hadoop fs -ls -p s3a://bucket/`
1. With the hadoop CLI, try to create a new directory with a request such as
`hadoop fs -mkdirs -p s3a://bucket/path/p1/`


### <a name="no_role"></a> IOException: "Unset property fs.s3a.assumed.role.arn"

The Assumed Role Credential Provider is enabled, but `fs.s3a.assumed.role.arn` is unset.

```
java.io.IOException: Unset property fs.s3a.assumed.role.arn
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:76)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
```

### <a name="not_authorized_for_assumed_role"></a> "Not authorized to perform sts:AssumeRole"

This can arise if the role ARN set in `fs.s3a.assumed.role.arn` is invalid
or one to which the caller has no access.

```
java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
 on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
  Not authorized to perform sts:AssumeRole (Service: AWSSecurityTokenService; Status Code: 403;
   Error Code: AccessDenied; Request ID: aad4e59a-f4b0-11e7-8c78-f36aaa9457f6):AccessDenied
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
```

### <a name="root_account"></a> "Roles may not be assumed by root accounts"

You can't assume a role with the root account of an AWS account;
you need to create a new user and give it the permission to change into
the role.

```
java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
 on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    Roles may not be assumed by root accounts. (Service: AWSSecurityTokenService; Status Code: 403; Error Code: AccessDenied;
    Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2):
    No AWS Credentials provided by AssumedRoleCredentialProvider :
    com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    Roles may not be assumed by root accounts. (Service: AWSSecurityTokenService;
     Status Code: 403; Error Code: AccessDenied; Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
  ... 22 more
Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
 Roles may not be assumed by root accounts.
  (Service: AWSSecurityTokenService; Status Code: 403; Error Code: AccessDenied;
   Request ID: e86dfd8f-e758-11e7-88e7-ad127c04b5e2)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
```

### <a name="invalid_duration"></a> "Assume Role session duration should be in the range of 15min - 1Hr"

The value of `fs.s3a.assumed.role.session.duration` is out of range.

```
java.lang.IllegalArgumentException: Assume Role session duration should be in the range of 15min
- 1Hr
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$Builder.withRoleSessionDurationSeconds(STSAssumeRoleSessionCredentialsProvider.java:437)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:86)
```


### <a name="malformed_policy"></a> `MalformedPolicyDocumentException` "The policy is not in the valid JSON format"


The policy set in `fs.s3a.assumed.role.policy` is not valid according to the
AWS specification of Role Policies.

```
rg.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
  The policy is not in the valid JSON format. (Service: AWSSecurityTokenService; Status Code: 400;
   Error Code: MalformedPolicyDocument; Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c):
   MalformedPolicyDocument: The policy is not in the valid JSON format.
   (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
    Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
Caused by: com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
 The policy is not in the valid JSON format.
  (Service: AWSSecurityTokenService; Status Code: 400;
   Error Code: MalformedPolicyDocument; Request ID: baf8cb62-f552-11e7-9768-9df3b384e40c)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 19 more
```

### <a name="malformed_policy"></a> `MalformedPolicyDocumentException` "Syntax errors in policy"

The policy set in `fs.s3a.assumed.role.policy` is not valid JSON.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException:
Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.MalformedPolicyDocumentException:
  Syntax errors in policy. (Service: AWSSecurityTokenService;
  Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45):
  MalformedPolicyDocument: Syntax errors in policy.
  (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
 (Service: AWSSecurityTokenService; Status Code: 400; Error Code: MalformedPolicyDocument;
  Request ID: 24a281e8-f553-11e7-aa91-a96becfb4d45)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 19 more
```

### <a name="recursive_auth"></a> `IOException`: "AssumedRoleCredentialProvider cannot be in fs.s3a.assumed.role.credentials.provider"

You can't use the Assumed Role Credential Provider as the provider in
`fs.s3a.assumed.role.credentials.provider`.

```
java.io.IOException: AssumedRoleCredentialProvider cannot be in fs.s3a.assumed.role.credentials.provider
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:86)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
```

### <a name="invalid_keypair"></a> `AWSBadRequestException`: "not a valid key=value pair"


There's an space or other typo in the `fs.s3a.access.key` or `fs.s3a.secret.key` values used for the
inner authentication which is breaking signature creation.

```
 org.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider
  on : com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
   'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign) in Authorization header:
    'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date.
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code:
    IncompleteSignature; Request ID: c4a8841d-f556-11e7-99f9-af005a829416):IncompleteSignature:
    'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign)
    in Authorization header: 'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date,
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: IncompleteSignature;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)

Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    'valid/20180109/us-east-1/sts/aws4_request' not a valid key=value pair (missing equal-sign)
    in Authorization header: 'AWS4-HMAC-SHA256 Credential=not valid/20180109/us-east-1/sts/aws4_request,
    SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;host;user-agent;x-amz-date,
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: IncompleteSignature;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 25 more
```

### <a name="invalid_token"></a> `AccessDeniedException/InvalidClientTokenId`: "The security token included in the request is invalid"

The credentials used to authenticate with the AWS Security Token Service are invalid.

```
[ERROR] Failures:
[ERROR] java.nio.file.AccessDeniedException: : Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider on :
 com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
  The security token included in the request is invalid.
  (Service: AWSSecurityTokenService; Status Code: 403; Error Code: InvalidClientTokenId;
   Request ID: 74aa7f8a-f557-11e7-850c-33d05b3658d7):InvalidClientTokenId
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:215)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)

Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
The security token included in the request is invalid.
 (Service: AWSSecurityTokenService; Status Code: 403; Error Code: InvalidClientTokenId;
 Request ID: 74aa7f8a-f557-11e7-850c-33d05b3658d7)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:127)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:116)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 25 more
```

### <a name="invalid_session"></a> `AWSSecurityTokenServiceExceptiond`: "Member must satisfy regular expression pattern: `[\w+=,.@-]*`"


The session name, as set in `fs.s3a.assumed.role.session.name` must match the wildcard `[\w+=,.@-]*`.

If the property is unset, it is extracted from the current username and then sanitized to
match these constraints.
If set explicitly, it must be valid.

```
org.apache.hadoop.fs.s3a.AWSBadRequestException: Instantiate org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider on
    com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
    Request ID: 7c437acb-f55d-11e7-9ad8-3b5e4f701c20):ValidationError:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint: Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:209)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:616)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProviderSet(S3AUtils.java:520)
  at org.apache.hadoop.fs.s3a.DefaultS3ClientFactory.createS3Client(DefaultS3ClientFactory.java:52)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:252)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3354)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:474)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)

Caused by: com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException:
    1 validation error detected: Value 'Session Names cannot Hava Spaces!' at 'roleSessionName'
    failed to satisfy constraint:
    Member must satisfy regular expression pattern: [\w+=,.@-]*
    (Service: AWSSecurityTokenService; Status Code: 400; Error Code: ValidationError;
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.doInvoke(AWSSecurityTokenServiceClient.java:1271)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.invoke(AWSSecurityTokenServiceClient.java:1247)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.executeAssumeRole(AWSSecurityTokenServiceClient.java:454)
  at com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient.assumeRole(AWSSecurityTokenServiceClient.java:431)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.newSession(STSAssumeRoleSessionCredentialsProvider.java:321)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.access$000(STSAssumeRoleSessionCredentialsProvider.java:37)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:76)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider$1.call(STSAssumeRoleSessionCredentialsProvider.java:73)
  at com.amazonaws.auth.RefreshableTask.refreshValue(RefreshableTask.java:256)
  at com.amazonaws.auth.RefreshableTask.blockingRefresh(RefreshableTask.java:212)
  at com.amazonaws.auth.RefreshableTask.getValue(RefreshableTask.java:153)
  at com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.getCredentials(STSAssumeRoleSessionCredentialsProvider.java:299)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.getCredentials(AssumedRoleCredentialProvider.java:135)
  at org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider.<init>(AssumedRoleCredentialProvider.java:124)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
  at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
  at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
  at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
  at org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider(S3AUtils.java:583)
  ... 26 more
```


### <a name="access_denied"></a> `java.nio.file.AccessDeniedException` within a FileSystem API call

If an operation fails with an `AccessDeniedException`, then the role does not have
the permission for the S3 Operation invoked during the call.

```
java.nio.file.AccessDeniedException: s3a://bucket/readonlyDir:
  rename(s3a://bucket/readonlyDir, s3a://bucket/renameDest)
 on s3a://bucket/readonlyDir:
  com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
  (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 2805F2ABF5246BB1;
   S3 Extended Request ID: iEXDVzjIyRbnkAc40MS8Sjv+uUQNvERRcqLsJsy9B0oyrjHLdkRKwJ/phFfA17Kjn483KSlyJNw=),
   S3 Extended Request ID: iEXDVzjIyRbnkAc40MS8Sjv+uUQNvERRcqLsJsy9B0oyrjHLdkRKwJ/phFfA17Kjn483KSlyJNw=:AccessDenied
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:216)
  at org.apache.hadoop.fs.s3a.S3AUtils.translateException(S3AUtils.java:143)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.rename(S3AFileSystem.java:853)
 ...
Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
 (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied; Request ID: 2805F2ABF5246BB1;
  S3 Extended Request ID: iEXDVzjIyRbnkAc40MS8Sjv+uUQNvERRcqLsJsy9B0oyrjHLdkRKwJ/phFfA17Kjn483KSlyJNw=),
  S3 Extended Request ID: iEXDVzjIyRbnkAc40MS8Sjv+uUQNvERRcqLsJsy9B0oyrjHLdkRKwJ/phFfA17Kjn483KSlyJNw=
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.handleErrorResponse(AmazonHttpClient.java:1638)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeOneRequest(AmazonHttpClient.java:1303)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeHelper(AmazonHttpClient.java:1055)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:743)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:717)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:699)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:667)
  at com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:649)
  at com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:513)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4229)
  at com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:4176)
  at com.amazonaws.services.s3.AmazonS3Client.deleteObject(AmazonS3Client.java:2066)
  at com.amazonaws.services.s3.AmazonS3Client.deleteObject(AmazonS3Client.java:2052)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.lambda$deleteObject$7(S3AFileSystem.java:1338)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:314)
  at org.apache.hadoop.fs.s3a.Invoker.retryUntranslated(Invoker.java:280)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.deleteObject(S3AFileSystem.java:1334)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.removeKeys(S3AFileSystem.java:1657)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.innerRename(S3AFileSystem.java:1046)
  at org.apache.hadoop.fs.s3a.S3AFileSystem.rename(S3AFileSystem.java:851)
```

This is the policy restriction behaving as intended: the caller is trying to
perform an action which is forbidden.

1. If a policy has been set in `fs.s3a.assumed.role.policy` then it must declare *all*
permissions which the caller is allowed to perform. The existing role policies
act as an outer constraint on what the caller can perform, but are not inherited.

1. If the policy for a bucket is set up with complex rules on different paths,
check the path for the operation.

1. The policy may have omitted one or more actions which are required.
Make sure that all the read and write permissions are allowed for any bucket/path
to which data is being written to, and read permissions for all
buckets read from.

If the bucket is using SSE-KMS to encrypt data:

1. The caller must have the `kms:Decrypt` permission to read the data.
1. The caller needs `kms:Decrypt` and `kms:GenerateDataKey`.

Without permissions, the request fails *and there is no explicit message indicating
that this is an encryption-key issue*.

### <a name="dynamodb_exception"></a> `AccessDeniedException` + `AmazonDynamoDBException`

```
java.nio.file.AccessDeniedException: bucket1:
  com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException:
  User: arn:aws:sts::980678866538:assumed-role/s3guard-test-role/test is not authorized to perform:
  dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-west-1:980678866538:table/bucket1
   (Service: AmazonDynamoDBv2; Status Code: 400;
```

The caller is trying to access an S3 bucket which uses S3Guard, but the caller
lacks the relevant DynamoDB access permissions.

The `dynamodb:DescribeTable` operation is the first one used in S3Guard to access,
the DynamoDB table, so it is often the first to fail. It can be a sign
that the role has no permissions at all to access the table named in the exception,
or just that this specific permission has been omitted.

If the role policy requested for the assumed role didn't ask for any DynamoDB
permissions, this is where all attempts to work with a S3Guarded bucket will
fail. Check the value of `fs.s3a.assumed.role.policy`
