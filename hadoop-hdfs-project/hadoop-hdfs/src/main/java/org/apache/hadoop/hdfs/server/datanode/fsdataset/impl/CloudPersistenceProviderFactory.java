package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;

public class CloudPersistenceProviderFactory {

  public static CloudPersistenceProvider getCloudClient(Configuration conf) {
    String cloudProvider = conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER,
            DFSConfigKeys.DFS_CLOUD_PROVIDER_DEFAULT);
    if (cloudProvider.compareToIgnoreCase(CloudProvider.AWS.name()) == 0) {
      return new CloudPersistenceProviderS3Impl(conf);
    } else {
      throw new UnsupportedOperationException("Cloud provider '" + cloudProvider +
              "' is not supported");
    }
  }
}
