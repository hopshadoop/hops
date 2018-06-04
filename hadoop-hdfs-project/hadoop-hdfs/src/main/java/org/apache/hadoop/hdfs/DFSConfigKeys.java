/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.http.HttpConfig;

/**
 * This class contains constants for configuration keys used
 * in hdfs.
 */

@InterfaceAudience.Private
public class DFSConfigKeys extends CommonConfigurationKeys {
  

  public static final String DFS_STORAGE_DRIVER_JAR_FILE =
      "dfs.storage.driver.jarFile";

  public static final String DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT = "";

  public static final String DFS_STORAGE_DRIVER_CLASS =
      "dfs.storage.driver.class";
  public static final String DFS_STORAGE_DRIVER_CLASS_DEFAULT =
      "io.hops.metadata.ndb.NdbStorageFactory";

  public static final String DFS_STORAGE_DRIVER_CONFIG_FILE =
      "dfs.storage.driver.configfile";
  public static final String DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT =
      "ndb-config.properties";

  public static final String DFS_NAMENODE_QUOTA_ENABLED_KEY =
      "dfs.namenode.quota.enabled";
  public static final boolean DFS_NAMENODE_QUOTA_ENABLED_DEFAULT = true;

  public static final String DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY =
      "dfs.namenode.quota.update.interval";
  public static final int DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_DEFAULT = 1000;

  public static final String DFS_NAMENODE_QUOTA_UPDATE_LIMIT_KEY =
      "dfs.namenode.quota.update.limit";
  public static final int DFS_NAMENODE_QUOTA_UPDATE_LIMIT_DEFAULT = 100000;

  public static final String DFS_NAMENODE_QUOTA_UPDATE_ID_BATCH_SIZE =
      "dfs.namenode.quota.update.id.batchsize";
  public static final int DFS_NAMENODE_QUOTA_UPDATE_ID_BATCH_SIZ_DEFAULT =
      100000;

  public static final String DFS_NAMENODE_QUOTA_UPDATE_ID_UPDATE_THRESHOLD =
      "dfs.namenode.quota.update.updateThreshold";
  public static final float
      DFS_NAMENODE_QUOTA_UPDATE_ID_UPDATE_THRESHOLD_DEFAULT = (float) 0.5;

  public static final String DFS_NAMENODE_INODEID_BATCH_SIZE =
      "dfs.namenode.inodeid.batchsize";
  public static final int DFS_NAMENODE_INODEID_BATCH_SIZE_DEFAULT = 1000;

  public static final String DFS_NAMENODE_BLOCKID_BATCH_SIZE =
      "dfs.namenode.blockid.batchsize";
  public static final int DFS_NAMENODE_BLOCKID_BATCH_SIZE_DEFAULT = 1000;

  public static final String DFS_NAMENODE_INODEID_UPDATE_THRESHOLD =
      "dfs.namenode.inodeid.updateThreshold";
  public static final float DFS_NAMENODE_INODEID_UPDATE_THRESHOLD_DEFAULT =
      (float) 0.5;

  public static final String DFS_NAMENODE_BLOCKID_UPDATE_THRESHOLD =
      "dfs.namenode.blockid.updateThreshold";
  public static final float DFS_NAMENODE_BLOCKID_UPDATE_THRESHOLD_DEFAULT =
      (float) 0.5;

  public static final String DFS_NAMENODE_IDSMONITOR_CHECK_INTERVAL_IN_MS =
      "dfs.namenode.id.updateThreshold";
  public static final int DFS_NAMENODE_IDSMONITOR_CHECK_INTERVAL_IN_MS_DEFAULT =
      1000;

  public static final String DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE =
      "dfs.namenode.processReport.batchsize";
  public static final int DFS_NAMENODE_PROCESS_REPORT_BATCH_SIZE_DEFAULT =
      5000;
  
  public static final String DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE =
      "dfs.namenode.misreplicated.batchsize";
  public static final int
      DFS_NAMENODE_PROCESS_MISREPLICATED_BATCH_SIZE_DEFAULT = 1000;
  
  public static final String DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS =
      "dfs.namenode.misreplicated.noofbatches";
  public static final int
      DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_BATCHS_DEFAULT = 100;

  public static final String DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS =
      "dfs.namenode.misreplicated.noofbatches";
  public static final int
      DFS_NAMENODE_PROCESS_MISREPLICATED_NO_OF_THREADS_DEFAULT = 10;
  
  public static final String DFS_TRANSACTION_STATS_ENABLED =
      "dfs.transaction.stats.enabled";
  public static final boolean DFS_TRANSACTION_STATS_ENABLED_DEFAULT = false;

  public static final String DFS_TRANSACTION_STATS_DETAILED_ENABLED =
      "dfs.transaction.stats.detailed.enabled";
  public static final boolean DFS_TRANSACTION_STATS_DETAILED_ENABLED_DEFAULT =
      false;

  public static final String DFS_TRANSACTION_STATS_DIR =
      "dfs.transaction.stats.dir";
  public static final String DFS_TRANSACTION_STATS_DIR_DEFAULT =
      "/tmp/hopsstats";

  public static final String DFS_TRANSACTION_STATS_WRITER_ROUND =
      "dfs.transaction.stats.writerround";
  public static final int DFS_TRANSACTION_STATS_WRITER_ROUND_DEFAULT = 120;

  public static final String  DFS_DIR_DELETE_BATCH_SIZE=
      "dfs.dir.delete.batch.size";
  public static final int DFS_DIR_DELETE_BATCH_SIZE_DEFAULT = 50;

  /*for client failover api*/
  // format {ip:port, ip:port, ip:port} comma separated
  public static final String DFS_NAMENODES_RPC_ADDRESS_KEY =
      "dfs.namenodes.rpc.addresses";
  public static final String DFS_NAMENODES_RPC_ADDRESS_DEFAULT = "";

  // format {ip:port, ip:port, ip:port} comma separated
  public static final String DFS_NAMENODES_SERVICE_RPC_ADDRESS_KEY =
      "dfs.namenodes.servicerpc.addresses";

  public static final String DFS_NAMENODE_SELECTOR_POLICY_KEY =
      "dfs.namenode.selector-policy";
  public static final String DFS_NAMENODE_SELECTOR_POLICY_DEFAULT =
      "RANDOM_STICKY";     //RANDOM ROUND_ROBIN RANDOM_STICKY
  
  public static final String DFS_BLOCK_POOL_ID_KEY = "dfs.block.pool.id";
  public static final String DFS_BLOCK_POOL_ID_DEFAULT = "HOP_BLOCK_POOL_123";
  
  public static final String DFS_NAME_SPACE_ID_KEY = "dfs.name.space.id";
  public static final int DFS_NAME_SPACE_ID_DEFAULT = 911; // :)
  
  public static final String DFS_CLIENT_RETRIES_ON_FAILURE_KEY =
      "dfs.client.max.retries.on.failure";
  public static final int DFS_CLIENT_RETRIES_ON_FAILURE_DEFAULT = 2;
      //min value is 0. Better set it >= 1

  public static final String DFS_NAMENODE_TX_RETRY_COUNT_KEY =
          "dfs.namenode.tx.retry.count";
  public static final int DFS_NAMENODE_TX_RETRY_COUNT_DEFAULT = 5;

  public static final String DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_KEY =
          "dfs.namenode.tx.initial.wait.time.before.retry";
  public static final int DFS_NAMENODE_TX_INITIAL_WAIT_TIME_BEFORE_RETRY_DEFAULT = 2000;

  public static final String DFS_CLIENT_INITIAL_WAIT_ON_RETRY_IN_MS_KEY =
      "dfs.client.initial.wait.on.retry";
  public static final int DFS_CLIENT_INITIAL_WAIT_ON_RETRY_IN_MS_DEFAULT = 1000;
  
  public static final String DFS_CLIENT_REFRESH_NAMENODE_LIST_IN_MS_KEY =
      "dfs.client.refresh.namenode.list";
  public static final int DFS_CLIENT_REFRESH_NAMENODE_LIST_IN_MS_DEFAULT =
      60 * 1000; //time in milliseconds.
  
  public static final String DFS_SET_PARTITION_KEY_ENABLED =
      "dfs.ndb.setpartitionkey.enabled";
  public static final boolean DFS_SET_PARTITION_KEY_ENABLED_DEFAULT = true;

  public static final String  DFS_SET_RANDOM_PARTITION_KEY_ENABLED =
      "dfs.ndb.setrandompartitionkey.enabled";
  public static final boolean  DFS_SET_RANDOM_PARTITION_KEY_ENABLED_DEFAULT =
      true;

  public static final String DFS_RESOLVING_CACHE_ENABLED = "dfs" +
      ".resolvingcache.enabled";
  public static final boolean DFS_RESOLVING_CACHE_ENABLED_DEFAULT = true;
  
  public static final String DFS_MEMCACHE_SERVER =
      "dfs.resolvingcache.memcache.server.address";
  public static final String DFS_MEMCACHE_SERVER_DEFAULT = "127.0.0.1:11212";
  
  public static final String DFS_MEMCACHE_CONNECTION_POOL_SIZE =
      "dfs.resolvingcache.memcache.connectionpool.size";
  public static final int DFS_MEMCACHE_CONNECTION_POOL_SIZE_DEFAULT = 10;
  
  public static final String DFS_MEMCACHE_KEY_PREFIX =
      "dfs.resolvingcache.memcache.key.prefix";
  public static final String DFS_MEMCACHE_KEY_PREFIX_DEFAULT = "p:";
  
  public static final String DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS =
      "dfs.resolvingcache.memcache.key.expiry";
  public static final int DFS_MEMCACHE_KEY_EXPIRY_IN_SECONDS_DEFAULT = 0;

  public static final String DFS_RESOLVING_CACHE_TYPE = "dfs.resolvingcache" +
      ".type";

  //INode, Path, InMemory, Optimal
  public static final String DFS_RESOLVING_CACHE_TYPE_DEFAULT = "InMemory";

  public static final String DFS_INMEMORY_CACHE_MAX_SIZE = "dfs" +
      ".resolvingcache.inmemory.maxsize";
  public static final int DFS_INMEMORY_CACHE_MAX_SIZE_DEFAULT = 100000;
  
  public static final String DFS_NDC_ENABLED_KEY = "dfs.ndc.enable";
  public static final boolean DFS_NDC_ENABLED_DEFAULT = false;

  public static final String DFS_SUBTREE_EXECUTOR_LIMIT_KEY =
      "dfs.namenode.subtree-executor-limit";
  public static final int DFS_SUBTREE_EXECUTOR_LIMIT_DEFAULT = 80;

  public static final String DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_KEY =
          "dfs.subtree.clean.failed.ops.locks.delay";
  public static final long DFS_SUBTREE_CLEAN_FAILED_OPS_LOCKS_DELAY_DEFAULT =
          10*1000*60; //Reclaim locks after 10 mins

  public static final String DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY =
          "dfs.namenode.subtree.hierarchical.locking";
  public static final boolean DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY_DEFAULT = true;

  public static final String ERASURE_CODING_CODECS_KEY =
      "dfs.erasure_coding.codecs.json";
  public static final String ERASURE_CODING_ENABLED_KEY =
      "dfs.erasure_coding.enabled";
  public static final boolean DEFAULT_ERASURE_CODING_ENABLED_KEY = false;
  public static final String PARITY_FOLDER = "dfs.erasure_coding.parity_folder";
  public static final String DEFAULT_PARITY_FOLDER = "/parity";
  public static final String ENCODING_MANAGER_CLASSNAME_KEY =
      "dfs.erasure_coding.encoding_manager";
  public static final String DEFAULT_ENCODING_MANAGER_CLASSNAME =
      "io.hops.erasure_coding.MapReduceEncodingManager";
  public static final String BLOCK_REPAIR_MANAGER_CLASSNAME_KEY =
      "dfs.erasure_coding.block_rapair_manager";
  public static final String DEFAULT_BLOCK_REPAIR_MANAGER_CLASSNAME =
      "io.hops.erasure_coding.MapReduceBlockRepairManager";
  public static final String RECHECK_INTERVAL_KEY =
      "dfs.erasure_coding.recheck_interval";
  public static final int DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;
  public static final String ACTIVE_ENCODING_LIMIT_KEY =
      "dfs.erasure_coding.active_encoding_limit";
  public static final int DEFAULT_ACTIVE_ENCODING_LIMIT = 10;
  public static final String ACTIVE_REPAIR_LIMIT_KEY =
      "dfs.erasure_coding.active_repair_limit";
  public static final int DEFAULT_ACTIVE_REPAIR_LIMIT = 10;
  public static final String REPAIR_DELAY_KEY =
      "dfs.erasure_coding.repair_delay";
  public static final int DEFAULT_REPAIR_DELAY_KEY = 30 * 60 * 1000;
  public static final String ACTIVE_PARITY_REPAIR_LIMIT_KEY =
      "dfs.erasure_coding.active_parity_repair_limit";
  public static final int DEFAULT_ACTIVE_PARITY_REPAIR_LIMIT = 10;
  public static final String PARITY_REPAIR_DELAY_KEY =
      "dfs.erasure_coding.parity_repair_delay";
  public static final int DEFAULT_PARITY_REPAIR_DELAY = 30 * 60 * 1000;
  public static final String DELETION_LIMIT_KEY =
      "dfs.erasure_coding.deletion_limit";
  public static final int DEFAULT_DELETION_LIMIT = 100;

  public static final String DFS_BR_LB_MAX_BLK_PER_TW =
          "dfs.block.report.load.balancing.max.blks.per.time.window";
  public static final long DFS_BR_LB_MAX_BLK_PER_TW_DEFAULT = 1000000;
  
  public static final String DFS_BR_LB_TIME_WINDOW_SIZE =
          "dfs.block.report.load.balancing.time.window.size";
  public static final long DFS_BR_LB_TIME_WINDOW_SIZE_DEFAULT = 60*1000; //1 min
  
  public static final String DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD =
          "dfs.blk.report.load.balancing.db.var.update.threashold";
  public static final long DFS_BR_LB_DB_VAR_UPDATE_THRESHOLD_DEFAULT = 60*1000;
  
  public static final String DFS_STORE_SMALL_FILES_IN_DB_KEY =
          "dfs.store.small.files.in.db";
  public static final boolean DFS_STORE_SMALL_FILES_IN_DB_DEFAULT = false;

  public static final String DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_KEY =
          "dfs.db.ondisk.small.file.max.size";
  public static final int DFS_DB_ONDISK_SMALL_FILE_MAX_SIZE_DEFAULT = 2000;

  public static final String DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_KEY =
          "dfs.db.ondisk.medium.file.max.size";
  public static final int DFS_DB_ONDISK_MEDIUM_FILE_MAX_SIZE_DEFAULT = 4000;

  public static final String DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_KEY =
          "dfs.db.ondisk.large.file.max.size";
  public static final int DFS_DB_ONDISK_LARGE_FILE_MAX_SIZE_DEFAULT = 64*1024;

  public static final String DFS_DB_FILE_MAX_SIZE_KEY =
          "dfs.db.file.max.size";
  public static final int DFS_DB_FILE_MAX_SIZE_DEFAULT = 64 * 1024;

  public static final String DFS_DB_INMEMORY_FILE_MAX_SIZE_KEY =
          "dfs.db.inmemory.file.max.size";
  public static final int DFS_DB_INMEMORY_FILE_MAX_SIZE_DEFAULT = 1*1024; // 1KB

  public static final String DFS_DN_INCREMENTAL_BR_DISPATCHER_THREAD_POOL_SIZE_KEY =
          "dfs.dn.incremental.br.thread.pool.size";
  public static final int DFS_DN_INCREMENTAL_BR_DISPATCHER_THREAD_POOL_SIZE_DEFAULT = 256;

  public static final String DFS_CLIENT_DELAY_BEFORE_FILE_CLOSE_KEY = "dsf.client.delay.before.file.close";
  public static final int DFS_CLIENT_DELAY_BEFORE_FILE_CLOSE_DEFAULT = 0;

  public static final String DFS_BLOCK_SIZE_KEY = "dfs.blocksize";
  public static final long DFS_BLOCK_SIZE_DEFAULT = 128 * 1024 * 1024;
  public static final String DFS_REPLICATION_KEY = "dfs.replication";
  public static final short DFS_REPLICATION_DEFAULT = 3;
  public static final String DFS_STREAM_BUFFER_SIZE_KEY =
      "dfs.stream-buffer-size";
  public static final int DFS_STREAM_BUFFER_SIZE_DEFAULT = 4096;
  public static final String DFS_BYTES_PER_CHECKSUM_KEY =
      "dfs.bytes-per-checksum";
  public static final int DFS_BYTES_PER_CHECKSUM_DEFAULT = 512;
  public static final String DFS_CLIENT_RETRY_POLICY_ENABLED_KEY =
      "dfs.client.retry.policy.enabled";
  public static final boolean DFS_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false;
  public static final String DFS_CLIENT_RETRY_POLICY_SPEC_KEY =
      "dfs.client.retry.policy.spec";
  public static final String DFS_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "10000,6,60000,10"; //t1,n1,t2,n2,...
  public static final String DFS_CHECKSUM_TYPE_KEY = "dfs.checksum.type";
  public static final String DFS_CHECKSUM_TYPE_DEFAULT = "CRC32C";
  public static final String DFS_CLIENT_WRITE_PACKET_SIZE_KEY =
      "dfs.client-write-packet-size";
  public static final int DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64 * 1024;
  public static final String
      DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_KEY =
      "dfs.client.block.write.replace-datanode-on-failure.enable";
  public static final boolean
      DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_ENABLE_DEFAULT = true;
  public static final String
      DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_KEY =
      "dfs.client.block.write.replace-datanode-on-failure.policy";
  public static final String
      DFS_CLIENT_WRITE_REPLACE_DATANODE_ON_FAILURE_POLICY_DEFAULT = "DEFAULT";
  public static final String DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY =
      "dfs.client.socketcache.capacity";
  public static final int DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT = 16;
  public static final String DFS_CLIENT_USE_DN_HOSTNAME =
      "dfs.client.use.datanode.hostname";
  public static final boolean DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT = false;
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_WRITES = "dfs.client.cache.drop.behind.writes";
  public static final String  DFS_CLIENT_CACHE_DROP_BEHIND_READS = "dfs.client.cache.drop.behind.reads";
  public static final String  DFS_CLIENT_CACHE_READAHEAD = "dfs.client.cache.readahead";
  public static final String DFS_HDFS_BLOCKS_METADATA_ENABLED =
      "dfs.datanode.hdfs-blocks-metadata.enabled";
  public static final boolean DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT = false;
  public static final String
      DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS =
      "dfs.client.file-block-storage-locations.num-threads";
  public static final int
      DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT = 10;
  public static final String DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT =
      "dfs.client.file-block-storage-locations.timeout";
  public static final int
      DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_DEFAULT = 60;
  public static final String  DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH = "dfs.client.retry.times.get-last-block-length";
  public static final int     DFS_CLIENT_RETRY_TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT = 3;
  public static final String  DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH = "dfs.client.retry.interval-ms.get-last-block-length";
  public static final int     DFS_CLIENT_RETRY_INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT = 4000;
  public static final String DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT =
      "^(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?(,(default:)?(user|group|mask|other):[[A-Za-z_][A-Za-z0-9._-]]*:([rwx-]{3})?)*$";

  // HA related configuration
  public static final String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX =
      "dfs.client.failover.proxy.provider";
  public static final String DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY =
      "dfs.client.failover.max.attempts";
  public static final int DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY =
      "dfs.client.failover.sleep.base.millis";
  public static final int DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY =
      "dfs.client.failover.sleep.max.millis";
  public static final int DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
  public static final String DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY =
      "dfs.client.failover.connection.retries";
  public static final int DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT = 0;
  public static final String
      DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
      "dfs.client.failover.connection.retries.on.timeouts";
  public static final int
      DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 0;
  public static final String  DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY = "dfs.client.retry.max.attempts";
  public static final int     DFS_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
  
  public static final String DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY =
      "dfs.client.socketcache.expiryMsec";
  public static final long DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT = 3000;
  public static final String  DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL = "dfs.client.write.exclude.nodes.cache.expiry.interval.millis";
  public static final long    DFS_CLIENT_WRITE_EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT = 10 * 60 * 1000; // 10 minutes, in ms
  public static final String DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY =
      "dfs.datanode.balance.bandwidthPerSec";
  public static final long DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT =
      1024 * 1024;
  public static final String  DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY = "dfs.datanode.balance.max.concurrent.moves";
  public static final int     DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT = 5;

  public static final String DFS_DATANODE_READAHEAD_BYTES_KEY =
      "dfs.datanode.readahead.bytes";
  public static final long DFS_DATANODE_READAHEAD_BYTES_DEFAULT =
      4 * 1024 * 1024; // 4MB
  public static final String DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY =
      "dfs.datanode.drop.cache.behind.writes";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT =
      false;
  public static final String DFS_DATANODE_SYNC_BEHIND_WRITES_KEY =
      "dfs.datanode.sync.behind.writes";
  public static final boolean DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT = false;
  public static final String DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY =
      "dfs.datanode.drop.cache.behind.reads";
  public static final boolean DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT =
      false;
  public static final String DFS_DATANODE_USE_DN_HOSTNAME =
      "dfs.datanode.use.datanode.hostname";
  public static final boolean DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT = false;

  public static final String DFS_NAMENODE_HTTP_PORT_KEY = "dfs.http.port";
  public static final int DFS_NAMENODE_HTTP_PORT_DEFAULT = 50070;
  public static final String DFS_NAMENODE_HTTP_ADDRESS_KEY =
      "dfs.namenode.http-address";
  public static final String DFS_NAMENODE_HTTP_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_NAMENODE_HTTP_PORT_DEFAULT;
  public static final String DFS_NAMENODE_RPC_ADDRESS_KEY =
      "dfs.namenode.rpc-address";
  public static final String  DFS_NAMENODE_RPC_BIND_HOST_KEY = "dfs.namenode.rpc-bind-host";
  public static final String DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY =
      "dfs.namenode.servicerpc-address";
  public static final String  DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY = "dfs.namenode.servicerpc-bind-host";
  public static final String DFS_NAMENODE_MAX_OBJECTS_KEY =
      "dfs.namenode.max.objects";
  public static final long DFS_NAMENODE_MAX_OBJECTS_DEFAULT = 0;
  public static final String DFS_NAMENODE_SAFEMODE_EXTENSION_KEY =
      "dfs.namenode.safemode.extension";
  public static final int DFS_NAMENODE_SAFEMODE_EXTENSION_DEFAULT = 30000;
  public static final String DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY =
      "dfs.namenode.safemode.threshold-pct";
  public static final float DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT =
      0.999f;
  // set this to a slightly smaller value than
  // DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT to populate
  // needed replication queues before exiting safe mode
  public static final String DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY =
      "dfs.namenode.replqueue.threshold-pct";
  public static final String DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY =
      "dfs.namenode.safemode.min.datanodes";
  public static final int DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT = 0;
  public static final String DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY =
      "dfs.namenode.heartbeat.recheck-interval";
  public static final int DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT =
      5 * 60 * 1000;
  public static final String DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY =
      "dfs.namenode.tolerate.heartbeat.multiplier";
  public static final int DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT =
      4;
  public static final String DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY =
      "dfs.client.https.keystore.resource";
  public static final String DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT =
      "ssl-client.xml";
  public static final String DFS_CLIENT_HTTPS_NEED_AUTH_KEY =
      "dfs.client.https.need-auth";
  public static final boolean DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT = false;
  public static final String DFS_CLIENT_CACHED_CONN_RETRY_KEY =
      "dfs.client.cached.conn.retry";
  public static final int DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT = 3;
  public static final String DFS_NAMENODE_ACCESSTIME_PRECISION_KEY =
      "dfs.namenode.accesstime.precision";
  public static final long DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT = 3600000;
  public static final String DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY =
      "dfs.namenode.replication.considerLoad";
  public static final boolean DFS_NAMENODE_REPLICATION_CONSIDERLOAD_DEFAULT =
      true;
  public static final String DFS_NAMENODE_REPLICATION_INTERVAL_KEY =
      "dfs.namenode.replication.interval";
  public static final int DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT = 3;
  public static final String DFS_NAMENODE_REPLICATION_MIN_KEY =
      "dfs.namenode.replication.min";
  public static final int DFS_NAMENODE_REPLICATION_MIN_DEFAULT = 1;
  public static final String DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY =
      "dfs.namenode.replication.pending.timeout-sec";
  public static final int DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_DEFAULT =
      -1;
  public static final String DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY =
      "dfs.namenode.replication.max-streams";
  public static final int DFS_NAMENODE_REPLICATION_MAX_STREAMS_DEFAULT = 2;
  public static final String DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_KEY =
      "dfs.namenode.replication.max-streams-hard-limit";
  public static final int DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT =
      4;
  public static final String DFS_WEBHDFS_ENABLED_KEY = "dfs.webhdfs.enabled";
  public static final boolean DFS_WEBHDFS_ENABLED_DEFAULT = true;
  public static final String  DFS_WEBHDFS_USER_PATTERN_KEY = "dfs.webhdfs.user.provider.user.pattern";
  public static final String  DFS_WEBHDFS_USER_PATTERN_DEFAULT = "^[A-Za-z_][A-Za-z0-9._-]*[$]?$";
  public static final String DFS_PERMISSIONS_ENABLED_KEY =
      "dfs.permissions.enabled";
  public static final boolean DFS_PERMISSIONS_ENABLED_DEFAULT = true;
  public static final String DFS_PERMISSIONS_SUPERUSERGROUP_KEY =
      "dfs.permissions.superusergroup";
  public static final String DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT =
      "supergroup";
  public static final String DFS_ADMIN = "dfs.cluster.administrators";
  public static final String DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY =
      "dfs.https.server.keystore.resource";
  public static final String DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT =
      "ssl-server.xml";
  public static final String DFS_NAMENODE_NAME_DIR_RESTORE_KEY =
      "dfs.namenode.name.dir.restore";
  public static final String  DFS_NAMENODE_ACLS_ENABLED_KEY = "dfs.namenode.acls.enabled";
  public static final boolean DFS_NAMENODE_ACLS_ENABLED_DEFAULT = false;
  public static final boolean DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT = false;
  public static final String DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY =
      "dfs.namenode.support.allow.format";
  public static final boolean DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT = true;
  public static final String DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY =
      "dfs.namenode.min.supported.datanode.version";
  public static final String
      DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_DEFAULT = "2.1.0-beta";
  
  public static final String DFS_LIST_LIMIT = "dfs.ls.limit";
  public static final int DFS_LIST_LIMIT_DEFAULT = Integer.MAX_VALUE; //1000; [HopsFS] Jira Hops-45
  public static final String  DFS_CONTENT_SUMMARY_LIMIT_KEY = "dfs.content-summary.limit";
  public static final int     DFS_CONTENT_SUMMARY_LIMIT_DEFAULT = 0;
  public static final String DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY =
      "dfs.datanode.failed.volumes.tolerated";
  public static final int DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;
  public static final String DFS_DATANODE_SYNCONCLOSE_KEY =
      "dfs.datanode.synconclose";
  public static final boolean DFS_DATANODE_SYNCONCLOSE_DEFAULT = false;
  public static final String DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY =
      "dfs.datanode.socket.reuse.keepalive";
  public static final int DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT = 4000;
  
  public static final String DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY = "dfs.namenode.datanode.registration.ip-hostname-check";
  public static final boolean DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_DEFAULT = true;

  // Whether to enable datanode's stale state detection and usage for reads
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY =
      "dfs.namenode.avoid.read.stale.datanode";
  public static final boolean
      DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT = false;
  // Whether to enable datanode's stale state detection and usage for writes
  public static final String DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY =
      "dfs.namenode.avoid.write.stale.datanode";
  public static final boolean
      DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT = false;
  // The default value of the time interval for marking datanodes as stale
  public static final String DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY =
      "dfs.namenode.stale.datanode.interval";
  public static final long DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT =
      30 * 1000; // 30s
  // The stale interval cannot be too small since otherwise this may cause too frequent churn on stale states. 
  // This value uses the times of heartbeat interval to define the minimum value for stale interval.  
  public static final String DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY =
      "dfs.namenode.stale.datanode.minimum.interval";
  public static final int DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT =
      3; // i.e. min_interval is 3 * heartbeat_interval = 9s
  
  // When the percentage of stale datanodes reaches this ratio,
  // allow writing to stale nodes to prevent hotspots.
  public static final String
      DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY =
      "dfs.namenode.write.stale.datanode.ratio";
  public static final float
      DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT = 0.5f;

  // Replication monitoring related keys
  public static final String DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION =
      "dfs.namenode.invalidate.work.pct.per.iteration";
  public static final float
      DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT = 0.32f;
  public static final String
      DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION =
      "dfs.namenode.replication.work.multiplier.per.iteration";
  public static final int
      DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT = 2;

  //Delegation token related keys
  public static final String DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_KEY =
      "dfs.namenode.delegation.key.update-interval";
  public static final long DFS_NAMENODE_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000; // 1 day
  public static final String DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
      "dfs.namenode.delegation.token.renew-interval";
  public static final long
      DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
      24 * 60 * 60 * 1000;  // 1 day
  public static final String DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY =
      "dfs.namenode.delegation.token.max-lifetime";
  public static final long DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
      7 * 24 * 60 * 60 * 1000; // 7 days
  public static final String DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY =
      "dfs.namenode.delegation.token.always-use"; // for tests
  public static final boolean DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_DEFAULT =
      false;

  //Filesystem limit keys
  public static final String DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY =
      "dfs.namenode.fs-limits.max-component-length";
  public static final int DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT = 0;
      // no limit
  public static final String DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY =
      "dfs.namenode.fs-limits.max-directory-items";
  public static final int DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT = 0;
  public static final String  DFS_NAMENODE_MIN_BLOCK_SIZE_KEY = "dfs.namenode.fs-limits.min-block-size";
  public static final long    DFS_NAMENODE_MIN_BLOCK_SIZE_DEFAULT = 1024*1024;
  public static final String  DFS_NAMENODE_MAX_BLOCKS_PER_FILE_KEY = "dfs.namenode.fs-limits.max-blocks-per-file";
  public static final long    DFS_NAMENODE_MAX_BLOCKS_PER_FILE_DEFAULT = 1024*1024;

  //Following keys have no defaults
  public static final String DFS_DATANODE_DATA_DIR_KEY =
      "dfs.datanode.data.dir";
  public static final String DFS_NAMENODE_HTTPS_PORT_KEY = "dfs.https.port";
  public static final int DFS_NAMENODE_HTTPS_PORT_DEFAULT = 50470;
  public static final String DFS_NAMENODE_HTTPS_ADDRESS_KEY =
      "dfs.namenode.https-address";
  public static final String DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_NAMENODE_HTTPS_PORT_DEFAULT;
  public static final String DFS_CLIENT_READ_PREFETCH_SIZE_KEY =
      "dfs.client.read.prefetch.size";
  public static final String DFS_CLIENT_RETRY_WINDOW_BASE =
      "dfs.client.retry.window.base";
  public static final String DFS_METRICS_SESSION_ID_KEY =
      "dfs.metrics.session-id";
  public static final String DFS_METRICS_PERCENTILES_INTERVALS_KEY =
      "dfs.metrics.percentiles.intervals";
  public static final String DFS_DATANODE_HOST_NAME_KEY =
      "dfs.datanode.hostname";
  public static final String DFS_NAMENODE_HOSTS_KEY = "dfs.namenode.hosts";
  public static final String DFS_NAMENODE_HOSTS_EXCLUDE_KEY =
      "dfs.namenode.hosts.exclude";
  public static final String DFS_CLIENT_SOCKET_TIMEOUT_KEY =
      "dfs.client.socket-timeout";
  public static final String DFS_HOSTS = "dfs.hosts";
  public static final String DFS_HOSTS_EXCLUDE = "dfs.hosts.exclude";
  public static final String DFS_CLIENT_LOCAL_INTERFACES =
      "dfs.client.local.interfaces";
  public static final String DFS_NAMENODE_AUDIT_LOGGERS_KEY =
      "dfs.namenode.audit.loggers";
  public static final String DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME = "default";
  public static final String  DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_KEY = "dfs.namenode.audit.log.token.tracking.id";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_TOKEN_TRACKING_ID_DEFAULT = false;
  public static final String  DFS_NAMENODE_AUDIT_LOG_ASYNC_KEY = "dfs.namenode.audit.log.async";
  public static final boolean DFS_NAMENODE_AUDIT_LOG_ASYNC_DEFAULT = false;
 

  // Much code in hdfs is not yet updated to use these keys.
  public static final String
      DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY =
      "dfs.client.block.write.locateFollowingBlock.retries";
  public static final int
      DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT = 10;
      //HOP default was 5
  public static final String DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY =
      "dfs.client.block.write.retries";
  public static final int DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT = 3;
  public static final String DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY =
      "dfs.client.max.block.acquire.failures";
  public static final int DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT = 3;
  public static final String DFS_CLIENT_USE_LEGACY_BLOCKREADER =
      "dfs.client.use.legacy.blockreader";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT = false;
  public static final String  DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL = "dfs.client.use.legacy.blockreader.local";
  public static final boolean DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT = false;
  public static final String  DFS_BALANCER_MOVEDWINWIDTH_KEY = "dfs.balancer.movedWinWidth";
  public static final long    DFS_BALANCER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_BALANCER_MOVERTHREADS_KEY = "dfs.balancer.moverThreads";
  public static final int     DFS_BALANCER_MOVERTHREADS_DEFAULT = 1000;
  public static final String  DFS_BALANCER_DISPATCHERTHREADS_KEY = "dfs.balancer.dispatcherThreads";
  public static final int     DFS_BALANCER_DISPATCHERTHREADS_DEFAULT = 200;

  public static final String  DFS_MOVER_MOVEDWINWIDTH_KEY = "dfs.mover.movedWinWidth";
  public static final long    DFS_MOVER_MOVEDWINWIDTH_DEFAULT = 5400*1000L;
  public static final String  DFS_MOVER_MOVERTHREADS_KEY = "dfs.mover.moverThreads";
  public static final int     DFS_MOVER_MOVERTHREADS_DEFAULT = 1000;

  public static final String  DFS_DATANODE_ADDRESS_KEY = "dfs.datanode.address";
  public static final int     DFS_DATANODE_DEFAULT_PORT = 50010;
  public static final String  DFS_DATANODE_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_DEFAULT_PORT;
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_KEY = "dfs.datanode.data.dir.perm";
  public static final String  DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT = "700";
  public static final String  DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY = "dfs.datanode.directoryscan.interval";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT = 21600;
  public static final String  DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY = "dfs.datanode.directoryscan.threads";
  public static final int     DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT = 1;
  public static final String  DFS_DATANODE_DNS_INTERFACE_KEY = "dfs.datanode.dns.interface";
  public static final String  DFS_DATANODE_DNS_INTERFACE_DEFAULT = "default";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_KEY = "dfs.datanode.dns.nameserver";
  public static final String  DFS_DATANODE_DNS_NAMESERVER_DEFAULT = "default";
  public static final String  DFS_DATANODE_DU_RESERVED_KEY = "dfs.datanode.du.reserved";
  public static final long    DFS_DATANODE_DU_RESERVED_DEFAULT = 0;
  public static final String  DFS_DATANODE_HANDLER_COUNT_KEY = "dfs.datanode.handler.count";
  public static final int     DFS_DATANODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";
  public static final int     DFS_DATANODE_HTTP_DEFAULT_PORT = 50075;
  public static final String  DFS_DATANODE_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + DFS_DATANODE_HTTP_DEFAULT_PORT;
  public static final String  DFS_DATANODE_MAX_RECEIVER_THREADS_KEY = "dfs.datanode.max.transfer.threads";
  public static final int     DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT = 4096;

  public static final String DFS_DATANODE_NUMBLOCKS_KEY =
      "dfs.datanode.numblocks";
  public static final int DFS_DATANODE_NUMBLOCKS_DEFAULT = 64;
  public static final String DFS_DATANODE_SCAN_PERIOD_HOURS_KEY =
      "dfs.datanode.scan.period.hours";
  public static final int DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT = 0;
  public static final String DFS_DATANODE_TRANSFERTO_ALLOWED_KEY =
      "dfs.datanode.transferTo.allowed";
  public static final boolean DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT = true;
  public static final String DFS_HEARTBEAT_INTERVAL_KEY =
      "dfs.heartbeat.interval";
  public static final long DFS_HEARTBEAT_INTERVAL_DEFAULT = 3;
  public static final String DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY =
      "dfs.namenode.decommission.interval";
  public static final int DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT = 30;
  public static final String DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY =
      "dfs.namenode.decommission.nodes.per.interval";
  public static final int DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT =
      5;
  public static final String DFS_NAMENODE_HANDLER_COUNT_KEY =
      "dfs.namenode.handler.count";
  public static final int DFS_NAMENODE_HANDLER_COUNT_DEFAULT = 10;
  public static final String DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY =
      "dfs.namenode.service.handler.count";
  public static final int DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT = 10;
  public static final String DFS_SUPPORT_APPEND_KEY = "dfs.support.append";
  public static final boolean DFS_SUPPORT_APPEND_DEFAULT = true;
  public static final String DFS_HTTPS_ENABLE_KEY = "dfs.https.enable";
  public static final boolean DFS_HTTPS_ENABLE_DEFAULT = false;
  public static final String  DFS_HTTP_POLICY_KEY = "dfs.http.policy";
  public static final String  DFS_HTTP_POLICY_DEFAULT =  HttpConfig.Policy.HTTP_ONLY.name();
  public static final String DFS_DEFAULT_CHUNK_VIEW_SIZE_KEY =
      "dfs.default.chunk.view.size";
  public static final int DFS_DEFAULT_CHUNK_VIEW_SIZE_DEFAULT = 32 * 1024;
  public static final String DFS_DATANODE_HTTPS_ADDRESS_KEY =
      "dfs.datanode.https.address";
  public static final String DFS_DATANODE_HTTPS_PORT_KEY =
      "datanode.https.port";
  public static final int DFS_DATANODE_HTTPS_DEFAULT_PORT = 50475;
  public static final String DFS_DATANODE_HTTPS_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_DATANODE_HTTPS_DEFAULT_PORT;
  public static final String DFS_DATANODE_IPC_ADDRESS_KEY =
      "dfs.datanode.ipc.address";
  public static final int DFS_DATANODE_IPC_DEFAULT_PORT = 50020;
  public static final String DFS_DATANODE_IPC_ADDRESS_DEFAULT =
      "0.0.0.0:" + DFS_DATANODE_IPC_DEFAULT_PORT;
  public static final String DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY =
      "dfs.datanode.min.supported.namenode.version";
  public static final String
      DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT = "2.1.0-beta";

  public static final String DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY =
      "dfs.block.access.token.enable";
  public static final boolean DFS_BLOCK_ACCESS_TOKEN_ENABLE_DEFAULT = false;
  public static final String DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY =
      "dfs.block.access.key.update.interval";
  public static final long DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_DEFAULT = 600L;
  public static final String DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY =
      "dfs.block.access.token.lifetime";
  public static final long DFS_BLOCK_ACCESS_TOKEN_LIFETIME_DEFAULT = 600L;

  public static final String DFS_BLOCK_REPLICATOR_CLASSNAME_KEY =
      "dfs.block.replicator.classname";
  public static final Class<BlockPlacementPolicyDefault>
      DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT =
      BlockPlacementPolicyDefault.class;

  public static final String DFS_REPLICATION_MAX_KEY = "dfs.replication.max";
  public static final int DFS_REPLICATION_MAX_DEFAULT = 512;
  public static final String DFS_DF_INTERVAL_KEY = "dfs.df.interval";
  public static final int DFS_DF_INTERVAL_DEFAULT = 60000;
  public static final String DFS_BLOCKREPORT_INTERVAL_MSEC_KEY =
      "dfs.blockreport.intervalMsec";
  public static final long DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT =
      60 * 60 * 1000L;
  public static final String DFS_BLOCKREPORT_INITIAL_DELAY_KEY =
      "dfs.blockreport.initialDelay";
  public static final int DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT = 0;
  public static final String  DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY = "dfs.blockreport.split.threshold";
  public static final long    DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT = 1000 * 1000;
  public static final String DFS_BLOCK_INVALIDATE_LIMIT_KEY =
      "dfs.block.invalidate.limit";
  public static final int DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT = 1000;
  public static final String DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED_KEY =
      "dfs.corruptfilesreturned.max";
  public static final int DFS_DEFAULT_MAX_CORRUPT_FILES_RETURNED = 500;

  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_KEY =
      "dfs.client.read.shortcircuit";
  public static final boolean DFS_CLIENT_READ_SHORTCIRCUIT_DEFAULT = false;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_KEY =
      "dfs.client.read.shortcircuit.skip.checksum";
  public static final boolean
      DFS_CLIENT_READ_SHORTCIRCUIT_SKIP_CHECKSUM_DEFAULT = false;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_KEY =
      "dfs.client.read.shortcircuit.buffer.size";
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_KEY = "dfs.client.read.shortcircuit.streams.cache.size";
  public static final int DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_SIZE_DEFAULT = 100;
  public static final String DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_KEY = "dfs.client.read.shortcircuit.streams.cache.expiry.ms";
  public static final long DFS_CLIENT_READ_SHORTCIRCUIT_STREAMS_CACHE_EXPIRY_MS_DEFAULT = 5000;
  public static final int DFS_CLIENT_READ_SHORTCIRCUIT_BUFFER_SIZE_DEFAULT =
      1024 * 1024;
  public static final String DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC = "dfs.client.domain.socket.data.traffic";
  public static final boolean DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT = false;
  public static final String DFS_CLIENT_MMAP_CACHE_SIZE = "dfs.client.mmap.cache.size";
  public static final int DFS_CLIENT_MMAP_CACHE_SIZE_DEFAULT = 1024;
  public static final String DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS = "dfs.client.mmap.cache.timeout.ms";
  public static final long DFS_CLIENT_MMAP_CACHE_TIMEOUT_MS_DEFAULT  = 15 * 60 * 1000;
  public static final String DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT = "dfs.client.mmap.cache.thread.runs.per.timeout";
  public static final int DFS_CLIENT_MMAP_CACHE_THREAD_RUNS_PER_TIMEOUT_DEFAULT  = 4;

  //Keys with no defaults
  public static final String DFS_DATANODE_PLUGINS_KEY = "dfs.datanode.plugins";
  public static final String DFS_DATANODE_FSDATASET_FACTORY_KEY =
      "dfs.datanode.fsdataset.factory";
  public static final String DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY =
      "dfs.datanode.fsdataset.volume.choosing.policy";
  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold";
  public static final long    DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT = 1024L * 1024L * 1024L * 10L; // 10 GB
  public static final String  DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY = "dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction";
  public static final float   DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT = 0.75f;
  public static final String DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY =
      "dfs.datanode.socket.write.timeout";
  public static final String DFS_DATANODE_STARTUP_KEY = "dfs.datanode.startup";
  public static final String DFS_NAMENODE_PLUGINS_KEY = "dfs.namenode.plugins";
  public static final String DFS_WEB_UGI_KEY = "dfs.web.ugi";
  public static final String DFS_NAMENODE_STARTUP_KEY = "dfs.namenode.startup";
  public static final String DFS_DATANODE_KEYTAB_FILE_KEY =
      "dfs.datanode.keytab.file";
  public static final String DFS_DATANODE_USER_NAME_KEY =
      "dfs.datanode.kerberos.principal";
  public static final String DFS_NAMENODE_KEYTAB_FILE_KEY =
      "dfs.namenode.keytab.file";
  public static final String DFS_NAMENODE_USER_NAME_KEY =
      "dfs.namenode.kerberos.principal";
  public static final String DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY =
      "dfs.namenode.kerberos.internal.spnego.principal";
  public static final String DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY =
      "dfs.namenode.name.cache.threshold";
  public static final int DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT = 10;

  public static final String DFS_NAMESERVICES = "dfs.nameservices";
  public static final String DFS_NAMESERVICE_ID = "dfs.nameservice.id";
  public static final String DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY =
      "dfs.namenode.resource.check.interval";
  public static final int DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT = 5000;
  public static final String DFS_NAMENODE_DU_RESERVED_KEY =
      "dfs.namenode.resource.du.reserved";
  public static final long DFS_NAMENODE_DU_RESERVED_DEFAULT = 1024 * 1024 * 100;
      // 100 MB
  public static final String DFS_NAMENODE_CHECKED_VOLUMES_KEY =
      "dfs.namenode.resource.checked.volumes";
  public static final String DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY =
      "dfs.namenode.resource.checked.volumes.minimum";
  public static final int DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT = 1;
  public static final String DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY =
      "dfs.web.authentication.kerberos.principal";
  public static final String DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY =
      "dfs.web.authentication.kerberos.keytab";
  public static final String DFS_NAMENODE_MAX_OP_SIZE_KEY =
      "dfs.namenode.max.op.size";
  public static final int DFS_NAMENODE_MAX_OP_SIZE_DEFAULT = 50 * 1024 * 1024;
  
  public static final String DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY =
      "dfs.block.local-path-access.user";
  public static final String DFS_DOMAIN_SOCKET_PATH_KEY = "dfs.domain.socket.path";
  public static final String DFS_DOMAIN_SOCKET_PATH_DEFAULT = "";
  
  public static final String  DFS_STORAGE_POLICY_ENABLED_KEY = "dfs.storage.policy.enabled";
  public static final boolean DFS_STORAGE_POLICY_ENABLED_DEFAULT = true;

  // HA related configuration
  public static final String DFS_HA_NAMENODES_KEY_PREFIX = "dfs.ha.namenodes";
  
  // Security-related configs
  public static final String DFS_ENCRYPT_DATA_TRANSFER_KEY =
      "dfs.encrypt.data.transfer";
  public static final boolean DFS_ENCRYPT_DATA_TRANSFER_DEFAULT = false;
  public static final String DFS_DATA_ENCRYPTION_ALGORITHM_KEY =
      "dfs.encrypt.data.transfer.algorithm";
  
  // Hash bucket config
  public static final String DFS_NUM_BUCKETS_KEY =
      "dfs.blockreport.numbuckets";
  public static final int DFS_NUM_BUCKETS_DEFAULT = 1000;
  
  // Handling unresolved DN topology mapping
  public static final String  DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY = 
      "dfs.namenode.reject-unresolved-dn-topology-mapping";
  public static final boolean DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT =
      false;
  
  public static final String DFS_MAX_NUM_BLOCKS_TO_LOG_KEY = "dfs.namenode.max-num-blocks-to-log";
  public static final long   DFS_MAX_NUM_BLOCKS_TO_LOG_DEFAULT = 1000l;
  
  public static final String DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY = "dfs.namenode.enable.retrycache";
  public static final boolean DFS_NAMENODE_ENABLE_RETRY_CACHE_DEFAULT = true;
  public static final String DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_KEY = "dfs.namenode.retrycache.expirytime.millis";
  public static final long DFS_NAMENODE_RETRY_CACHE_EXPIRYTIME_MILLIS_DEFAULT = 600000; // 10 minutes
  public static final String DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_KEY = "dfs.namenode.retrycache.heap.percent";
  public static final float DFS_NAMENODE_RETRY_CACHE_HEAP_PERCENT_DEFAULT = 0.03f;
  
  // The number of NN response dropped by client proactively in each RPC call.
  // For testing NN retry cache, we can set this property with positive value.
  public static final String DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_KEY = "dfs.client.test.drop.namenode.response.number";
  public static final int DFS_CLIENT_TEST_DROP_NAMENODE_RESPONSE_NUM_DEFAULT = 0;

  
  // Hidden configuration undocumented in hdfs-site. xml
  // Timeout to wait for block receiver and responder thread to stop
  public static final String DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY = "dfs.datanode.xceiver.stop.timeout.millis";
  public static final long   DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT = 60000;
  
  // WebHDFS retry policy
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY = "dfs.http.client.retry.policy.enabled";
  public static final boolean DFS_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false;
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY = "dfs.http.client.retry.policy.spec";
  public static final String  DFS_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT = "10000,6,60000,10"; //t1,n1,t2,n2,...
  public static final String  DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY = "dfs.http.client.failover.max.attempts";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT = 15;
  public static final String  DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_KEY = "dfs.http.client.retry.max.attempts";
  public static final int     DFS_HTTP_CLIENT_RETRY_MAX_ATTEMPTS_DEFAULT = 10;
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY = "dfs.http.client.failover.sleep.base.millis";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_BASE_DEFAULT = 500;
  public static final String  DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY = "dfs.http.client.failover.sleep.max.millis";
  public static final int     DFS_HTTP_CLIENT_FAILOVER_SLEEPTIME_MAX_DEFAULT = 15000;
}
