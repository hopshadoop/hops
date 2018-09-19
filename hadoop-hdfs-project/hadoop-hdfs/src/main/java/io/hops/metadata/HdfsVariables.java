/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.common.CountersQueue;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.leaderElection.VarsRegister;
import io.hops.metadata.common.entity.ArrayVariable;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

public class HdfsVariables {

  private interface Handler{
    Object handle(VariableDataAccess<Variable, Variable.Finder> vd) throws StorageException;
  }

  private static Object handleVariable(Handler handler, boolean
      writeLock)
      throws StorageException {
    VariableDataAccess<Variable, Variable.Finder> vd = (VariableDataAccess)
        HdfsStorageFactory.getDataAccess(VariableDataAccess.class);

    boolean insideActiveTransaction = HdfsStorageFactory.getConnector()
        .isTransactionActive();
    if(!insideActiveTransaction){
      HdfsStorageFactory.getConnector().beginTransaction();
    }

    if(writeLock) {
      HdfsStorageFactory.getConnector().writeLock();
    }else{
      HdfsStorageFactory.getConnector().readLock();
    }

    try {
      Object response = handler.handle(vd);
      return response;
    }finally {
      if(!insideActiveTransaction){
        HdfsStorageFactory.getConnector().commit();
      }else{
        HdfsStorageFactory.getConnector().readCommitted();
      }
    }
  }

  private static Object handleVariableWithReadLock(Handler handler)
      throws StorageException {
    return handleVariable(handler, false);
  }

  private static Object handleVariableWithWriteLock(Handler handler)
      throws StorageException {
    return handleVariable(handler, true);
  }

  public static CountersQueue.Counter incrementBlockIdCounter(
      final long increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_BLOCK_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        return incrementCounter(Variable.Finder.BlockID, increment);
      }
    }.handle();
  }

  public static CountersQueue.Counter incrementINodeIdCounter(
      final long increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_INODE_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        return incrementCounter(Variable.Finder.INodeID, increment);
      }
    }.handle();
  }

  public static CountersQueue.Counter incrementQuotaUpdateIdCounter(
      final long increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_INODE_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        return incrementCounter(Variable.Finder.QuotaUpdateID, increment);
      }
    }.handle();
  }

  public static CountersQueue.Counter incrementCacheDirectiveIdCounter(
      final int increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(
        HDFSOperationType.UPDATE_CACHE_DIRECTIVE_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        return incrementCounter(Variable.Finder.CacheDirectiveID, increment);
      }
    }.handle();
  }
  
  private static CountersQueue.Counter incrementCounter(final Variable.Finder
      finder, final long increment)
      throws StorageException {

    return (CountersQueue.Counter) handleVariableWithWriteLock(new Handler() {
      @Override
      public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
          throws StorageException {
        Variable variable =  vd.getVariable(finder);
        if(variable instanceof IntVariable){
          assert increment==(int) increment;
          int oldValue = ((IntVariable) vd.getVariable(finder)).getValue();
          int newValue = IntMath.checkedAdd(oldValue, (int)increment);
          vd.setVariable(new IntVariable(finder, newValue));
          return new CountersQueue.Counter(oldValue, newValue);

        }else if(variable instanceof LongVariable){
          long oldValue = ((LongVariable) variable).getValue() == null ? 0: ((LongVariable) variable).getValue();
          long newValue = LongMath.checkedAdd(oldValue, increment);
          vd.setVariable(new LongVariable(finder, newValue));
          return new CountersQueue.Counter(oldValue, newValue);
        }


        throw new IllegalStateException("Cannot increment Variable of type " +
            variable.getClass().getSimpleName());
      }
    });
  }

  public static void resetMisReplicatedIndex() throws IOException {
    incrementMisReplicatedIndex(0);
  }

  public static Long incrementMisReplicatedIndex(final int increment)
      throws IOException {
    return (Long) new LightWeightRequestHandler(
        HDFSOperationType.INCREMENT_MIS_REPLICATED_FILES_INDEX) {
      @Override
      public Object performTask() throws IOException {

        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            LongVariable var = (LongVariable) vd
                .getVariable(Variable.Finder.MisReplicatedFilesIndex);
            long oldValue = var == null ? 0 : var.getValue();
            long newValue = increment == 0 ? 0 : LongMath.checkedAdd
                (oldValue, increment);
            vd.setVariable(new LongVariable(Variable.Finder.MisReplicatedFilesIndex,
                newValue));
            return newValue;
          }
        });
      }
    }.handle();
  }
  
  public static void setSafeModeInfo(final FSNamesystem.SafeModeInfo safeModeInfo, final long reached) throws
      IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_SAFE_MODE_INFO) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new LongVariable(Variable.Finder.SafeModeReached, reached));
            List<Object> vals = new ArrayList<>();
            if (safeModeInfo != null) {
              vals.add(safeModeInfo.getThreshold());
              vals.add(safeModeInfo.getDatanodeThreshold());
              vals.add(safeModeInfo.getExtension());
              vals.add(safeModeInfo.getSafeReplication());
              vals.add(safeModeInfo.getReplicationQueueThreshold());
              vals.add(safeModeInfo.isResourcesLow() ? 0 : 1);
            }
            vd.setVariable(new ArrayVariable(Variable.Finder.SafeModeInfo, vals));

            return null;
          }
        });
      }
    }.handle();
  }
  
  public static void exitSafeMode() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.EXIT_SAFE_MODE) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new LongVariable(Variable.Finder.SafeModeReached, -1));
            vd.setVariable(new ArrayVariable(Variable.Finder.SafeModeInfo, new ArrayList<>()));
            return null;
          }
        });
      }
    }.handle();
  }

  public static void setSafeModeReached(final long reached) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_SAFE_MODE_REACHED) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new LongVariable(Variable.Finder.SafeModeReached, reached));
            return null;
          }
        });
      }
    }.handle();
  }
  
  public static long getSafeModeReached() throws IOException {
    return (long) new LightWeightRequestHandler(HDFSOperationType.GET_SAFE_MODE_REACHED) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            LongVariable var =
                (LongVariable) vd.getVariable(Variable.Finder.SafeModeReached);
            return var.getValue();
          }
        });
      }
    }.handle();
  }

  public static List<Object> getSafeModeFromDB() throws IOException {
    return (List<Object>) new LightWeightRequestHandler(
        HDFSOperationType.GET_CLUSTER_SAFE_MODE) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            ArrayVariable var = (ArrayVariable) vd.getVariable(Variable.Finder.SafeModeInfo);
            return var.getVarsValue();

          }
        });
      }
    }.handle();
  }
  
  public static void setBrLbMasBlkPerMin(final long value) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_BR_LB_MAX_BLKS_PER_TW) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new LongVariable(Variable.Finder.BrLbMaxBlkPerTW, value));
            LOG.debug("Set block report max blocks per time window is : "+ value);
            return null;
          }
        });
      }
    }.handle();
  }

  public static long getBrLbMaxBlkPerTW() throws IOException {
    return (Long) new LightWeightRequestHandler(
            HDFSOperationType.GET_BR_LB_MAX_BLKS_PER_TW) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            LongVariable var =
                (LongVariable) vd.getVariable(Variable.Finder.BrLbMaxBlkPerTW);
            return var.getValue();
          }
        });
      }
    }.handle();
  }

  public static void setReplicationIndex(List<Integer> indeces)
      throws StorageException, TransactionContextException {
    Variables.updateVariable(
        new ArrayVariable(Variable.Finder.ReplicationIndex, indeces));
  }

  public static List<Integer> getReplicationIndex()
      throws StorageException, TransactionContextException {
    return (List<Integer>) ((ArrayVariable) Variables
        .getVariable(Variable.Finder.ReplicationIndex)).getVarsValue();
  }

  public static void setStorageInfo(StorageInfo storageInfo)
      throws StorageException, TransactionContextException {
    List<Object> vals = new ArrayList<>();
    vals.add(storageInfo.getLayoutVersion());
    vals.add(storageInfo.getNamespaceID());
    vals.add(storageInfo.getClusterID());
    vals.add(storageInfo.getCTime());
    vals.add(storageInfo.getBlockPoolId());
    vals.add(storageInfo.getStorageType().name());
    Variables
        .updateVariable(new ArrayVariable(Variable.Finder.StorageInfo, vals));
  }

  public static StorageInfo getStorageInfo()
      throws StorageException, TransactionContextException {
    ArrayVariable var =
        (ArrayVariable) Variables.getVariable(Variable.Finder.StorageInfo);
    List<Object> vals = (List<Object>) var.getVarsValue();

    if(vals.size()>=6){
      return new StorageInfo((Integer) vals.get(0), (Integer) vals.get(1),
        (String) vals.get(2), (Long) vals.get(3), HdfsServerConstants.NodeType.valueOf((String) vals.get(5)),
        (String) vals.get(4));
      
    }else{
      //updating from 2.8.2.4
      StorageInfo info = new StorageInfo((Integer) vals.get(0), (Integer) vals.get(1),
        (String) vals.get(2), (Long) vals.get(3), HdfsServerConstants.NodeType.NAME_NODE,
        (String) vals.get(4));
      setStorageInfo(info);
      return info;
    }
    
  }

  public static void setRollingUpgradeInfo(RollingUpgradeInfo rollingUpgradeInfo) throws TransactionContextException,
      StorageException {
    if (rollingUpgradeInfo != null) {
      ClientNamenodeProtocolProtos.RollingUpgradeInfoProto proto = PBHelper.convert(rollingUpgradeInfo);
      byte[] array = proto.toByteArray();
      Variables.updateVariable(new ByteArrayVariable(Variable.Finder.RollingUpgradeInfo, proto.toByteArray()));
    } else {
      Variables.updateVariable(new ByteArrayVariable(Variable.Finder.RollingUpgradeInfo, null));
    }
  }

  public static RollingUpgradeInfo getRollingUpgradeInfo() throws TransactionContextException, StorageException,
      InvalidProtocolBufferException {
    ByteArrayVariable var = (ByteArrayVariable) Variables.getVariable(Variable.Finder.RollingUpgradeInfo);
    if (var == null || var.getLength() <= 0) {
      return null;
    }
    byte[] array = var.getBytes();
    ClientNamenodeProtocolProtos.RollingUpgradeInfoProto proto = ClientNamenodeProtocolProtos.RollingUpgradeInfoProto.
        parseFrom((byte[]) var.getValue());
    return PBHelper.convert(proto);
  }
  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next)
      throws IOException {
    updateBlockTokenKeys(curr, next, null);
  }
  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next,
      BlockKey simple) throws IOException {
    ArrayVariable arr = new ArrayVariable(Variable.Finder.BlockTokenKeys);
    arr.addVariable(serializeBlockKey(curr, Variable.Finder.BTCurrKey));
    arr.addVariable(serializeBlockKey(next, Variable.Finder.BTNextKey));
    if (simple != null) {
      arr.addVariable(serializeBlockKey(simple, Variable.Finder.BTSimpleKey));
    }
    Variables.updateVariable(arr);
  }
  
  public static Map<Integer, BlockKey> getAllBlockTokenKeysByID()
      throws IOException {
    return getAllBlockTokenKeys(true, false);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByType()
      throws IOException {
    return getAllBlockTokenKeys(false, false);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByIDLW()
      throws IOException {
    return getAllBlockTokenKeys(true, true);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByTypeLW()
      throws IOException {
    return getAllBlockTokenKeys(false, true);
  }

  public static int getSIdCounter()
      throws StorageException, TransactionContextException {
    return (Integer) Variables.getVariable(Variable.Finder.SIdCounter)
        .getValue();
  }

  public static void setSIdCounter(int sid)
      throws StorageException, TransactionContextException {
    Variables.updateVariable(new IntVariable(Variable.Finder.SIdCounter, sid));
  }
  
  private static Map<Integer, BlockKey> getAllBlockTokenKeys(boolean useKeyId,
      boolean leightWeight) throws IOException {
    List<Variable> vars = (List<Variable>) (leightWeight ?
        getVariableLightWeight(Variable.Finder.BlockTokenKeys).getValue() :
        Variables.getVariable(Variable.Finder.BlockTokenKeys).getValue());
    Map<Integer, BlockKey> keys = new HashMap<>();
    for (Variable var : vars) {
      BlockKey key = deserializeBlockKey((ByteArrayVariable) var);
      int mapKey = useKeyId ? key.getKeyId() : key.getKeyType().ordinal();
      keys.put(mapKey, key);
    }
    return keys;
  }
  
  private static ByteArrayVariable serializeBlockKey(BlockKey key,
      Variable.Finder keyType) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    key.write(dos);
    dos.flush();
    return new ByteArrayVariable(keyType, os.toByteArray());
  }

  private static BlockKey deserializeBlockKey(ByteArrayVariable var)
      throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream((byte[]) var.getValue());
    DataInputStream dis = new DataInputStream(is);
    BlockKey key = new BlockKey();
    key.readFields(dis);
    switch (var.getType()) {
      case BTCurrKey:
        key.setKeyType(BlockKey.KeyType.CurrKey);
        break;
      case BTNextKey:
        key.setKeyType(BlockKey.KeyType.NextKey);
        break;
      case BTSimpleKey:
        key.setKeyType(BlockKey.KeyType.SimpleKey);
    }
    return key;
  }
  
  private static final Log LOG = LogFactory.getLog(HdfsVariables.class);
  
  public static boolean getNeedRescan()
      throws StorageException, TransactionContextException, IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.GET_NEED_RESCAN) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            return vd.getVariable(Variable.Finder.NeedRescan);
          }
        });
      }
    };
    IntVariable var = (IntVariable) handler.handle();
    if (var.getValue() != null && var.getValue() != 0) {
      return true;
    }
    return false;
  }

  public static void setNeedRescan(final boolean needRescan)
      throws StorageException, TransactionContextException, IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_NEED_RESCAN) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            int val;
            if (needRescan) {
              val = 1;
            } else {
              val = 0;
            }
            vd.setVariable(new IntVariable(Variable.Finder.NeedRescan, val));
            return null;
          }
        });
      }
    }.handle();
  }
  
  public static void setBlockTotal(final int value) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_BLOCK_TOTAL) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new IntVariable(Variable.Finder.BlockTotal, value));
            vd.setVariable(new IntVariable(Variable.Finder.BlockThreshold, 0));
            vd.setVariable(new IntVariable(Variable.Finder.BlockReplicationQueueThreshold, 0));
            System.out.println("GAUTIER blockTotal= " + value);
            return null;
          }
        });
      }
    }.handle();
  }

  public static void setBlockTotal(final int blockTotal, final int blockThreshold,
      final int blockReplicationQueueThreshold) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_BLOCK_TOTAL) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            vd.setVariable(new IntVariable(Variable.Finder.BlockTotal, blockTotal));
            vd.setVariable(new IntVariable(Variable.Finder.BlockThreshold, blockThreshold));
            vd.setVariable(new IntVariable(Variable.Finder.BlockReplicationQueueThreshold,
                blockReplicationQueueThreshold));
            System.out.println("GAUTIER blockTotal= " + blockTotal);
            return null;
          }
        });
      }
    }.handle();
  }
  
  public static void updateBlockTotal(final int deltaBlockTotal, final double threshold,
      final double replicationQueueThreshold) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.SET_BLOCK_TOTAL) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithWriteLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            IntVariable blockTotalVar = (IntVariable) vd.getVariable(Variable.Finder.BlockTotal);
            int blockTotal = blockTotalVar.getValue();
            IntVariable blockThresholdVar = (IntVariable) vd.getVariable(Variable.Finder.BlockThreshold);
            int blockThreshold = blockThresholdVar.getValue();
            IntVariable blockReplicationQueueThresholdVar = (IntVariable) vd.getVariable(
                Variable.Finder.BlockReplicationQueueThreshold);
            int blockReplicationQueueThreshold = blockReplicationQueueThresholdVar.getValue();

            assert blockTotal + deltaBlockTotal >= 0 : "Can't reduce blockTotal " + blockTotal + " by "
                + deltaBlockTotal + ": would be negative";

            int newBlockThreshold = (int) (blockTotal * threshold);
            int newBlockReplicationQueueThreshold = (int) (blockTotal * replicationQueueThreshold);
            vd.setVariable(new IntVariable(Variable.Finder.BlockTotal, blockTotal + deltaBlockTotal));
            vd.setVariable(new IntVariable(Variable.Finder.BlockThreshold, newBlockThreshold));
            vd.setVariable(new IntVariable(Variable.Finder.BlockReplicationQueueThreshold,
                newBlockReplicationQueueThreshold));
            int total = blockTotal + deltaBlockTotal;
            System.out.println("GAUTIER update blockTotal= " + total);
            return null;
          }
        });
      }
    }.handle();
  }

  public static int getBlockTotal() throws IOException {
    return (int) new LightWeightRequestHandler(HDFSOperationType.GET_BLOCK_TOTAL) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            IntVariable var =
                (IntVariable) vd.getVariable(Variable.Finder.BlockTotal);
            int result = 0;
            if(var.getValue()!=null){
              result = var.getValue();
            }
            return result;

          }
        });
      }
    }.handle();
  }
    
  public static int getBlockThreshold() throws IOException {
    return (int) new LightWeightRequestHandler(HDFSOperationType.GET_BLOCK_THRESHOLD) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            IntVariable var = (IntVariable) vd.getVariable(Variable.Finder.BlockThreshold);
            int result = 0;
            if(var.getValue()!=null){
              result = var.getValue();
            }
            return result;

          }
        });
      }
    }.handle();
  }

  public static int getBlockReplicationQueueThreshold() throws IOException {
    return (int) new LightWeightRequestHandler(HDFSOperationType.GET_BLOCK_REPLICATION_QUEUE_THRESHOLD) {
      @Override
      public Object performTask() throws IOException {
        return handleVariableWithReadLock(new Handler() {
          @Override
          public Object handle(VariableDataAccess<Variable, Variable.Finder> vd)
              throws StorageException {
            IntVariable var = (IntVariable) vd.getVariable(Variable.Finder.BlockReplicationQueueThreshold);
            int result = 0;
            if(var.getValue()!=null){
              result = var.getValue();
            }
            return result;

          }
        });
      }
    }.handle();
  }

  private static Variable getVariableLightWeight(final Variable.Finder varType)
      throws IOException {
    return (Variable) new LightWeightRequestHandler(
        HDFSOperationType.GET_VARIABLE) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) HdfsStorageFactory
            .getDataAccess(VariableDataAccess.class);
        return vd.getVariable(varType);
      }
    }.handle();
  }
  
  public static void registerDefaultValues(Configuration conf) {
    Variable.registerVariableDefaultValue(Variable.Finder.BlockID,
        new LongVariable(0).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.INodeID,
        new LongVariable(2)
            .getBytes()); // 1 is taken by the root and zero is parent of the root
    Variable.registerVariableDefaultValue(Variable.Finder.ReplicationIndex,
        new ArrayVariable(Arrays.asList(0, 0, 0, 0, 0)).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.SIdCounter,
        new IntVariable(0).getBytes());
    Variable
        .registerVariableDefaultValue(Variable.Finder.MisReplicatedFilesIndex,
            new LongVariable(0).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.SafeModeReached,
        new IntVariable(-1).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.QuotaUpdateID,
        new IntVariable(0).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.BrLbMaxBlkPerTW,
            new LongVariable(conf.getLong(DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW,
                    DFSConfigKeys.DFS_BR_LB_MAX_BLK_PER_TW_DEFAULT)).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.CacheDirectiveID,
        new LongVariable(1).getBytes());
    Variable.registerVariableDefaultValue(Variable.Finder.NeedRescan,
        new IntVariable(0).getBytes());
    VarsRegister.registerHdfsDefaultValues();
    // This is a workaround that is needed until HA-YARN has its own format command
    VarsRegister.registerYarnDefaultValues();
  }
}
