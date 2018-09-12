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
package io.hops.common;

import com.google.common.collect.Lists;
import io.hops.metadata.HdfsVariables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class IDsGeneratorFactory {

  private static final Log LOG = LogFactory.getLog(IDsGeneratorFactory.class);

  private class INodeIDGen extends IDsGenerator{
    INodeIDGen(int batchSize, float threshold) {
      super(batchSize, threshold);
    }

    @Override
    CountersQueue.Counter incrementCounter(int inc) throws IOException {
      return HdfsVariables.incrementINodeIdCounter(inc);
    }
  }

  private class BlockIDGen extends IDsGenerator{
    BlockIDGen(int batchSize, float threshold) {
      super(batchSize, threshold);
    }

    @Override
    CountersQueue.Counter incrementCounter(int inc) throws IOException {
      return HdfsVariables.incrementBlockIdCounter(inc);
    }
  }

  private class QuotaUpdateIDGen extends IDsGenerator{
    QuotaUpdateIDGen(int batchSize, float threshold) {
      super(batchSize, threshold);
    }

    @Override
    CountersQueue.Counter incrementCounter(int inc) throws IOException {
      return HdfsVariables.incrementQuotaUpdateIdCounter(inc);
    }
  }

  private class CacheDirectiveIDGen extends IDsGenerator{
    CacheDirectiveIDGen(int batchSize, float threshold) {
      super(batchSize, threshold);
    }

    @Override
    CountersQueue.Counter incrementCounter(int inc) throws IOException {
      return HdfsVariables.incrementCacheDirectiveIdCounter(inc);
    }
  }
  
  private static IDsGeneratorFactory factory;
  private IDsGeneratorFactory(){
  }

  public static IDsGeneratorFactory getInstance(){
    if(factory == null){
      factory = new IDsGeneratorFactory();
    }
    return factory;
  }

  private List<IDsGenerator> iDsGenerators = Lists.newArrayList();

  Boolean isConfigured = false;
  void setConfiguration(int inodeIdsBatchSize, int blockIdsBatchSize,
      int quotaUpdateIdsBatchSize, int cacheDirectiveIdsBatchSize, float inodeIdsThreshold,
      float blockIdsThreshold, float quotaUpdateIdsThreshold, float cacheDirectiveIdsThreshold) {

    synchronized (isConfigured) {
      if (isConfigured) {
        LogFactory.getLog(this.getClass())
            .error("Called setConfiguration more than once.");
        return;
      }
      isConfigured = true;
    }

    iDsGenerators.add(new INodeIDGen(inodeIdsBatchSize, inodeIdsThreshold));
    iDsGenerators.add(new BlockIDGen(blockIdsBatchSize, blockIdsThreshold));
    iDsGenerators.add(new QuotaUpdateIDGen(quotaUpdateIdsBatchSize,
        quotaUpdateIdsThreshold));
    iDsGenerators.add(new CacheDirectiveIDGen(cacheDirectiveIdsBatchSize,
        cacheDirectiveIdsThreshold));
  }

  public int getUniqueINodeID(){
    int id = (int)iDsGenerators.get(0).getUniqueID();
    LOG.debug("Unique INode generated. id="+id);
    return id;
  }

  public long getUniqueBlockID(){
    return iDsGenerators.get(1).getUniqueID();
  }

  public int getUniqueQuotaUpdateID(){
    return (int)iDsGenerators.get(2).getUniqueID();
  }

  public long getUniqueCacheDirectiveID() {
    long id = iDsGenerators.get(3).getUniqueID();
    if (id == 0) {
      id = iDsGenerators.get(3).getUniqueID();
    }
    return id;
  }

  void getNewIDs() throws IOException {
    for(IDsGenerator iDsGenerator : iDsGenerators){
      if(iDsGenerator.getMoreIdsIfNeeded()) {
        LOG.debug("get more ids for [" + iDsGenerator.getClass().getSimpleName
            () + "] " + iDsGenerator.getCQ());
      }
    }
  }
}
