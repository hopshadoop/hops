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
package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;

public class LastUpdatedContentSummary extends QuotaUsage {
  
  public static class Builder extends QuotaUsage.Builder{
    public Builder() {
    }
  
    @Override
    public LastUpdatedContentSummary.Builder fileAndDirectoryCount(long count) {
      super.fileAndDirectoryCount(count);
      return this;
    }
    
    @Override
    public LastUpdatedContentSummary.Builder quota(long quota){
      super.quota(quota);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder spaceConsumed(long spaceConsumed) {
      super.spaceConsumed(spaceConsumed);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder spaceQuota(long spaceQuota) {
      super.spaceQuota(spaceQuota);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder typeConsumed(long typeConsumed[]) {
      super.typeConsumed(typeConsumed);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder typeQuota(StorageType type, long quota) {
      super.typeQuota(type, quota);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder typeConsumed(StorageType type, long consumed) {
      super.typeConsumed(type, consumed);
      return this;
    }
  
    @Override
    public LastUpdatedContentSummary.Builder typeQuota(long typeQuota[]) {
      super.typeQuota(typeQuota);
      return this;
    }
    
    public LastUpdatedContentSummary build(){
      return new LastUpdatedContentSummary(this);
    }
  }
  
  protected LastUpdatedContentSummary(Builder builder){
    super(builder);
  }
  
  public long getFileAndDirCount() {
    return super.getFileAndDirectoryCount();
  }

  public long getSpaceConsumed() {
    return super.getSpaceConsumed();
  }

  public long getNsQuota() {
    return super.getQuota();
  }

  public long getDsQuota() {
    return super.getSpaceQuota();
  }
  
  @Override
  public boolean equals(Object to) {
    return super.equals(to);
  }
  
  @Override
  public int hashCode() {
    return super.hashCode();
  }
}

