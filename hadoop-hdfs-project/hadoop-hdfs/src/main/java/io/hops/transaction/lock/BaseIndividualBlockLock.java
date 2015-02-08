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
package io.hops.transaction.lock;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;

abstract class BaseIndividualBlockLock extends Lock {

  protected final List<BlockInfoContiguous> blocks;

  BaseIndividualBlockLock() {
    this.blocks = new ArrayList<>();
  }

  Collection<BlockInfoContiguous> getBlocks() {
    return blocks;
  }

  @Override
  protected Lock.Type getType() {
    return Lock.Type.Block;
  }
}
