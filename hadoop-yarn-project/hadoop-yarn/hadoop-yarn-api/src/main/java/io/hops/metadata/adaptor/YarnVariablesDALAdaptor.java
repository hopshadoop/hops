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
package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.entity.YarnVariables;

public class YarnVariablesDALAdaptor
    extends DalAdaptor<YarnVariables, YarnVariables>
    implements YarnVariablesDataAccess<YarnVariables> {

  private final YarnVariablesDataAccess<YarnVariables> dataAccess;

  public YarnVariablesDALAdaptor(
      YarnVariablesDataAccess<YarnVariables> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public YarnVariables convertHDFStoDAL(YarnVariables hdfsClass)
      throws StorageException {
    return hdfsClass;
  }

  @Override
  public YarnVariables convertDALtoHDFS(YarnVariables dalClass)
      throws StorageException {
    return dalClass;
  }

  @Override
  public YarnVariables findById(int id) throws StorageException {
    YarnVariables found = dataAccess.findById(id);
    return found;
  }

  @Override
  public void add(YarnVariables toAdd) throws StorageException {
    dataAccess.add(toAdd);
  }
}
