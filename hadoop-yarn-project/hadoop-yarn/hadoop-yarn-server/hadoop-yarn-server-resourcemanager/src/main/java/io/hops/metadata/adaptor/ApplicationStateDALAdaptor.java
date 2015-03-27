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
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;

import java.util.Collection;
import java.util.List;

public class ApplicationStateDALAdaptor
    extends DalAdaptor<ApplicationState, ApplicationState>
    implements ApplicationStateDataAccess<ApplicationState> {

  private final ApplicationStateDataAccess<ApplicationState> dataAccess;

  public ApplicationStateDALAdaptor(
      ApplicationStateDataAccess<ApplicationState> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public ApplicationState convertHDFStoDAL(ApplicationState hdfsClass)
      throws StorageException {
    return hdfsClass;
  }

  @Override
  public ApplicationState convertDALtoHDFS(ApplicationState dalClass)
      throws StorageException {
    return dalClass;
  }

  @Override
  public ApplicationState findByApplicationId(String applicationid)
      throws StorageException {
    return dataAccess.findByApplicationId(applicationid);
  }

  @Override
  public void addAll(Collection<ApplicationState> toAdd)
      throws StorageException {
    dataAccess.addAll(toAdd);
  }
  
  @Override
  public void removeAll(Collection<ApplicationState> toRemove)
      throws StorageException {
    dataAccess.removeAll(toRemove);
  }
  
  @Override
  public void add(ApplicationState toAdd) throws StorageException {
    dataAccess.add(toAdd);
  }
  
  @Override
  public void remove(ApplicationState toRemove) throws StorageException {
    dataAccess.remove(toRemove);
  }

  @Override
  public List<ApplicationState> getAll() throws StorageException {
    return dataAccess.getAll();
  }
}
