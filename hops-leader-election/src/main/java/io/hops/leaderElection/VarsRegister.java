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
package io.hops.leaderElection;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.entity.StringVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.entity.LeVariables;
import io.hops.transaction.EntityManager;

public class VarsRegister {

  public static long getMaxID(Variable.Finder finder)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    return leParams.getMaxId();
  }

  public static long getTimePeriod(Variable.Finder finder)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    return leParams.getTimePeriod();
  }

  public static boolean isEvict(Variable.Finder finder)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    return leParams.isEvictFlag();
  }

  public static void setMaxID(Variable.Finder finder, long val)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    leParams.setMaxId(val);
    updateVariable(new StringVariable(finder, leParams.toString()));
  }

  public static void setTimePeriod(Variable.Finder finder, long val)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    leParams.setTimePeriod(val);
    updateVariable(new StringVariable(finder, leParams.toString()));
  }

  public static void setEvictFlag(Variable.Finder finder, boolean val)
      throws TransactionContextException, StorageException {
    String params = (String) getVariable(finder).getValue();
    LeVariables leParams = LeVariables.parseString(params);
    leParams.setEvictFlag(val);
    updateVariable(new StringVariable(finder, leParams.toString()));
  }

  private static void updateVariable(Variable var)
      throws TransactionContextException, StorageException {
    EntityManager.update(var);
  }

  private static Variable getVariable(Variable.Finder varType)
      throws TransactionContextException, StorageException {
    return EntityManager.find(varType);
  }

  public static void registerYarnDefaultValues() {
    LeVariables param = new LeVariables(false, 0, 0);
    StringVariable YarnVars =
        new StringVariable(Variable.Finder.YarnLeParams, param.toString());
    Variable.registerUserDefinedDefaultValue(Variable.Finder.YarnLeParams,
        YarnVars.getBytes());
  }
  
  public static void registerHdfsDefaultValues() {
    LeVariables param = new LeVariables(false, 0, 0);
    StringVariable hdfsVars =
        new StringVariable(Variable.Finder.HdfsLeParams, param.toString());
    Variable.registerUserDefinedDefaultValue(Variable.Finder.HdfsLeParams,
        hdfsVars.getBytes());
  }
}
