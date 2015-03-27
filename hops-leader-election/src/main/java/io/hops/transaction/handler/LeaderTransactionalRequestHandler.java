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
package io.hops.transaction.handler;

import io.hops.transaction.TransactionInfo;
import io.hops.transaction.lock.LeaderElectionTransactionalLockAcquirer;
import io.hops.transaction.lock.TransactionLockAcquirer;

import java.io.IOException;

public abstract class LeaderTransactionalRequestHandler
    extends TransactionalRequestHandler {

  public LeaderTransactionalRequestHandler(LeaderOperationType opType) {
    super(opType);
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new LeaderElectionTransactionalLockAcquirer();
  }

  @Override
  protected Object execute(final Object namesystem) throws IOException {

    return super.execute(new TransactionInfo() {
      @Override
      public String getContextName(OperationType opType) {
        return opType.toString();
      }

      @Override
      public void performPostTransactionAction() throws IOException {

      }
    });
  }

  @Override
  public void preTransactionSetup() throws IOException {

  }

  @Override
  protected final boolean shouldAbort(Exception e) {
    return true;
  }
}
