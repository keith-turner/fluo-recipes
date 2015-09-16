/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.recipes.accumulo.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.fluo.api.client.Transaction;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.transaction.LogEntry;
import io.fluo.recipes.transaction.RecordingTransaction;
import io.fluo.recipes.transaction.RecordingTransactionBase;
import io.fluo.recipes.transaction.TxLog;
import org.apache.accumulo.core.data.Mutation;

public class AccumuloReplicator extends AccumuloExporter<Bytes, TxLog> {

  @Override
  protected List<Mutation> convert(Bytes key, long seq, TxLog txLog) {
    Map<Bytes, Mutation> mutationMap = new HashMap<>();
    for (LogEntry le : txLog.getLogEntries()) {
      LogEntry.Operation op = le.getOp();
      if (op.equals(LogEntry.Operation.DELETE) || op.equals(LogEntry.Operation.SET)) {
        Mutation m = mutationMap.computeIfAbsent(le.getRow(), k -> new Mutation(k.toArray()));
        if (op.equals(LogEntry.Operation.DELETE)) {
          m.putDelete(le.getColumn().getFamily().toArray(),
              le.getColumn().getQualifier().toArray(), seq);
        } else {
          m.put(le.getColumn().getFamily().toArray(), le.getColumn().getQualifier().toArray(), seq,
              le.getValue().toArray());
        }
      }
    }
    return new ArrayList<>(mutationMap.values());
  }

  private static Predicate<LogEntry> getFilter() {
    return le -> le.getOp().equals(LogEntry.Operation.DELETE)
        || le.getOp().equals(LogEntry.Operation.SET);
  }

  public static RecordingTransactionBase wrap(TransactionBase txb) {
    return RecordingTransactionBase.wrap(txb, getFilter());
  }

  public static RecordingTransaction wrap(Transaction tx) {
    return RecordingTransaction.wrap(tx, getFilter());
  }
}
