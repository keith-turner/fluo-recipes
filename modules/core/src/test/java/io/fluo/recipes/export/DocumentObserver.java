/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.export;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.recipes.export.ExportQueue;

public class DocumentObserver extends TypedObserver {

  ExportQueue<String, RefUpdates> refExportQueue;

  @Override
  public void init(Context context) throws Exception {
    refExportQueue = new ExportQueueIT.RefExporter().getExportQueue(context.getAppConfiguration());
  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(new Column("content", "new"), NotificationType.STRONG);
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    String newContent = tx.get().row(row).col(col).toString();
    Set<String> newRefs = new HashSet<>(Arrays.asList(newContent.split(" ")));
    Set<String> currentRefs =
        new HashSet<>(Arrays.asList(tx.get().row(row).fam("content").qual("current").toString("")
            .split(" ")));

    Set<String> addedRefs = new HashSet<>(newRefs);
    addedRefs.removeAll(currentRefs);

    Set<String> deletedRefs = new HashSet<>(currentRefs);
    deletedRefs.removeAll(newRefs);

    String key = row.toString().substring(2);
    RefUpdates val = new RefUpdates(addedRefs, deletedRefs);

    refExportQueue.add(tx, key, val);

    tx.mutate().row(row).fam("content").qual("current").set(newContent);
  }
}
