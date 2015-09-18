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

package io.fluo.recipes.accumulo.export;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Transaction;
import io.fluo.api.data.Bytes;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransaction;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.transaction.RecordingTransaction;
import io.fluo.recipes.transaction.TxLog;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloReplicatorIT extends AccumuloITBase {

  private String et;
  public static final String QUEUE_ID = "aeqt";
  private TypeLayer tl = new TypeLayer(new StringEncoder());


  @Override
  public void setupExporter() throws Exception {
    ExportQueue.configure(props, new ExportQueue.Options(QUEUE_ID, AccumuloReplicator.class,
        Bytes.class, TxLog.class, 5));

    // create and configure export table
    et = "export" + tableCounter.getAndIncrement();
    cluster.getConnector("root", "secret").tableOperations().create(et);
    AccumuloReplicator.setExportTableInfo(props.getAppConfiguration(), QUEUE_ID, new TableInfo(
        cluster.getInstanceName(), cluster.getZooKeepers(), "root", "secret", et));

  }

  @Test
  public void testAccumuloReplicator() throws Exception {

    ExportQueue<Bytes, TxLog> eq = ExportQueue.getInstance(QUEUE_ID, props.getAppConfiguration());

    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {

      Map<String, String> expected = new HashMap<>();

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = AccumuloReplicator.wrap(tx);
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k1", "v1");
        write(ttx, expected, "k2", "v2");
        write(ttx, expected, "k3", "v3");
        eq.add(tx, Bytes.of("q1"), rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = AccumuloReplicator.wrap(tx);
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k1", "v4");
        delete(ttx, expected, "k3");
        write(ttx, expected, "k2", "v5");
        write(ttx, expected, "k4", "v6");
        eq.add(tx, Bytes.of("q1"), rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());

      try (Transaction tx = fc.newTransaction()) {
        RecordingTransaction rtx = AccumuloReplicator.wrap(tx);
        TypedTransaction ttx = tl.wrap(rtx);
        write(ttx, expected, "k2", "v7");
        write(ttx, expected, "k3", "v8");
        delete(ttx, expected, "k1");
        delete(ttx, expected, "k4");
        eq.add(tx, Bytes.of("q1"), rtx.getTxLog());
        tx.commit();
      }

      miniFluo.waitForObservers();
      Assert.assertEquals(expected, getExports());
    }
  }

  private void write(TypedTransaction ttx, Map<String, String> expected, String key, String value) {
    ttx.mutate().row(key).fam("fam").qual("qual").set(value);
    expected.put(key, value);
  }

  private void delete(TypedTransaction ttx, Map<String, String> expected, String key) {
    ttx.mutate().row(key).fam("fam").qual("qual").delete();
    expected.remove(key);
  }

  private Map<String, String> getExports() throws Exception {
    Scanner scanner =
        cluster.getConnector("root", "secret").createScanner(et, Authorizations.EMPTY);
    Map<String, String> ret = new HashMap<>();

    for (Entry<Key, Value> entry : scanner) {
      String k = entry.getKey().getRowData().toString();
      Assert.assertFalse(ret.containsKey(k));
      ret.put(k, entry.getValue().toString());
    }
    return ret;
  }
}
