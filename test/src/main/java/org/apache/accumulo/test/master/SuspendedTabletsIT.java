/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.master;

import com.google.common.net.HostAndPort;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class SuspendedTabletsIT extends ConfigurableMacBase {
  public static final int TSERVERS = 5;
  public static final long SUSPEND_DURATION = MILLISECONDS.convert(2, MINUTES);
  public static final int SPLITS = 100;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TABLE_SUSPEND_DURATION, SUSPEND_DURATION + "ms");
    cfg.setNumTservers(TSERVERS);
  }

  private static class DistributionState {
    public final Set<HostAndPort> haveTablets = new HashSet<>();
    public final Set<HostAndPort> haveSuspends = new HashSet<>();
    public int hostedCount = 0;
    public int assignedCount = 0;
    public int suspendedCount = 0;
    public int unassignedTablets = 0;
  }

  DistributionState getDistribution(ClientContext ctx, String tableName) throws Exception {
    Map<String,String> idMap = ctx.getConnector().tableOperations().tableIdMap();
    String tableId = Objects.requireNonNull(idMap.get(tableName));
    while (true) {
      DistributionState result = new DistributionState();
      try (MetaDataTableScanner scanner = new MetaDataTableScanner(ctx, new Range())) {
        while (scanner.hasNext()) {
          TabletLocationState tls = scanner.next();
          if (!tls.extent.getTableId().equals(tableId)) {
            continue;
          }
          if (tls.suspend != null) {
            result.haveSuspends.add(tls.suspend.server);
            ++result.suspendedCount;
          } else if (tls.current != null) {
            result.haveTablets.add(tls.current.getLocation());
            ++result.hostedCount;
          } else if (tls.future != null) {
            ++result.assignedCount;
          } else {
            result.unassignedTablets += 1;
          }
        }
        return result;
      } catch (Exception ex) {}
    }
  }

  @Test
  public void reassignmentTakesAWhile() throws Exception {
    String tableName = getUniqueNames(1)[0];

    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    Instance instance = new ZooKeeperInstance(getCluster().getClientConfig());
    ClientContext ctx = new ClientContext(instance, creds, getCluster().getClientConfig());

    Connector conn = ctx.getConnector();
    conn.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SUSPEND_DURATION.getKey(), "0s");

    // Create a table with a bunch of splits
    conn.tableOperations().create(tableName);
    SortedSet<Text> splitPoints = new TreeSet<>();
    for (int i = 1; i < SPLITS; ++i) {
      splitPoints.add(new Text("" + i));
    }
    conn.tableOperations().addSplits(tableName, splitPoints);

    // Wait for all of the tablets for our table to be hosted
    DistributionState ds;
    for (ds = getDistribution(ctx, tableName); ds.hostedCount != SPLITS || ds.haveTablets.size() <= 1; ds = getDistribution(ctx, tableName)) {
      Thread.sleep(1000);
    }

    // Pray all of our tablet servers get at least 1 tablet.
    int hostsCount = ds.haveTablets.size();

    // Kill enough tablet servers to guarantee that one of our table's tablets will be on the dead server.
    long killTime = System.nanoTime();
    Iterator<ProcessReference> prIt = getCluster().getProcesses().get(ServerType.TABLET_SERVER).iterator();
    for (int i = hostsCount; i <= TSERVERS; ++i) {
      prIt.next().getProcess().destroyForcibly().waitFor();
    }

    // Eventually some tablets will be suspended.
    for (ds = getDistribution(ctx, tableName); ds.suspendedCount == 0; ds = getDistribution(ctx, tableName)) {
      Thread.sleep(1000);
    }
    // By this point, all tablets should be either hosted or suspended. All suspended tablets should
    // "belong" to one of the dead tablet servers.
    Assert.assertTrue(1 <= ds.haveSuspends.size());
    Assert.assertEquals(SPLITS, ds.hostedCount + ds.suspendedCount + ds.unassignedTablets);

    // Wait for all of our tablets to be hosted again.
    for (ds = getDistribution(ctx, tableName); ds.hostedCount != SPLITS; ds = getDistribution(ctx, tableName)) {
      Thread.sleep(1000);
    }

    long recoverTime = System.nanoTime();
    Assert.assertTrue(recoverTime - killTime >= NANOSECONDS.convert(SUSPEND_DURATION, MILLISECONDS));
    Assert.assertTrue(recoverTime - killTime <= NANOSECONDS.convert(SUSPEND_DURATION, MILLISECONDS) + NANOSECONDS.convert(2, MINUTES));
  }
}
