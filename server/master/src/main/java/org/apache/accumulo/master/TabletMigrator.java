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
package org.apache.accumulo.master;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Daemon;
import static org.apache.accumulo.master.Master.log;
import org.apache.accumulo.master.event.GenericEvent;
import org.apache.accumulo.master.event.MasterEventBus;
import org.apache.accumulo.master.state.TableCounts;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.tables.TableManager;

public class TabletMigrator {
  final private static long TIME_BETWEEN_MIGRATION_CLEANUPS = MILLISECONDS.convert(5 * 60, SECONDS);
  
  private final SortedMap<KeyExtent,TServerInstance> migrations = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,TServerInstance>());
  final private Object balancedNotifier = new Object();
  
  private final ServerConfigurationFactory confFactory;
  private final MasterEventBus eventBus;
  private final MasterStateMachine stateMachine;
  private final TabletServerManager tservers;
  private final ScheduledExecutorService ses;
  private final ClientContext context;
  
  private TabletBalancer tabletBalancer;
  
  TabletMigrator(
          ServerConfigurationFactory confFactory, 
          MasterEventBus eventBus, 
          MasterStateMachine stateMachine, 
          TabletServerManager tservers,
          ScheduledExecutorService ses,
          ClientContext context
          ) 
  {
    this.confFactory = confFactory;
    this.eventBus = eventBus;
    this.stateMachine = stateMachine;
    this.tservers = tservers;
    this.ses = ses;
    this.context = context;
    
    this.tabletBalancer = confFactory.getConfiguration().instantiateClassProperty(Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
    this.tabletBalancer.init(confFactory);
  }

  public void start() {
    ses.scheduleWithFixedDelay(new MigrationCleanupTask(), 0, TIME_BETWEEN_MIGRATION_CLEANUPS, MILLISECONDS);
  }
  
  /** Wait until balancing has been performed, and has decided not to move any tablets. */
  public void waitForBalance(TInfo tinfo) {
    synchronized (balancedNotifier) {
      long eventCounter;
      do {
        eventCounter = eventBus.waitForEvents(0, 0);
        try {
          balancedNotifier.wait();
        } catch (InterruptedException e) {
          log.debug(e.toString(), e);
        }
      } while (displayUnassigned() > 0 || migrations.size() > 0 || eventCounter != eventBus.waitForEvents(0, 0));
    }
  }
  
  /** Returns the handle to the tablet server that the specified tablet is migrating to, or null if that tablet is not undergoing migration. */
  public TServerInstance getMigrationDestination(KeyExtent extent) {
    return migrations.get(extent);
  }
  
  
  private long balanceTablets() {
    List<TabletMigration> migrationsOut = new ArrayList<>();
    long wait = tabletBalancer.balance(tservers.getStatusSnapshot(), getMigrationsSnapshot(), migrationsOut);

    for (TabletMigration m : TabletBalancer.checkMigrationSanity(tservers.getStatusSnapshot().keySet(), migrationsOut)) {
      if (migrations.containsKey(m.tablet)) {
        log.warn("balancer requested migration more than once, skipping " + m);
        continue;
      }
      migrations.put(m.tablet, m.newServer);
      log.debug("migration " + m);
    }
    if (migrationsOut.size() > 0) {
      eventBus.post(new GenericEvent("Migrating %d more tablets, %d total", migrationsOut.size(), migrations.size()));
    } else {
      synchronized (balancedNotifier) {
        balancedNotifier.notifyAll();
      }
    }
    return wait;
  }
  
  public Set<KeyExtent> getMigrationsSnapshot() {
    Set<KeyExtent> migrationKeys = new HashSet<>();
    synchronized (migrations) {
      migrationKeys.addAll(migrations.keySet());
    }
    return Collections.unmodifiableSet(migrationKeys);
  }  
  
  private void clearMigrations(String tableId) {
    synchronized (migrations) {
      Iterator<KeyExtent> iterator = migrations.keySet().iterator();
      while (iterator.hasNext()) {
        KeyExtent extent = iterator.next();
        if (extent.getTableId().equals(tableId)) {
          iterator.remove();
        }
      }
    }
  }
  
  // The number of unassigned tablets that should be assigned: displayed on the monitor page
  int displayUnassigned() {
    int result = 0;
    switch (stateMachine.getMasterState()) {
      case NORMAL:
        // Count offline tablets for online tables
        for (TabletGroupWatcher watcher : watchers) {
          TableManager manager = TableManager.getInstance();
          for (Map.Entry<String,TableCounts> entry : watcher.getStats().entrySet()) {
            String tableId = entry.getKey();
            TableCounts counts = entry.getValue();
            TableState tableState = manager.getTableState(tableId);
            if (tableState != null && tableState.equals(TableState.ONLINE)) {
              result += counts.unassigned() + counts.assignedToDeadServers() + counts.assigned();
            }
          }
        }
        break;
      case SAFE_MODE:
        // Count offline tablets for the metadata table
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(MetadataTable.ID).unassigned();
        }
        break;
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        for (TabletGroupWatcher watcher : watchers) {
          result += watcher.getStats(MetadataTable.ID).unassigned();
        }
        break;
      default:
        break;
    }
    return result;
  }  

  private class MigrationCleanupTask implements Runnable {
    @Override
    public void run() {
      String oldName=Thread.currentThread().getName();
      Thread.currentThread().setName("Migration Cleanup Thread");
      if (stateMachine.stillMaster()) {
        if (!migrations.isEmpty()) {
          try {
            cleanupOfflineMigrations();
            cleanupNonexistentMigrations(context.getConnector());
          } catch (Exception ex) {
            log.error("Error cleaning up migrations", ex);
          }
        }        
      }
      Thread.currentThread().setName(oldName);
    }

    /**
     * If a migrating tablet splits, and the tablet dies before sending the master a message, the migration will refer to a non-existing tablet, so it can never
     * complete. Periodically scan the metadata table and remove any migrating tablets that no longer exist.
     */
    private void cleanupNonexistentMigrations(final Connector connector) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      Set<KeyExtent> found = new HashSet<>();
      for (Map.Entry<Key,Value> entry : scanner) {
        KeyExtent extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        if (migrations.containsKey(extent)) {
          found.add(extent);
        }
      }
      migrations.keySet().retainAll(found);
    }

    /**
     * If migrating a tablet for a table that is offline, the migration can never succeed because no tablet server will load the tablet. check for offline
     * tables and remove their migrations.
     */
    private void cleanupOfflineMigrations() {
      TableManager manager = TableManager.getInstance();
      for (String tableId : Tables.getIdToNameMap(context.getInstance()).keySet()) {
        TableState state = manager.getTableState(tableId);
        if (TableState.OFFLINE == state) {
          clearMigrations(tableId);
        }
      }
    }
  }
  
  
}
