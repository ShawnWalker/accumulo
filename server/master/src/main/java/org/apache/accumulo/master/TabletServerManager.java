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
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.Daemon;
import static org.apache.accumulo.master.Master.ONE_SECOND;
import static org.apache.accumulo.master.Master.log;
import org.apache.accumulo.master.event.MasterEventBus;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class TabletServerManager {
  final private static long DEFAULT_WAIT_FOR_WATCHER = MILLISECONDS.convert(10, SECONDS);

  private volatile SortedMap<TServerInstance,TabletServerStatus> tserverStatus = Collections.unmodifiableSortedMap(new TreeMap<TServerInstance,TabletServerStatus>());
  final Map<TServerInstance,AtomicInteger> badServers = Collections.synchronizedMap(new DefaultMap<TServerInstance,AtomicInteger>(new AtomicInteger()));
  final Set<TServerInstance> serversToShutdown = Collections.synchronizedSet(new HashSet<TServerInstance>());

  private final MasterEventBus eventBus;
  private final LiveTServerSet tserverSet;
  private final MasterStateMachine stateMachine;
  
  TabletServerManager(ClientContext context, MasterStateMachine stateMachine, MasterEventBus eventBus) {
    this.eventBus = eventBus;
    this.tserverSet = new LiveTServerSet(context, null);
    this.stateMachine = stateMachine;
  }
  
  public LiveTServerSet.TServerConnection getConnection(TServerInstance server) {
    return tserverSet.getConnection(server);
  }
  
  public SortedMap<TServerInstance, TabletServerStatus> getStatusSnapshot() {
    return Collections.unmodifiableSortedMap(tserverStatus);
  }
  
  private SortedMap<TServerInstance,TabletServerStatus> gatherTableInformation() {
    long start = System.currentTimeMillis();
    int threads = Math.max(getConfiguration().getCount(Property.MASTER_STATUS_THREAD_POOL_SIZE), 1);
    ExecutorService tp = Executors.newFixedThreadPool(threads);
    final SortedMap<TServerInstance,TabletServerStatus> result = new TreeMap<>();
    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
    for (TServerInstance serverInstance : currentServers) {
      final TServerInstance server = serverInstance;
      tp.submit(new Runnable() {
        @Override
        public void run() {
          try {
            Thread t = Thread.currentThread();
            String oldName = t.getName();
            try {
              t.setName("Getting status from " + server);
              LiveTServerSet.TServerConnection connection = tserverSet.getConnection(server);
              if (connection == null)
                throw new IOException("No connection to " + server);
              TabletServerStatus status = connection.getTableMap(false);
              result.put(server, status);
            } finally {
              t.setName(oldName);
            }
          } catch (Exception ex) {
            log.error("unable to get tablet server status " + server + " " + ex.toString());
            log.debug("unable to get tablet server status " + server, ex);
            if (badServers.get(server).incrementAndGet() > MAX_BAD_STATUS_COUNT) {
              log.warn("attempting to stop " + server);
              try {
                LiveTServerSet.TServerConnection connection = tserverSet.getConnection(server);
                if (connection != null) {
                  connection.halt(masterLock);
                }
              } catch (TTransportException e) {
                // ignore: it's probably down
              } catch (Exception e) {
                log.info("error talking to troublesome tablet server ", e);
              }
              badServers.remove(server);
            }
          }
        }
      });
    }
    tp.shutdown();
    try {
      tp.awaitTermination(getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT) * 2, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.debug("Interrupted while fetching status");
    }
    synchronized (badServers) {
      badServers.keySet().retainAll(currentServers);
      badServers.keySet().removeAll(result.keySet());
    }
    log.debug(String.format("Finished gathering information from %d servers in %.2f seconds", result.size(), (System.currentTimeMillis() - start) / 1000.));
    return result;
  }  
  
  private class StatusThread extends Daemon {

    private boolean goodStats() {
      int start;
      switch (stateMachine.getMasterState()) {
        case UNLOAD_METADATA_TABLETS:
          start = 1;
          break;
        case UNLOAD_ROOT_TABLET:
          start = 2;
          break;
        default:
          start = 0;
      }
      for (int i = start; i < watchers.size(); i++) {
        TabletGroupWatcher watcher = watchers.get(i);
        if (watcher.stats.getLastMasterState() != stateMachine.getMasterState()) {
          log.debug(watcher.getName() + ": " + watcher.stats.getLastMasterState() + " != " + stateMachine.getMasterState());
          return false;
        }
      }
      return true;
    }

    @Override
    public void run() {
      setName("Status Thread");
      MasterEventBus.Listener eventListener = eventBus.getListener();
      while (stateMachine.stillMaster()) {
        long wait = DEFAULT_WAIT_FOR_WATCHER;
        try {
          switch (getMasterGoalState()) {
            case NORMAL:
              stateMachine.setMasterState(MasterState.NORMAL);
              break;
            case SAFE_MODE:
              if (stateMachine.getMasterState() == MasterState.NORMAL) {
                stateMachine.setMasterState(MasterState.SAFE_MODE);
              }
              if (stateMachine.getMasterState() == MasterState.HAVE_LOCK) {
                stateMachine.setMasterState(MasterState.SAFE_MODE);
              }
              break;
            case CLEAN_STOP:
              switch (stateMachine.getMasterState()) {
                case NORMAL:
                  stateMachine.setMasterState(MasterState.SAFE_MODE);
                  break;
                case SAFE_MODE: {
                  int count = nonMetaDataTabletsAssignedOrHosted();
                  log.debug(String.format("There are %d non-metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats())
                    stateMachine.setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
                }
                  break;
                case UNLOAD_METADATA_TABLETS: {
                  int count = assignedOrHosted(MetadataTable.ID);
                  log.debug(String.format("There are %d metadata tablets assigned or hosted", count));
                  if (count == 0 && goodStats())
                    stateMachine.setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                }
                  break;
                case UNLOAD_ROOT_TABLET: {
                  int count = assignedOrHosted(MetadataTable.ID);
                  if (count > 0 && goodStats()) {
                    log.debug(String.format("%d metadata tablets online", count));
                    stateMachine.setMasterState(MasterState.UNLOAD_ROOT_TABLET);
                  }
                  int root_count = assignedOrHosted(RootTable.ID);
                  if (root_count > 0 && goodStats())
                    log.debug("The root tablet is still assigned or hosted");
                  if (count + root_count == 0 && goodStats()) {
                    Set<TServerInstance> currentServers = tserverSet.getCurrentServers();
                    log.debug("stopping " + currentServers.size() + " tablet servers");
                    for (TServerInstance server : currentServers) {
                      try {
                        serversToShutdown.add(server);
                        tserverSet.getConnection(server).fastHalt(masterLock);
                      } catch (TException e) {
                        // its probably down, and we don't care
                      } finally {
                        tserverSet.remove(server);
                      }
                    }
                    if (currentServers.size() == 0)
                      stateMachine.setMasterState(MasterState.STOP);
                  }
                }
                  break;
                default:
                  break;
              }
          }
        } catch (Throwable t) {
          log.error("Error occurred reading / switching master goal state. Will continue with attempt to update status", t);
        }

        try {
          wait = updateStatus();
          eventListener.waitForEvents(wait);
        } catch (Throwable t) {
          log.error("Error balancing tablets, will wait for " + WAIT_BETWEEN_ERRORS / ONE_SECOND + " (seconds) and then retry", t);
          sleepUninterruptibly(WAIT_BETWEEN_ERRORS, TimeUnit.MILLISECONDS);
        }
      }
    }
  
    private long updateStatus() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
      tserverStatus = Collections.synchronizedSortedMap(gatherTableInformation());
      checkForHeldServer(tserverStatus);

      if (!badServers.isEmpty()) {
        log.debug("not balancing because the balance information is out-of-date " + badServers.keySet());
      } else if (notHosted() > 0) {
        log.debug("not balancing because there are unhosted tablets: " + notHosted());
      } else if (getMasterGoalState() == MasterGoalState.CLEAN_STOP) {
        log.debug("not balancing because the master is attempting to stop cleanly");
      } else if (!serversToShutdown.isEmpty()) {
        log.debug("not balancing while shutting down servers " + serversToShutdown);
      } else {
        return balanceTablets();
      }
      return DEFAULT_WAIT_FOR_WATCHER;
    }

    private void checkForHeldServer(SortedMap<TServerInstance,TabletServerStatus> tserverStatus) {
      TServerInstance instance = null;
      int crazyHoldTime = 0;
      int someHoldTime = 0;
      final long maxWait = getConfiguration().getTimeInMillis(Property.TSERV_HOLD_TIME_SUICIDE);
      for (Map.Entry<TServerInstance,TabletServerStatus> entry : tserverStatus.entrySet()) {
        if (entry.getValue().getHoldTime() > 0) {
          someHoldTime++;
          if (entry.getValue().getHoldTime() > maxWait) {
            instance = entry.getKey();
            crazyHoldTime++;
          }
        }
      }
      if (crazyHoldTime == 1 && someHoldTime == 1 && tserverStatus.size() > 1) {
        log.warn("Tablet server " + instance + " exceeded maximum hold time: attempting to kill it");
        try {
          LiveTServerSet.TServerConnection connection = tserverSet.getConnection(instance);
          if (connection != null)
            connection.fastHalt(masterLock);
        } catch (TException e) {
          log.error("{}", e.getMessage(), e);
        }
        tserverSet.remove(instance);
      }
    }
  }

}
