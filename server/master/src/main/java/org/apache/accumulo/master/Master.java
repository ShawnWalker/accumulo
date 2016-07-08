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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.master.thrift.MasterClientService.Processor;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AgeOffStore;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.master.metrics.MasterMetricsFactory;
import org.apache.accumulo.master.recovery.RecoveryManager;
import org.apache.accumulo.master.replication.MasterReplicationCoordinator;
import org.apache.accumulo.master.replication.ReplicationDriver;
import org.apache.accumulo.master.replication.WorkDriver;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.CurrentState;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.ZooStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.replication.ZooKeeperInitialization;
import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenKeyManager;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyDistributor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableObserver;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.accumulo.master.event.GenericEvent;
import org.apache.accumulo.master.event.MasterEventBus;
import org.apache.accumulo.master.event.MasterStateChangeEvent;

/**
 * The Master is responsible for assigning and balancing tablets to tablet servers.
 *
 * The master will also coordinate log recoveries and reports general status.
 */
public class Master extends AccumuloServerContext implements LiveTServerSet.Listener, TableObserver, CurrentState {

  final static Logger log = LoggerFactory.getLogger(Master.class);

  final static int ONE_SECOND = 1000;
  final static long TIME_TO_WAIT_BETWEEN_SCANS = 60 * ONE_SECOND;
  final static long WAIT_BETWEEN_ERRORS = ONE_SECOND;
  final private static int MAX_CLEANUP_WAIT_TIME = ONE_SECOND;
  final private static int TIME_TO_WAIT_BETWEEN_LOCK_CHECKS = ONE_SECOND;
  final static int MAX_TSERVER_WORK_CHUNK = 5000;
  final private static int MAX_BAD_STATUS_COUNT = 3;

  final VolumeManager fs;
  final private String hostname;
  final SecurityOperation security;
  final private Object mergeLock = new Object();
  private ReplicationDriver replicationWorkDriver;
  private WorkDriver replicationWorkAssigner;
  RecoveryManager recoveryManager = null;

  // Delegation Token classes
  private final boolean delegationTokensAvailable;
  private ZooAuthenticationKeyDistributor keyDistributor;
  private AuthenticationTokenKeyManager authenticationTokenKeyManager;

  ZooLock masterLock = null;
  private TServer clientService = null;
  
  private final ScheduledExecutorService periodicTasks;
  private final MasterEventBus eventBus;
  private final MasterStateMachine stateMachine;
  private final TabletMigrator tabletDistributor;
  private final TabletServerManager tserverManager;
  private final TabletAssigner assigner;
  
  Fate<Master> fate;

  final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  public Master(ServerConfigurationFactory config, VolumeManager fs, String hostname) throws IOException {
    super(config);
    this.serverConfig = config;
    this.fs = fs;
    this.hostname = hostname;

    this.periodicTasks=Executors.newScheduledThreadPool(4);
    this.eventBus = new MasterEventBus();
    this.eventBus.register(this);
    this.stateMachine=new MasterStateMachine(eventBus);
    
    AccumuloConfiguration aconf = serverConfig.getConfiguration();

    log.info("Version " + Constants.VERSION);
    log.info("Instance " + getInstance().getInstanceID());
    ThriftTransportPool.getInstance().setIdleTime(aconf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));

    try {
      AccumuloVFSClassLoader.getContextManager().setContextConfig(new ContextManager.DefaultContextsConfig(new Iterable<Entry<String,String>>() {
        @Override
        public Iterator<Entry<String,String>> iterator() {
          return getConfiguration().iterator();
        }
      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.security = AuditedSecurityOperation.getInstance(this);

    // Create the secret manager (can generate and verify delegation tokens)
    final long tokenLifetime = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME);
    setSecretManager(new AuthenticationTokenSecretManager(getInstance(), tokenLifetime));

    authenticationTokenKeyManager = null;
    keyDistributor = null;
    if (getConfiguration().getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      // SASL is enabled, create the key distributor (ZooKeeper) and manager (generates/rolls secret keys)
      log.info("SASL is enabled, creating delegation token key manager and distributor");
      final long tokenUpdateInterval = aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL);
      keyDistributor = new ZooAuthenticationKeyDistributor(ZooReaderWriter.getInstance(), ZooUtil.getRoot(getInstance()) + Constants.ZDELEGATION_TOKEN_KEYS);
      authenticationTokenKeyManager = new AuthenticationTokenKeyManager(getSecretManager(), keyDistributor, tokenUpdateInterval, tokenLifetime);
      delegationTokensAvailable = true;
    } else {
      log.info("SASL is not enabled, delegation tokens will not be available");
      delegationTokensAvailable = false;
    }
    this.tabletDistributor=new TabletMigrator(this.serverConfig, this.nextEvent);
    this.tserverManager=new TabletServerManager(this, this.nextEvent);
  }
  
  @Subscribe
  private void onMasterChange(MasterStateChangeEvent stateChange) {
    if (stateChange.newState == MasterState.STOP) {
      // Give the server a little time before shutdown so the client
      // thread requesting the stop can return
      this.periodicTasks.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          // This frees the main thread and will cause the master to exit
          clientService.stop();
          Master.this.eventBus.post(new GenericEvent("stopped event loop"));
        }
      }, 100l, 1000l, TimeUnit.MILLISECONDS);
    }

    if (stateChange.oldState != stateChange.newState && (stateChange.newState == MasterState.HAVE_LOCK)) {
      upgradeZookeeper();
    }

    if (stateChange.oldState != stateChange.newState && (stateChange.newState == MasterState.NORMAL)) {
      upgradeMetadata();
    }
  }
  
  private void moveRootTabletToRootTable(IZooReaderWriter zoo) throws Exception {
    String dirZPath = ZooUtil.getRoot(getInstance()) + RootTable.ZROOT_TABLET_PATH;

    if (!zoo.exists(dirZPath)) {
      Path oldPath = fs.getFullPath(FileType.TABLE, "/" + MetadataTable.ID + "/root_tablet");
      if (fs.exists(oldPath)) {
        String newPath = fs.choose(Optional.of(RootTable.ID), ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + RootTable.ID;
        fs.mkdirs(new Path(newPath));
        if (!fs.rename(oldPath, new Path(newPath))) {
          throw new IOException("Failed to move root tablet from " + oldPath + " to " + newPath);
        }

        log.info("Upgrade renamed " + oldPath + " to " + newPath);
      }

      Path location = null;

      for (String basePath : ServerConstants.getTablesDirs()) {
        Path path = new Path(basePath + "/" + RootTable.ID + RootTable.ROOT_TABLET_LOCATION);
        if (fs.exists(path)) {
          if (location != null) {
            throw new IllegalStateException("Root table at multiple locations " + location + " " + path);
          }

          location = path;
        }
      }

      if (location == null)
        throw new IllegalStateException("Failed to find root tablet");

      log.info("Upgrade setting root table location in zookeeper " + location);
      zoo.putPersistentData(dirZPath, location.toString().getBytes(), NodeExistsPolicy.FAIL);
    }
  }

  private boolean haveUpgradedZooKeeper = false;

  private void upgradeZookeeper() {
    // 1.5.1 and 1.6.0 both do some state checking after obtaining the zoolock for the
    // monitor and before starting up. It's not tied to the data version at all (and would
    // introduce unnecessary complexity to try to make the master do it), but be aware
    // that the master is not the only thing that may alter zookeeper before starting.

    final int accumuloPersistentVersion = Accumulo.getAccumuloPersistentVersion(fs);
    if (Accumulo.persistentVersionNeedsUpgrade(accumuloPersistentVersion)) {
      // This Master hasn't started Fate yet, so any outstanding transactions must be from before the upgrade.
      // Change to Guava's Verify once we use Guava 17.
      if (null != fate) {
        throw new IllegalStateException(
            "Access to Fate should not have been initialized prior to the Master transitioning to active. Please save all logs and file a bug.");
      }
      Accumulo.abortIfFateTransactions();
      try {
        log.info("Upgrading zookeeper");

        IZooReaderWriter zoo = ZooReaderWriter.getInstance();
        final String zooRoot = ZooUtil.getRoot(getInstance());

        log.debug("Handling updates for version " + accumuloPersistentVersion);

        log.debug("Cleaning out remnants of logger role.");
        zoo.recursiveDelete(zooRoot + "/loggers", NodeMissingPolicy.SKIP);
        zoo.recursiveDelete(zooRoot + "/dead/loggers", NodeMissingPolicy.SKIP);

        final byte[] zero = new byte[] {'0'};
        log.debug("Initializing recovery area.");
        zoo.putPersistentData(zooRoot + Constants.ZRECOVERY, zero, NodeExistsPolicy.SKIP);

        for (String id : zoo.getChildren(zooRoot + Constants.ZTABLES)) {
          log.debug("Prepping table " + id + " for compaction cancellations.");
          zoo.putPersistentData(zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_COMPACT_CANCEL_ID, zero, NodeExistsPolicy.SKIP);
        }

        String zpath = zooRoot + Constants.ZCONFIG + "/tserver.wal.sync.method";
        // is the entire instance set to use flushing vs sync?
        boolean flushDefault = false;
        try {
          byte data[] = zoo.getData(zpath, null);
          if (new String(data, UTF_8).endsWith("flush")) {
            flushDefault = true;
          }
        } catch (KeeperException.NoNodeException ex) {
          // skip
        }
        for (String id : zoo.getChildren(zooRoot + Constants.ZTABLES)) {
          log.debug("Converting table " + id + " WALog setting to Durability");
          try {
            String path = zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_CONF + "/table.walog.enabled";
            byte[] data = zoo.getData(path, null);
            boolean useWAL = Boolean.parseBoolean(new String(data, UTF_8));
            zoo.recursiveDelete(path, NodeMissingPolicy.FAIL);
            path = zooRoot + Constants.ZTABLES + "/" + id + Constants.ZTABLE_CONF + "/" + Property.TABLE_DURABILITY.getKey();
            if (useWAL) {
              if (flushDefault) {
                zoo.putPersistentData(path, "flush".getBytes(), NodeExistsPolicy.SKIP);
              } else {
                zoo.putPersistentData(path, "sync".getBytes(), NodeExistsPolicy.SKIP);
              }
            } else {
              zoo.putPersistentData(path, "none".getBytes(), NodeExistsPolicy.SKIP);
            }
          } catch (KeeperException.NoNodeException ex) {
            // skip it
          }
        }

        // create initial namespaces
        String namespaces = ZooUtil.getRoot(getInstance()) + Constants.ZNAMESPACES;
        zoo.putPersistentData(namespaces, new byte[0], NodeExistsPolicy.SKIP);
        for (Pair<String,String> namespace : Iterables.concat(
            Collections.singleton(new Pair<>(Namespaces.ACCUMULO_NAMESPACE, Namespaces.ACCUMULO_NAMESPACE_ID)),
            Collections.singleton(new Pair<>(Namespaces.DEFAULT_NAMESPACE, Namespaces.DEFAULT_NAMESPACE_ID)))) {
          String ns = namespace.getFirst();
          String id = namespace.getSecond();
          log.debug("Upgrade creating namespace \"" + ns + "\" (ID: " + id + ")");
          if (!Namespaces.exists(getInstance(), id))
            TableManager.prepareNewNamespaceState(getInstance().getInstanceID(), id, ns, NodeExistsPolicy.SKIP);
        }

        // create replication table in zk
        log.debug("Upgrade creating table " + ReplicationTable.NAME + " (ID: " + ReplicationTable.ID + ")");
        TableManager.prepareNewTableState(getInstance().getInstanceID(), ReplicationTable.ID, Namespaces.ACCUMULO_NAMESPACE_ID, ReplicationTable.NAME,
            TableState.OFFLINE, NodeExistsPolicy.SKIP);

        // create root table
        log.debug("Upgrade creating table " + RootTable.NAME + " (ID: " + RootTable.ID + ")");
        TableManager.prepareNewTableState(getInstance().getInstanceID(), RootTable.ID, Namespaces.ACCUMULO_NAMESPACE_ID, RootTable.NAME, TableState.ONLINE,
            NodeExistsPolicy.SKIP);
        Initialize.initSystemTablesConfig();
        // ensure root user can flush root table
        security.grantTablePermission(rpcCreds(), security.getRootUsername(), RootTable.ID, TablePermission.ALTER_TABLE, Namespaces.ACCUMULO_NAMESPACE_ID);

        // put existing tables in the correct namespaces
        String tables = ZooUtil.getRoot(getInstance()) + Constants.ZTABLES;
        for (String tableId : zoo.getChildren(tables)) {
          String targetNamespace = (MetadataTable.ID.equals(tableId) || RootTable.ID.equals(tableId)) ? Namespaces.ACCUMULO_NAMESPACE_ID
              : Namespaces.DEFAULT_NAMESPACE_ID;
          log.debug("Upgrade moving table " + new String(zoo.getData(tables + "/" + tableId + Constants.ZTABLE_NAME, null), UTF_8) + " (ID: " + tableId
              + ") into namespace with ID " + targetNamespace);
          zoo.putPersistentData(tables + "/" + tableId + Constants.ZTABLE_NAMESPACE, targetNamespace.getBytes(UTF_8), NodeExistsPolicy.SKIP);
        }

        // rename metadata table
        log.debug("Upgrade renaming table " + MetadataTable.OLD_NAME + " (ID: " + MetadataTable.ID + ") to " + MetadataTable.NAME);
        zoo.putPersistentData(tables + "/" + MetadataTable.ID + Constants.ZTABLE_NAME, Tables.qualify(MetadataTable.NAME).getSecond().getBytes(UTF_8),
            NodeExistsPolicy.OVERWRITE);

        moveRootTabletToRootTable(zoo);

        // add system namespace permissions to existing users
        ZKPermHandler perm = new ZKPermHandler();
        perm.initialize(getInstance().getInstanceID(), true);
        String users = ZooUtil.getRoot(getInstance()) + "/users";
        for (String user : zoo.getChildren(users)) {
          zoo.putPersistentData(users + "/" + user + "/Namespaces", new byte[0], NodeExistsPolicy.SKIP);
          perm.grantNamespacePermission(user, Namespaces.ACCUMULO_NAMESPACE_ID, NamespacePermission.READ);
        }
        perm.grantNamespacePermission("root", Namespaces.ACCUMULO_NAMESPACE_ID, NamespacePermission.ALTER_TABLE);

        // add the currlog location for root tablet current logs
        zoo.putPersistentData(ZooUtil.getRoot(getInstance()) + RootTable.ZROOT_TABLET_CURRENT_LOGS, new byte[0], NodeExistsPolicy.SKIP);
        haveUpgradedZooKeeper = true;
      } catch (Exception ex) {
        // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
        log.error("FATAL: Error performing upgrade", ex);
        System.exit(1);
      }
    }
  }

  private final AtomicBoolean upgradeMetadataRunning = new AtomicBoolean(false);
  private final CountDownLatch waitForMetadataUpgrade = new CountDownLatch(1);

  private final ServerConfigurationFactory serverConfig;

  private MasterClientServiceHandler clientHandler;

  private void upgradeMetadata() {
    // we make sure we're only doing the rest of this method once so that we can signal to other threads that an upgrade wasn't needed.
    if (upgradeMetadataRunning.compareAndSet(false, true)) {
      final int accumuloPersistentVersion = Accumulo.getAccumuloPersistentVersion(fs);
      if (Accumulo.persistentVersionNeedsUpgrade(accumuloPersistentVersion)) {
        // sanity check that we passed the Fate verification prior to ZooKeeper upgrade, and that Fate still hasn't been started.
        // Change both to use Guava's Verify once we use Guava 17.
        if (!haveUpgradedZooKeeper) {
          throw new IllegalStateException(
              "We should only attempt to upgrade Accumulo's metadata table if we've already upgraded ZooKeeper. Please save all logs and file a bug.");
        }
        if (null != fate) {
          throw new IllegalStateException(
              "Access to Fate should not have been initialized prior to the Master finishing upgrades. Please save all logs and file a bug.");
        }
        Runnable upgradeTask = new Runnable() {
          int version = accumuloPersistentVersion;

          @Override
          public void run() {
            try {
              log.info("Starting to upgrade metadata table.");
              if (version == ServerConstants.MOVE_DELETE_MARKERS - 1) {
                log.info("Updating Delete Markers in metadata table for version 1.4");
                MetadataTableUtil.moveMetaDeleteMarkersFrom14(Master.this);
                version++;
              }
              if (version == ServerConstants.MOVE_TO_ROOT_TABLE - 1) {
                log.info("Updating Delete Markers in metadata table.");
                MetadataTableUtil.moveMetaDeleteMarkers(Master.this);
                version++;
              }
              if (version == ServerConstants.MOVE_TO_REPLICATION_TABLE - 1) {
                log.info("Updating metadata table with entries for the replication table");
                MetadataTableUtil.createReplicationTable(Master.this);
                version++;
              }
              log.info("Updating persistent data version.");
              Accumulo.updateAccumuloVersion(fs, accumuloPersistentVersion);
              log.info("Upgrade complete");
              waitForMetadataUpgrade.countDown();
            } catch (Exception ex) {
              // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
              log.error("FATAL: Error performing upgrade", ex);
              System.exit(1);
            }

          }
        };

        // need to run this in a separate thread because a lock is held that prevents metadata tablets from being assigned and this task writes to the
        // metadata table
        new Thread(upgradeTask).start();
      } else {
        waitForMetadataUpgrade.countDown();
      }
    }
  }

  public void mustBeOnline(final String tableId) throws ThriftTableOperationException {
    Tables.clearCache(getInstance());
    if (!Tables.getTableState(getInstance(), tableId).equals(TableState.ONLINE))
      throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.OFFLINE, "table is not online");
  }

  public MergeInfo getMergeInfo(String tableId) {
    synchronized (mergeLock) {
      try {
        String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + tableId + "/merge";
        if (!ZooReaderWriter.getInstance().exists(path))
          return new MergeInfo();
        byte[] data = ZooReaderWriter.getInstance().getData(path, new Stat());
        DataInputBuffer in = new DataInputBuffer();
        in.reset(data, data.length);
        MergeInfo info = new MergeInfo();
        info.readFields(in);
        return info;
      } catch (KeeperException.NoNodeException ex) {
        log.info("Error reading merge state, it probably just finished");
        return new MergeInfo();
      } catch (Exception ex) {
        log.warn("Unexpected error reading merge state", ex);
        return new MergeInfo();
      }
    }
  }

  public void setMergeState(MergeInfo info, MergeState state) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + info.getExtent().getTableId() + "/merge";
      info.setState(state);
      if (state.equals(MergeState.NONE)) {
        ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      } else {
        DataOutputBuffer out = new DataOutputBuffer();
        try {
          info.write(out);
        } catch (IOException ex) {
          throw new RuntimeException("Unlikely", ex);
        }
        ZooReaderWriter.getInstance().putPersistentData(path, out.getData(),
            state.equals(MergeState.STARTED) ? ZooUtil.NodeExistsPolicy.FAIL : ZooUtil.NodeExistsPolicy.OVERWRITE);
      }
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s set to %s", info.getExtent(), state);
  }

  public void clearMergeState(String tableId) throws IOException, KeeperException, InterruptedException {
    synchronized (mergeLock) {
      String path = ZooUtil.getRoot(getInstance().getInstanceID()) + Constants.ZTABLES + "/" + tableId + "/merge";
      ZooReaderWriter.getInstance().recursiveDelete(path, NodeMissingPolicy.SKIP);
      mergeLock.notifyAll();
    }
    nextEvent.event("Merge state of %s cleared", tableId);
  }

  void setMasterGoalState(MasterGoalState state) {
    try {
      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_GOAL_STATE, state.name().getBytes(),
          NodeExistsPolicy.OVERWRITE);
    } catch (Exception ex) {
      log.error("Unable to set master goal state in zookeeper");
    }
  }

  MasterGoalState getMasterGoalState() {
    while (true)
      try {
        byte[] data = ZooReaderWriter.getInstance().getData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_GOAL_STATE, null);
        return MasterGoalState.valueOf(new String(data));
      } catch (Exception e) {
        log.error("Problem getting real goal state from zookeeper: " + e);
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
  }

  public boolean hasCycled(long time) {
    for (TabletGroupWatcher watcher : watchers) {
      if (watcher.stats.lastScanFinished() < time)
        return false;
    }

    return true;
  }

  static enum TabletGoalState {
    HOSTED, UNASSIGNED, DELETED
  }

  TabletGoalState getSystemGoalState(TabletLocationState tls) {
    switch (getMasterState()) {
      case NORMAL:
        return TabletGoalState.HOSTED;
      case HAVE_LOCK: // fall-through intended
      case INITIAL: // fall-through intended
      case SAFE_MODE:
        if (tls.extent.isMeta())
          return TabletGoalState.HOSTED;
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_METADATA_TABLETS:
        if (tls.extent.isRootTablet())
          return TabletGoalState.HOSTED;
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_ROOT_TABLET:
        return TabletGoalState.UNASSIGNED;
      case STOP:
        return TabletGoalState.UNASSIGNED;
      default:
        throw new IllegalStateException("Unknown Master State");
    }
  }

  TabletGoalState getTableGoalState(KeyExtent extent) {
    TableState tableState = TableManager.getInstance().getTableState(extent.getTableId());
    if (tableState == null)
      return TabletGoalState.DELETED;
    switch (tableState) {
      case DELETING:
        return TabletGoalState.DELETED;
      case OFFLINE:
      case NEW:
        return TabletGoalState.UNASSIGNED;
      default:
        return TabletGoalState.HOSTED;
    }
  }

  TabletGoalState getGoalState(TabletLocationState tls, MergeInfo mergeInfo) {
    KeyExtent extent = tls.extent;
    // Shutting down?
    TabletGoalState state = getSystemGoalState(tls);
    if (state == TabletGoalState.HOSTED) {
      if (tls.current != null && serversToShutdown.contains(tls.current)) {
        return TabletGoalState.UNASSIGNED;
      }
      // Handle merge transitions
      if (mergeInfo.getExtent() != null) {
        log.debug("mergeInfo overlaps: " + extent + " " + mergeInfo.overlaps(extent));
        if (mergeInfo.overlaps(extent)) {
          switch (mergeInfo.getState()) {
            case NONE:
            case COMPLETE:
              break;
            case STARTED:
            case SPLITTING:
              return TabletGoalState.HOSTED;
            case WAITING_FOR_CHOPPED:
              if (tls.getState(tserverSet.getCurrentServers()).equals(TabletState.HOSTED)) {
                if (tls.chopped)
                  return TabletGoalState.UNASSIGNED;
              } else {
                if (tls.chopped && tls.walogs.isEmpty())
                  return TabletGoalState.UNASSIGNED;
              }

              return TabletGoalState.HOSTED;
            case WAITING_FOR_OFFLINE:
            case MERGING:
              return TabletGoalState.UNASSIGNED;
          }
        }
      }

      // taking table offline?
      state = getTableGoalState(extent);
      if (state == TabletGoalState.HOSTED) {
        // Maybe this tablet needs to be migrated
        TServerInstance dest = tabletDistributor.getMigrationDestination(extent);
        if (dest != null && tls.current != null && !dest.equals(tls.current)) {
          return TabletGoalState.UNASSIGNED;
        }
      }
    }
    return state;
  }
  
  public void run() throws IOException, InterruptedException, KeeperException {
    final String zroot = ZooUtil.getRoot(getInstance());

    getMasterLock(zroot + Constants.ZMASTER_LOCK);

    recoveryManager = new RecoveryManager(this);

    TableManager.getInstance().addObserver(this);

    StatusThread statusThread = new StatusThread();
    statusThread.start();

    MigrationCleanupThread migrationCleanupThread = new MigrationCleanupThread();
    migrationCleanupThread.start();

    tserverSet.startListeningForTabletServerChanges();

    ZooReaderWriter zReaderWriter = ZooReaderWriter.getInstance();

    zReaderWriter.getChildren(zroot + Constants.ZRECOVERY, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        nextEvent.event("Noticed recovery changes", event.getType());
        try {
          // watcher only fires once, add it back
          ZooReaderWriter.getInstance().getChildren(zroot + Constants.ZRECOVERY, this);
        } catch (Exception e) {
          log.error("Failed to add log recovery watcher back", e);
        }
      }
    });

    watchers.add(new TabletGroupWatcher(this, new MetaDataStateStore(this, this), null));
    watchers.add(new TabletGroupWatcher(this, new RootTabletStateStore(this, this), watchers.get(0)));
    watchers.add(new TabletGroupWatcher(this, new ZooTabletStateStore(new ZooStore(zroot)), watchers.get(1)));
    for (TabletGroupWatcher watcher : watchers) {
      watcher.start();
    }

    // Once we are sure the upgrade is complete, we can safely allow fate use.
    waitForMetadataUpgrade.await();

    try {
      final AgeOffStore<Master> store = new AgeOffStore<>(new org.apache.accumulo.fate.ZooStore<Master>(ZooUtil.getRoot(getInstance()) + Constants.ZFATE,
          ZooReaderWriter.getInstance()), 1000 * 60 * 60 * 8);

      int threads = getConfiguration().getCount(Property.MASTER_FATE_THREADPOOL_SIZE);

      fate = new Fate<>(this, store);
      fate.startTransactionRunners(threads);

      SimpleTimer.getInstance(getConfiguration()).schedule(new Runnable() {

        @Override
        public void run() {
          store.ageOff();
        }
      }, 63000, 63000);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    ZooKeeperInitialization.ensureZooKeeperInitialized(zReaderWriter, zroot);

    // Make sure that we have a secret key (either a new one or an old one from ZK) before we start
    // the master client service.
    if (null != authenticationTokenKeyManager && null != keyDistributor) {
      log.info("Starting delegation-token key manager");
      keyDistributor.initialize();
      authenticationTokenKeyManager.start();
      boolean logged = false;
      while (!authenticationTokenKeyManager.isInitialized()) {
        // Print out a status message when we start waiting for the key manager to get initialized
        if (!logged) {
          log.info("Waiting for AuthenticationTokenKeyManager to be initialized");
          logged = true;
        }
        sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
      }
      // And log when we are initialized
      log.info("AuthenticationTokenSecretManager is initialized");
    }

    clientHandler = new MasterClientServiceHandler(this);
    Iface rpcProxy = RpcWrapper.service(clientHandler, new Processor<Iface>(clientHandler));
    final Processor<Iface> processor;
    if (ThriftServerType.SASL == getThriftServerType()) {
      Iface tcredsProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clientHandler.getClass(), getConfiguration());
      processor = new Processor<>(tcredsProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    ServerAddress sa = TServerUtils.startServer(this, hostname, Property.MASTER_CLIENTPORT, processor, "Master", "Master Client Service Handler", null,
        Property.MASTER_MINTHREADS, Property.MASTER_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);
    clientService = sa.server;
    String address = sa.address.toString();
    log.info("Setting master lock data to " + address);
    masterLock.replaceLockData(address.getBytes());

    while (!clientService.isServing()) {
      sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    // Start the daemon to scan the replication table and make units of work
    replicationWorkDriver = new ReplicationDriver(this);
    replicationWorkDriver.start();

    // Start the daemon to assign work to tservers to replicate to our peers
    try {
      replicationWorkAssigner = new WorkDriver(this);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("Caught exception trying to initialize replication WorkDriver", e);
      throw new RuntimeException(e);
    }
    replicationWorkAssigner.start();

    // Start the replication coordinator which assigns tservers to service replication requests
    MasterReplicationCoordinator impl = new MasterReplicationCoordinator(this);
    ReplicationCoordinator.Processor<ReplicationCoordinator.Iface> replicationCoordinatorProcessor = new ReplicationCoordinator.Processor<>(RpcWrapper.service(
        impl, new ReplicationCoordinator.Processor<ReplicationCoordinator.Iface>(impl)));
    ServerAddress replAddress = TServerUtils.startServer(this, hostname, Property.MASTER_REPLICATION_COORDINATOR_PORT, replicationCoordinatorProcessor,
        "Master Replication Coordinator", "Replication Coordinator", null, Property.MASTER_REPLICATION_COORDINATOR_MINTHREADS,
        Property.MASTER_REPLICATION_COORDINATOR_THREADCHECK, Property.GENERAL_MAX_MESSAGE_SIZE);

    log.info("Started replication coordinator service at " + replAddress.address);

    // Advertise that port we used so peers don't have to be told what it is
    ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_REPLICATION_COORDINATOR_ADDR,
        replAddress.address.toString().getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);

    // Register replication metrics
    MasterMetricsFactory factory = new MasterMetricsFactory(getConfiguration(), this);
    Metrics replicationMetrics = factory.createReplicationMetrics();
    try {
      replicationMetrics.register();
    } catch (Exception e) {
      log.error("Failed to register replication metrics", e);
    }

    while (clientService.isServing()) {
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    log.info("Shutting down fate.");
    fate.shutdown();

    final long deadline = System.currentTimeMillis() + MAX_CLEANUP_WAIT_TIME;
    statusThread.join(remaining(deadline));
    replicationWorkAssigner.join(remaining(deadline));
    replicationWorkDriver.join(remaining(deadline));
    replAddress.server.stop();
    // Signal that we want it to stop, and wait for it to do so.
    if (authenticationTokenKeyManager != null) {
      authenticationTokenKeyManager.gracefulStop();
      authenticationTokenKeyManager.join(remaining(deadline));
    }

    // quit, even if the tablet servers somehow jam up and the watchers
    // don't stop
    for (TabletGroupWatcher watcher : watchers) {
      watcher.join(remaining(deadline));
    }
    log.info("exiting");
  }

  private long remaining(long deadline) {
    return Math.max(1, deadline - System.currentTimeMillis());
  }

  public ZooLock getMasterLock() {
    return masterLock;
  }

  private static class MasterLockWatcher implements ZooLock.AsyncLockWatcher {

    boolean acquiredLock = false;
    boolean failedToAcquireLock = false;

    @Override
    public void lostLock(LockLossReason reason) {
      Halt.halt("Master lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
    }

    @Override
    public void unableToMonitorLockNode(final Throwable e) {
      // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
      Halt.halt(-1, new Runnable() {
        @Override
        public void run() {
          log.error("FATAL: No longer able to monitor master lock node", e);
        }
      });

    }

    @Override
    public synchronized void acquiredLock() {
      log.debug("Acquired master lock");

      if (acquiredLock || failedToAcquireLock) {
        Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      log.warn("Failed to get master lock " + e);

      if (e instanceof NoAuthException) {
        String msg = "Failed to acquire master lock due to incorrect ZooKeeper authentication.";
        log.error(msg + " Ensure instance.secret is consistent across Accumulo configuration", e);
        Halt.halt(msg, -1);
      }

      if (acquiredLock) {
        Halt.halt("Zoolock in unexpected state FAL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          wait();
        } catch (InterruptedException e) {}
      }
    }
  }

  private void getMasterLock(final String zMasterLoc) throws KeeperException, InterruptedException {
    log.info("trying to get master lock");

    final String masterClientAddress = hostname + ":" + getConfiguration().getPort(Property.MASTER_CLIENTPORT)[0];

    while (true) {

      MasterLockWatcher masterLockWatcher = new MasterLockWatcher();
      masterLock = new ZooLock(zMasterLoc);
      masterLock.lockAsync(masterLockWatcher, masterClientAddress.getBytes());

      masterLockWatcher.waitForChange();

      if (masterLockWatcher.acquiredLock) {
        break;
      }

      if (!masterLockWatcher.failedToAcquireLock) {
        throw new IllegalStateException("master lock in unknown state");
      }

      masterLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(TIME_TO_WAIT_BETWEEN_LOCK_CHECKS, TimeUnit.MILLISECONDS);
    }

    setMasterState(MasterState.HAVE_LOCK);
  }

  public static void main(String[] args) throws Exception {
    try {
      SecurityUtil.serverLogin(SiteConfiguration.getInstance());

      ServerOpts opts = new ServerOpts();
      final String app = "master";
      opts.parseArgs(app, args);
      String hostname = opts.getAddress();
      Accumulo.setupLogging(app);
      ServerConfigurationFactory conf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
      VolumeManager fs = VolumeManagerImpl.get();
      Accumulo.init(fs, conf, app);
      Master master = new Master(conf, fs, hostname);
      DistributedTrace.enable(hostname, app, conf.getConfiguration());
      master.run();
    } catch (Exception ex) {
      log.error("Unexpected exception, exiting", ex);
      System.exit(1);
    } finally {
      DistributedTrace.disable();
    }
  }

  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(getInstance()) + Constants.ZDEADTSERVERS);
    if (added.size() > 0) {
      log.info("New servers: " + added);
      for (TServerInstance up : added)
        obit.delete(up.hostPort());
    }
    for (TServerInstance dead : deleted) {
      String cause = "unexpected failure";
      if (serversToShutdown.contains(dead))
        cause = "clean shutdown"; // maybe an incorrect assumption
      if (!getMasterGoalState().equals(MasterGoalState.CLEAN_STOP))
        obit.post(dead.hostPort(), cause);
    }

    Set<TServerInstance> unexpected = new HashSet<>(deleted);
    unexpected.removeAll(this.serversToShutdown);
    if (unexpected.size() > 0) {
      if (stillMaster() && !getMasterGoalState().equals(MasterGoalState.CLEAN_STOP)) {
        log.warn("Lost servers " + unexpected);
      }
    }
    serversToShutdown.removeAll(deleted);
    badServers.keySet().removeAll(deleted);
    // clear out any bad server with the same host/port as a new server
    synchronized (badServers) {
      cleanListByHostAndPort(badServers.keySet(), deleted, added);
    }
    synchronized (serversToShutdown) {
      cleanListByHostAndPort(serversToShutdown, deleted, added);
    }

    synchronized (migrations) {
      Iterator<Entry<KeyExtent,TServerInstance>> iter = migrations.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<KeyExtent,TServerInstance> entry = iter.next();
        if (deleted.contains(entry.getValue())) {
          log.info("Canceling migration of " + entry.getKey() + " to " + entry.getValue());
          iter.remove();
        }
      }
    }
    nextEvent.event("There are now %d tablet servers", current.size());
  }

  private static void cleanListByHostAndPort(Collection<TServerInstance> badServers, Set<TServerInstance> deleted, Set<TServerInstance> added) {
    Iterator<TServerInstance> badIter = badServers.iterator();
    while (badIter.hasNext()) {
      TServerInstance bad = badIter.next();
      for (TServerInstance add : added) {
        if (bad.hostPort().equals(add.hostPort())) {
          badIter.remove();
          break;
        }
      }
      for (TServerInstance del : deleted) {
        if (bad.hostPort().equals(del.hostPort())) {
          badIter.remove();
          break;
        }
      }
    }
  }

  @Override
  public void stateChanged(String tableId, TableState state) {
    nextEvent.event("Table state in zookeeper changed for %s to %s", tableId, state);
    if (TableState.OFFLINE == state) {
      clearMigrations(tableId);
    }
  }

  @Override
  public void initialize(Map<String,TableState> tableIdToStateMap) {}

  @Override
  public void sessionExpired() {}

  @Override
  public Set<String> onlineTables() {
    Set<String> result = new HashSet<>();
    if (getMasterState() != MasterState.NORMAL) {
      if (getMasterState() != MasterState.UNLOAD_METADATA_TABLETS)
        result.add(MetadataTable.ID);
      if (getMasterState() != MasterState.UNLOAD_ROOT_TABLET)
        result.add(RootTable.ID);
      return result;
    }
    TableManager manager = TableManager.getInstance();

    for (String tableId : Tables.getIdToNameMap(getInstance()).keySet()) {
      TableState state = manager.getTableState(tableId);
      if (state != null) {
        if (state == TableState.ONLINE)
          result.add(tableId);
      }
    }
    return result;
  }

  @Override
  public Set<TServerInstance> onlineTabletServers() {
    return tserverSet.getCurrentServers();
  }

  @Override
  public Collection<MergeInfo> merges() {
    List<MergeInfo> result = new ArrayList<>();
    for (String tableId : Tables.getIdToNameMap(getInstance()).keySet()) {
      result.add(getMergeInfo(tableId));
    }
    return result;
  }

  // recovers state from the persistent transaction to shutdown a server
  public void shutdownTServer(TServerInstance server) {
    nextEvent.event("Tablet Server shutdown requested for %s", server);
    serversToShutdown.add(server);
  }

  public EventCoordinator getEventCoordinator() {
    return nextEvent;
  }

  public ServerConfigurationFactory getConfigurationFactory() {
    return serverConfig;
  }

  public VolumeManager getFileSystem() {
    return this.fs;
  }

  public void assignedTablet(KeyExtent extent) {
    if (extent.isMeta()) {
      if (getMasterState().equals(MasterState.UNLOAD_ROOT_TABLET)) {
        setMasterState(MasterState.UNLOAD_METADATA_TABLETS);
      }
    }
    if (extent.isRootTablet()) {
      // probably too late, but try anyhow
      if (getMasterState().equals(MasterState.STOP)) {
        setMasterState(MasterState.UNLOAD_ROOT_TABLET);
      }
    }
  }

  public MasterMonitorInfo getMasterMonitorInfo() {
    final MasterMonitorInfo result = new MasterMonitorInfo();

    result.tServerInfo = new ArrayList<>();
    result.tableMap = new DefaultMap<>(new TableInfo());
    for (Entry<TServerInstance,TabletServerStatus> serverEntry : tserverStatus.entrySet()) {
      final TabletServerStatus status = serverEntry.getValue();
      result.tServerInfo.add(status);
      for (Entry<String,TableInfo> entry : status.tableMap.entrySet()) {
        TableInfoUtil.add(result.tableMap.get(entry.getKey()), entry.getValue());
      }
    }
    result.badTServers = new HashMap<>();
    synchronized (badServers) {
      for (TServerInstance bad : badServers.keySet()) {
        result.badTServers.put(bad.hostPort(), TabletServerState.UNRESPONSIVE.getId());
      }
    }
    result.state = getMasterState();
    result.goalState = getMasterGoalState();
    result.unassignedTablets = displayUnassigned();
    result.serversShuttingDown = new HashSet<>();
    synchronized (serversToShutdown) {
      for (TServerInstance server : serversToShutdown)
        result.serversShuttingDown.add(server.hostPort());
    }
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(getInstance()) + Constants.ZDEADTSERVERS);
    result.deadTabletServers = obit.getList();
    result.bulkImports = bulkImportStatus.getBulkLoadStatus();
    return result;
  }

  /**
   * Can delegation tokens be generated for users
   */
  public boolean delegationTokensAvailable() {
    return delegationTokensAvailable;
  }

  @Override
  public Set<TServerInstance> shutdownServers() {
    synchronized (serversToShutdown) {
      return new HashSet<>(serversToShutdown);
    }
  }

  public void markDeadServerLogsAsClosed(Map<TServerInstance,List<Path>> logsForDeadServers) throws WalMarkerException {
    WalStateManager mgr = new WalStateManager(this.inst, ZooReaderWriter.getInstance());
    for (Entry<TServerInstance,List<Path>> server : logsForDeadServers.entrySet()) {
      for (Path path : server.getValue()) {
        mgr.closeWal(server.getKey(), path);
      }
    }
  }

  public void updateBulkImportStatus(String directory, BulkImportState state) {
    bulkImportStatus.updateBulkImportStatus(Collections.singletonList(directory), state);
  }

  public void removeBulkImportStatus(String directory) {
    bulkImportStatus.removeBulkImportStatus(Collections.singletonList(directory));
  }
}
