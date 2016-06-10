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
package org.apache.accumulo.server.util.time;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Longs;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Track and query the time a particular server was last alive. These times are necessarily inaccurate. */
public class LastAlive {
  private static final Logger log = LoggerFactory.getLogger(LastAlive.class);
  private static LastAlive INSTANCE;

  /** How long to wait before querying the time from ZooKeeper again, assuming a successful read. */
  private static final long SUCCESS_READ_GRANULARITY = TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
  /** How long to wait before querying the time from ZooKeeper again, assuming a failed read. */
  private static final long FAIL_READ_GRANULARITY = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);

  /** How long to wait before posting the time to ZooKeeper again, assuming a successful write. */
  private static final long SUCCESS_WRITE_GRANULARITY = TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
  /** How long to wait before posting the time to ZooKeeper again, assuming a failed write. */
  private static final long FAIL_WRITE_GRANULARITY = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);

  /** When to give up on old entries. */
  private static final long CACHE_DURATION = TimeUnit.NANOSECONDS.convert(1, TimeUnit.HOURS);

  /** How many cache entries to keep, at most. */
  private static final long CACHE_SIZE = 1000000;

  private final ZooReaderWriter zk = ZooReaderWriter.getInstance();
  private final String zooRoot;

  private final NodeLastAlive master;
  private final CacheLoader<HostAndPort,NodeLastAlive> loader = new CacheLoader<HostAndPort,NodeLastAlive>() {
    @Override
    public NodeLastAlive load(HostAndPort tserverLocation) {
      return new NodeLastAlive(zooRoot + Constants.ZLASTALIVE_TSERVERS + "/" + tserverLocation.toString(), false);
    }
  };
  private final LoadingCache<HostAndPort,NodeLastAlive> tservers = CacheBuilder.newBuilder().expireAfterAccess(CACHE_DURATION, TimeUnit.NANOSECONDS)
      .maximumSize(CACHE_SIZE).build(loader);

  private LastAlive(String instanceId) {
    this.zooRoot = ZooUtil.getRoot(instanceId);
    this.master = new NodeLastAlive(zooRoot + Constants.ZLASTALIVE_MASTER, true);
  }

  public synchronized static LastAlive getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new LastAlive(HdfsZooInstance.getInstance().getInstanceID());
    }
    return INSTANCE;
  }

  /** Only to be called by the master server with the lock, this writes out that the master server is currently alive. */
  public void postMasterAlive() throws InterruptedException {
    master.postAlive(System.currentTimeMillis());
  }

  /** Only to be called by a tserver with its lock valid, this writes out that the tserver is currently alive. */
  public void postTserverAlive(HostAndPort tserverLocation) throws InterruptedException {
    // So we need not worry about clock skew between servers, we make the approximation that we're last alive at the
    // last time that the master reported it was last alive.
    Long masterLastAlive = master.getLastAlive();
    if (masterLastAlive == null) {
      // Failed to get time base posted by a master server, so we can't record the liveness of the tserver
      return;
    }
    tservers.getUnchecked(tserverLocation).postAlive(masterLastAlive);
  }

  /** Determine the last time the specified tserver was alive. */
  public Long getTserverLastAlive(HostAndPort tserverLocation) throws InterruptedException {
    return tservers.getUnchecked(tserverLocation).getLastAlive();
  }

  /**
   * Remove really old entries from being tracked.
   *
   * @param duration
   *          How far out of sync an entry must be before it's eligible for removal
   * @param unit
   *          unit for the specified duration.
   */
  public synchronized void trimStore(long duration, TimeUnit unit) throws KeeperException, InterruptedException {
    long threshold = TimeUnit.MILLISECONDS.convert(duration, unit);

    Long masterTime = master.getLastAlive();
    if (masterTime == null) {
      return;
    }

    try {
      List<String> entries = zk.getChildren(zooRoot + Constants.ZLASTALIVE_TSERVERS);
      for (String entry : entries) {
        String key = zooRoot + Constants.ZLASTALIVE_TSERVERS + "/" + entry;
        long timestamp = Longs.fromByteArray(zk.getData(key, null));
        if (Math.abs(timestamp - masterTime) > threshold) {
          zk.delete(key, -1);
        }
      }
    } catch (KeeperException ex) {}
  }

  /** Keep track of information per node. */
  private class NodeLastAlive {
    private final Random jitterAmount = new Random();
    private final String zPath;
    private final boolean ephemeral;

    private long nextQueryTime;
    private long nextPostTime;

    private Long lastRetrievedTime;

    NodeLastAlive(String zPath, boolean ephemeral) {
      this.zPath = zPath;
      this.ephemeral = ephemeral;
    }

    /** Retrieve the current time this node was last seen, or null if it hasn't ever been seen. */
    public synchronized Long getLastAlive() throws InterruptedException {
      long nanoTime = System.nanoTime();

      // Throttle reads so as not to overload ZooKeeper
      if (nanoTime >= nextQueryTime) {
        try {
          byte[] masterTime = zk.getData(zPath, null);
          lastRetrievedTime = Longs.fromByteArray(masterTime);
          nextQueryTime = nanoTime + jitter(SUCCESS_READ_GRANULARITY);
        } catch (KeeperException ex) {
          lastRetrievedTime = null;
          if (ex.code() == Code.NONODE) {
            // count "not found" as a success, so we don't slam ZooKeeper polling for a missing value.
            nextQueryTime = nanoTime + jitter(SUCCESS_READ_GRANULARITY);
          } else {
            if (log.isDebugEnabled()) {
              log.warn("Failed to retrieve last alive time from " + zPath, ex);
              nextQueryTime = nanoTime + jitter(FAIL_READ_GRANULARITY);
            }
          }
        }
      }
      return lastRetrievedTime;
    }

    public synchronized void postAlive(long time) throws InterruptedException {
      long nanoTime = System.nanoTime();

      // Since we provided a time, we shouldn't need to read one.
      lastRetrievedTime = time;
      nextQueryTime = nanoTime + jitter(SUCCESS_READ_GRANULARITY);

      // Throttle writes so as not to overload ZooKeeper
      if (nanoTime >= nextPostTime) {
        try {
          final byte[] timeBytes = Longs.toByteArray(time);
          if (ephemeral) {
            zk.putEphemeralData(zPath, timeBytes, ZooUtil.NodeExistsPolicy.OVERWRITE);
          } else {
            zk.putPersistentData(zPath, timeBytes, ZooUtil.NodeExistsPolicy.OVERWRITE);
          }
          nextPostTime = nanoTime + jitter(SUCCESS_WRITE_GRANULARITY);
        } catch (KeeperException | InterruptedException ex) {
          if (log.isDebugEnabled()) {
            log.debug("Failed to update last alive time at " + zPath, ex);
          }
          nextPostTime = nanoTime + jitter(FAIL_WRITE_GRANULARITY);
        }
      }
    }

    /** Add randomness to a duration. */
    private synchronized long jitter(long duration) {
      // 20% jitter.
      return duration + jitterAmount.nextLong() % (duration / 5);
    }

  }
}
