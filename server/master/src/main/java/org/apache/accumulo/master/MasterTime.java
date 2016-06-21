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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Keep a persistent roughly monotone view of how long a master has been overseeing this cluster. */
public class MasterTime extends TimerTask {
  private static final Logger log = LoggerFactory.getLogger(MasterTime.class);

  private final String zPath;
  private final ZooReaderWriter zk;
  private final Master master;
  private final Timer timer;

  /** System.nanoTime() when we last performed a read from ZooKeeper. */
  private long readTimeSNT;

  /** ZooKeeper-reported timestamp from our last ZooKeeper read. */
  private long readTimeZK;

  public MasterTime(Master master) throws IOException {
    this.zPath = ZooUtil.getRoot(master.getInstance()) + Constants.ZMASTER_TICK;
    this.zk = ZooReaderWriter.getInstance();
    this.master = master;

    try {
      zk.putPersistentData(zPath, "0".getBytes(StandardCharsets.UTF_8), NodeExistsPolicy.SKIP);
      readTimeZK = Long.parseLong(new String(zk.getData(zPath, null), StandardCharsets.UTF_8));
      readTimeSNT = System.nanoTime();
    } catch (Exception ex) {
      throw new IOException("Error updating master time", ex);
    }

    this.timer = new Timer();
    timer.schedule(this, 0, MILLISECONDS.convert(10, SECONDS));
  }

  /**
   * How long has this cluster had a Master?
   *
   * @returns Approximate total duration this cluster has had a Master, in milliseconds, or null if this information is not presently available.
   */
  public synchronized long getTime() {
    return MILLISECONDS.convert(System.nanoTime() - readTimeSNT + readTimeZK, NANOSECONDS);
  }

  @Override
  public void run() {
    switch (master.getMasterState()) {
      case INITIAL:
      case STOP:
        try {
          long readTimeZKTmp = Long.parseLong(new String(zk.getData(zPath, null), StandardCharsets.UTF_8));
          synchronized (this) {
            readTimeZK = readTimeZKTmp;
            readTimeSNT = System.nanoTime();
          }
        } catch (Exception ex) {
          if (log.isDebugEnabled()) {
            log.debug("Failed to retrieve master tick time", ex);
          }
        }
        break;
      case HAVE_LOCK:
      case SAFE_MODE:
      case NORMAL:
      case UNLOAD_METADATA_TABLETS:
      case UNLOAD_ROOT_TABLET:
        try {
          zk.putPersistentData(zPath, Long.toString(System.nanoTime() - readTimeSNT + readTimeZK).getBytes(StandardCharsets.UTF_8), NodeExistsPolicy.OVERWRITE);
        } catch (Exception ex) {
          if (log.isDebugEnabled()) {
            log.debug("Failed to update master tick time", ex);
          }
        }
    }
  }
}
