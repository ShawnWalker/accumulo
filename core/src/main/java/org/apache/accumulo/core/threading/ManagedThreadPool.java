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
package org.apache.accumulo.core.threading;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ManagedThreadPool extends AbstractExecutorService {
  private final static Logger log = LoggerFactory.getLogger(ManagedThreadPool.class);
  private final AtomicLong idCounter = new AtomicLong(0);
  private final ExecutorService corePool;

  ManagedThreadPool() {
    corePool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("Idle thread " + idCounter.incrementAndGet());
        return t;
      }
    });
  }

  @PreDestroy
  void finish() throws InterruptedException {
    shutdownNow();

    for (boolean finished = awaitTermination(5, TimeUnit.SECONDS); !finished; corePool.awaitTermination(5, TimeUnit.SECONDS)) {
      log.info("Awaiting shutdown of core thread pool");
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    return corePool.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return corePool.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return corePool.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return corePool.awaitTermination(timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    corePool.execute(command);
  }

  @Override
  public void shutdown() {
    corePool.shutdown();
  }
}
