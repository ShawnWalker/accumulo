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
package org.apache.accumulo.core.async;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.accumulo.core.async.AsyncScheduler.Priority;
import org.apache.accumulo.core.async.AsyncScheduler.Quantum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single threaded scheduler for asynchronous jobs. */
public class AsyncEngine extends Thread {
  protected static final Supplier<AsyncScheduler> DEFAULT_SCHEDULER=() -> new FifoAsyncScheduler();
  private static final Logger log = LoggerFactory.getLogger(AsyncEngine.class);
  private static final ThreadLocal<LocalEngine> CURRENT_ENGINE = new ThreadLocal<>();
  
  private final Set<AsyncPollTask> pollers = new HashSet<>();
  private final AsyncScheduler scheduler;
  private final ConcurrentLinkedQueue<Quantum> incomingJobs = new ConcurrentLinkedQueue<>();
  private AsyncScheduler.Priority activePriority;

  public AsyncEngine() {
    this(AsyncEngine.class.getSimpleName());
  }

  public AsyncEngine(String name) {
    this(name, DEFAULT_SCHEDULER.get());
  }
  
  public AsyncEngine(String name, AsyncScheduler scheduler) {
    super(name);
    this.scheduler = scheduler;
  }

  /**
   * Submit work to the {@code AsyncEngine} in the form of a function which generates an {@link AsyncFuture}. Upon completion, the result will be made available
   * as a CompletableFuture.
   * 
   * @param <T>
   * @param job
   * @return
   */
  public <T> Future<T> submit(ImmutableMap<String,String> priorityOptions, Supplier<AsyncFuture<T>> jobSupplier) {
    SettableFuture<T> future = SettableFuture.create();
    submit(new AsyncScheduler.Quantum(scheduler.initialize(priorityOptions), () -> {
      try {
        jobSupplier.get().handle((value, problem) -> {
          if (problem == null) {
            future.set(value);
          } else {
            future.setException(problem);
          }
          return null;
        });
      } catch (Exception ex) {
        future.setException(ex);
      }
    }));
    return future;
  }

  /** Add work to the engine from a different thread. */
  protected synchronized void submit(AsyncScheduler.Quantum work) {
    incomingJobs.add(work);
    notify();
  }  
  
  /**
   * Get the engine running on the current thread, or throw {@link IllegalStateException} if the current thread is not controlled by an AsyncEngine.
   */
  public static LocalEngine getLocalEngine() throws AsyncEngineMissingException {
    LocalEngine current = CURRENT_ENGINE.get();
    if (current == null) {
      throw new AsyncEngineMissingException();
    } else {
      return current;
    }
  }
  
  /** Create an instance of the LocalEngine. */
  protected LocalEngine constructLocalEngine() {
    return new LocalEngine();
  }
  
  /** Operations which can be executed from within the thread of the AsyncEngine. */
  public class LocalEngine {                
    /** Request that the engine periodically run the specified function, until it returns false */
    public void addPollTask(AsyncPollTask poller) {
      pollers.add(poller);
    }

    /** Get the priority of the task this engine is currently running. */
    public Priority getCurrentPriority() {
      return activePriority;
    }

    /** Request that the engine arrange for the specified task to be run. */
    public void schedule(Quantum q) {
      scheduler.enqueue(q);
    }

    /** Submit a job to another engine. */
    public <T> AsyncFuture<T> submitTo(AsyncEngine otherEngine, Supplier<AsyncFuture<T>> jobSupplier) {
      AsyncPromise<T> promise = new AsyncPromise<>();
      final Priority priority = getCurrentPriority();
      otherEngine.submit(priority.options, () -> {
        try {
          jobSupplier.get().handle((value, problem) -> {
            submit(new Quantum(priority, () -> {
              promise.setResult(value, problem);
            }));
            return null;
          });
        } catch (Exception ex) {
          submit(new Quantum(priority, () -> {
            promise.setException(ex);
          }));
        }
        return null;
      });
      return promise.getFuture();
    }
  }

  @Override
  public void run() {
    CURRENT_ENGINE.set(constructLocalEngine());
    while (true) {
      if (isInterrupted()) {
        interrupt();
        return;
      }
      // Run through all pollers.
      Iterator<AsyncPollTask> pollIt = pollers.iterator();
      while (pollIt.hasNext()) {
        try {
          if (!pollIt.next().poll()) {
            pollIt.remove();
          }
        } catch (Exception ex) {
          log.error("Caught exception while executing poll function", ex);
          pollIt.remove();
        }
      }
      boolean hadWork = false;
      // Drain the incoming job pool
      for (Quantum incomingJob = incomingJobs.poll(); incomingJob != null; incomingJob = incomingJobs.poll()) {
        hadWork = true;
        scheduler.enqueue(incomingJob);
      }
      for (int jobNum = 0; jobNum < 128; ++jobNum) {
        AsyncScheduler.Quantum task = scheduler.poll();
        if (task == null) {
          break;
        }
        try {
          activePriority = task.priority;
          hadWork = true;
          task.r.run();
        } catch (Throwable th) {
          log.error("Exception caught while running async job", th);
        } finally {
          activePriority = null;
        }
      }
      if (!hadWork) {
        synchronized (this) {
          if (incomingJobs.isEmpty()) {
            try {
              wait(10);
            } catch (InterruptedException ex) {
              interrupt();
              return;
            }
          }
        }
      }
    }
  }
}
