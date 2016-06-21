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

import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single threaded scheduler for asynchronous jobs.  */
public class AsyncEngine extends Thread {
  private static Logger log=LoggerFactory.getLogger(AsyncEngine.class);
  private static ThreadLocal<LocalEngine> CURRENT_ENGINE=new ThreadLocal<>();
  
  private final Set<Supplier<Boolean>> pollers=new HashSet<>();
  private final ArrayDeque<Runnable>[] queues=new ArrayDeque[DelayTolerance.values().length];
  private final ConcurrentLinkedQueue<IncomingJob> incomingJobs=new ConcurrentLinkedQueue<>();
  
  private DelayTolerance currentTolerance;

  public AsyncEngine() {
    this("AsyncEngine");
  }
  
  public AsyncEngine(String name) {
    super(name);
    for (int i=0;i<queues.length;++i) {
      queues[i]=new ArrayDeque();
    }
  }
  
  /** Submit work to the {@code AsyncEngine} in the form of a function which generates an {@link AsyncFuture}.  Upon
   * completion, the result will be made available as a CompletableFuture.
   * @param <T>
   * @param job
   * @return 
   */
  public <T> Future<T> submit(DelayTolerance tolerance, Supplier<AsyncFuture<T>> jobSupplier) {
    SettableFuture<T> future=SettableFuture.create();
    submit(new IncomingJob(tolerance, () -> {
      try {
        jobSupplier.get().handle((value, problem) -> {
          if (problem==null) {
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
  
  /** 
   * Get the engine running on the current thread, or throw {@link IllegalStateException} if the current thread
   * is not controlled by an AsyncEngine.
   */
  public static LocalEngine getLocalEngine() throws AsyncEngineMissingException {
    LocalEngine current=CURRENT_ENGINE.get();
    if (current==null) {
      throw new AsyncEngineMissingException();
    } else {
      return current;
    }
  }
  
  /** Operations which can be executed from within the thread of the AsyncEngine. */
  public class LocalEngine {
    /** Request that the engine periodically run the specified function, until it returns false */
    public void poll(Supplier<Boolean> poller) {
      pollers.add(poller);
    }

    /** Request that the engine arrange for the specified task to be run. */
    public void schedule(DelayTolerance priority, Runnable task) {
      scheduleLocal(priority, task);
    }

    /** Return the tolerance of the currently running job. */
    public DelayTolerance getCurrentTolerance() {
      return currentTolerance;
    }
    
    /** Submit a job to another engine. */
    public <T> AsyncFuture<T> submitTo(AsyncEngine otherEngine, Supplier<AsyncFuture<T>> jobSupplier) {
      AsyncPromise<T> promise=new AsyncPromise<>();
      DelayTolerance curTol=getCurrentTolerance();
      otherEngine.submit(new IncomingJob(curTol, () -> {
        try {
          jobSupplier.get().handle((value, problem) -> {
            submit(new IncomingJob(DelayTolerance.INTOLERANT_PLUS, () -> {
              promise.setResult(value, problem);
            }));
            return null;
          });
        } catch (Exception ex) {
          submit(new IncomingJob(DelayTolerance.INTOLERANT_PLUS, () -> {
            promise.setException(ex);
          }));
        }
      }));
      return promise.getFuture();
    }
  }

  private static class IncomingJob {
    private final DelayTolerance tolerance;
    private final Runnable runnable;
    IncomingJob(DelayTolerance tolerance, Runnable runnable) {
      this.tolerance=tolerance;
      this.runnable=runnable;
    }
  }
  
  protected void scheduleLocal(DelayTolerance tolerance, Runnable job) {
    queues[tolerance.ordinal()].addLast(job);    
  }
  
  protected synchronized void submit(IncomingJob job) {
    incomingJobs.add(job);
    notify();
  }
  
  @Override
  public void run() {
    CURRENT_ENGINE.set(new LocalEngine());
    while (true) {
      if (isInterrupted()) {
        interrupt();
        return;
      }
      // Run through all pollers.
      Iterator<Supplier<Boolean>> pollIt=pollers.iterator();
      while (pollIt.hasNext()) {
        try {
          if (!pollIt.next().get()) {
            pollIt.remove();
          }
        } catch (Exception ex) {
          log.error("Caught exception while executing poll function", ex);
          pollIt.remove();
        }
      }
      boolean hadWork=false;
      // Drain the incoming job pool
      for (IncomingJob incomingJob=incomingJobs.poll(); incomingJob!=null; incomingJob=incomingJobs.poll()) {
        hadWork=true;
        scheduleLocal(incomingJob.tolerance, incomingJob.runnable);
      }
      for (int queueNum=0;queueNum<queues.length;++queueNum) {
        currentTolerance=DelayTolerance.values()[queueNum];
        for (int i=0;i<1<<(queues.length-queueNum);++i) {
          if (queues[queueNum].isEmpty()) {
            break;
          }
          try {
            queues[queueNum].pollFirst().run();
          } catch (Throwable th) {
            log.error("Exception caught while running async job", th);
          }
          hadWork=true;
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
