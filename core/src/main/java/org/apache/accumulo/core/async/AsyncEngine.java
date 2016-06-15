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

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AsyncEngine extends Thread {
  private static Logger log=LoggerFactory.getLogger(AsyncEngine.class);
  private static ThreadLocal<AsyncEngine> CURRENT_ENGINE=new ThreadLocal<>();
  
  private final Set<Supplier<Boolean>> pollers=new HashSet<>();
  private final ArrayDeque<Runnable>[] queues=new ArrayDeque[DelayTolerance.values().length];
  private final ConcurrentLinkedQueue<SubmittedJob> incomingWork=new ConcurrentLinkedQueue<>();
  
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
  public synchronized <T> Future<T> submit(DelayTolerance tolerance, Supplier<AsyncFuture<T>> jobSupplier) {
    SubmittedJob<T> job=new SubmittedJob<T>(tolerance, jobSupplier);
    incomingWork.add(job);
    notify();
    return job.future;
  }
  
  
  /** 
   * Get the engine running on the current thread, or throw {@link IllegalStateException} if the current thread
   * is not controlled by an AsyncEngine.
   */
  public static AsyncEngine get() throws AsyncEngineMissingException {
    AsyncEngine current=CURRENT_ENGINE.get();
    if (current==null) {
      throw new AsyncEngineMissingException();
    } else {
      return current;
    }
  }
  
  /** Request that the engine periodically run the specified function, until it returns false */
  void addPoll(Supplier<Boolean> poller) {
    pollers.add(poller);
  }
  
  /** Request that the engine arrange for the specified task to be run. */
  void schedule(DelayTolerance priority, Runnable task) {
    queues[priority.ordinal()].addLast(task);
  }
  
  /** Return the tolerance of the currently running job. */
  DelayTolerance jobTolerance() {
    return currentTolerance;
  }
    
  private static class SubmittedJob<T> implements Runnable {
    private final DelayTolerance tolerance;
    private final Supplier<AsyncFuture<T>> jobSupplier;
    private final CompletableFuture<T> future;
    
    SubmittedJob(DelayTolerance tolerance, Supplier<AsyncFuture<T>> jobSupplier) {
      this.tolerance=tolerance;
      this.jobSupplier=jobSupplier;
      this.future=new CompletableFuture();
    }
    
    void schedule(AsyncEngine engine) {
      engine.schedule(tolerance, this);
    }
    
    @Override
    public void run() {
      jobSupplier.get().handle((value, problem) -> {
        if (problem!=null) {
          this.future.completeExceptionally(problem);
        } else {
          this.future.complete(value);
        }
        return null;
      });
    }
  }
  
  @Override
  public void run() {
    CURRENT_ENGINE.set(this);
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
      for (SubmittedJob incomingJob=incomingWork.poll(); incomingJob!=null; incomingJob=incomingWork.poll()) {
        hadWork=true;
        incomingJob.schedule(this);
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
          if (incomingWork.isEmpty()) {
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
