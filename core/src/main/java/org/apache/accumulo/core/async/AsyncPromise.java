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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.accumulo.core.async.AsyncScheduler.Quantum;

/** A writable, single assignment container for passing values. */
public class AsyncPromise<T> implements Cancellable {
  private final AsyncScheduler.Priority promisePriority;

  private static enum PromiseState {
    UNFULFILLED, FULFILLED, AWAITING_FUTURE, CANCELLED
  };

  private PromiseState state = PromiseState.UNFULFILLED;
  private T promisedValue;
  private Throwable promisedProblem;

  private Quantum firstWaitingTask;
  private List<Quantum> otherWaitingTasks;

  private Cancellable firstSource;
  private List<Cancellable> otherSources;

  public AsyncPromise(Cancellable... sources) {
    this(Arrays.asList(sources));
  }

  public AsyncPromise(Collection<? extends Cancellable> sources) {
    this(AsyncEngine.getLocalEngine().getCurrentPriority(), sources);
  }

  private AsyncPromise(AsyncScheduler.Priority promisePriority, Collection<? extends Cancellable> sources) {
    this.promisePriority = promisePriority;
    for (Cancellable source : sources) {
      if (firstSource == null) {
        firstSource = source;
      } else {
        if (otherSources == null) {
          otherSources = new ArrayList<>();
        }
        otherSources.add(source);
      }
    }
  }

  /** Return a future to receive data from this promise. */
  public AsyncFuture<T> getFuture() {
    return new PromisedFuture();
  }

  /**
   * Set the promise to {@code value}, and schedule all follow-on actions. After {@code setValue(...)} has been called, any subsequent calls to
   * {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be ignored.
   * 
   * @return true if the value was accepted, false if this {@code AsyncPromise} has already been set.
   */
  public boolean setValue(T value) {
    if (state == PromiseState.UNFULFILLED) {
      state = PromiseState.FULFILLED;
      this.promisedValue = value;
      dispatch();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set the promise to {@code problem}, and schedule all follow-on actions. After {@code setError(...)} has been called, any subsequent calls to
   * {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be ignored.
   * 
   * @return true if the exception was accepted, false if this {@code AsyncPromise} has already been set.
   */
  public boolean setException(Throwable problem) {
    if (state == PromiseState.UNFULFILLED) {
      state = PromiseState.FULFILLED;
      this.promisedProblem = Objects.requireNonNull(problem);
      dispatch();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Set the promise to {@code value}, and schedule all follow-on actions. Here, if {@code problem} is not null, then {@code value} is ignored and the
   * propagated state will be an exception. Otherwise the {@code value} will be propagated as a successful value (even if {@code value} is {@code null}). After
   * {@code setValue(...)} has been called, any subsequent calls to {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be
   * ignored.
   * 
   * @return true if the set was successful, false if this {@code AsyncPromise} has already been set.
   */
  public boolean setResult(T value, Throwable problem) {
    if (value!=null && problem!=null) {
      throw new IllegalStateException("May not set both value and problem");
    }
    if (state == PromiseState.UNFULFILLED) {
      state = PromiseState.FULFILLED;
      this.promisedValue = value;
      this.promisedProblem = problem;
      dispatch();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Have this promise follow the state of the specified future, triggering follow-on actions when the future becomes available. After {@code setError(...)} has
   * been called, any subsequent calls to {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be ignored.
   * 
   * @return true if the set was successful, false if this {@code AsyncPromise} has already been set.
   */
  public boolean setFuture(AsyncFuture<? extends T> future) {
    if (state == PromiseState.UNFULFILLED) {
      state = PromiseState.AWAITING_FUTURE;
      if (firstSource == null) {
        firstSource = future;
      } else {
        if (otherSources == null) {
          otherSources = new ArrayList<>();
        }
        otherSources.add(future);
      }
      future.handle((value, problem) -> {
        if (state == PromiseState.AWAITING_FUTURE) {
          this.promisedValue = value;
          this.promisedProblem = problem;
          state = PromiseState.FULFILLED;
          dispatch();
        }
        return null;
      });
      return true;
    } else if (state == PromiseState.CANCELLED) {
      future.cancel();
      return false;
    }
    return false;
  }

  @Override
  public void cancel(Throwable cause) {
    if (state == PromiseState.UNFULFILLED || state == PromiseState.AWAITING_FUTURE) {
      state = PromiseState.CANCELLED;
      promisedProblem = cause;
      dispatch();
    }
    if (firstSource != null) {
      firstSource.cancel(cause);
      firstSource = null;
    }
    if (otherSources != null) {
      for (Cancellable source : otherSources) {
        source.cancel(cause);
      }
      otherSources = null;
    }
  }

  /** Upon receipt of an event, send it to all of our sinks. */
  protected void dispatch() {
    AsyncEngine.LocalEngine engine = AsyncEngine.getLocalEngine();
    if (firstWaitingTask != null) {
      engine.schedule(firstWaitingTask);
    }
    if (otherWaitingTasks != null) {
      for (Quantum task : otherWaitingTasks) {
        engine.schedule(task);
      }
    }
    // In case this AsyncPromise is long-lived, release references to its task queue.
    firstWaitingTask = null;
    otherWaitingTasks = null;
  }

  private class PromisedFuture implements AsyncFuture<T> {
    @Override
    public void cancel(Throwable cause) {
      AsyncPromise.this.cancel(cause);
    }

    @Override
    public <U> AsyncFuture<U> handle(AsyncHandler<? super T,? extends U> reaction) {
      AsyncPromise<U> resultPromise = new AsyncPromise<>(AsyncPromise.this);
      chooseDispatch(new Quantum(promisePriority, ()-> {
        try {
          resultPromise.setValue(reaction.apply(promisedValue, promisedProblem));
        } catch (Throwable th) {
          resultPromise.setException(th);
        }
      }));
      return resultPromise.getFuture();
    }

    @Override
    public <U> AsyncFuture<U> handleSchedule(AsyncHandler<? super T,? extends AsyncFuture<? extends U>> reaction) {
      AsyncPromise<U> resultPromise = new AsyncPromise<>(AsyncPromise.this);
      chooseDispatch(new Quantum(promisePriority, ()-> {
        try {
          resultPromise.setFuture(reaction.apply(promisedValue, promisedProblem));
        } catch (Throwable th) {
          resultPromise.setException(th);
        }
      }));
      return resultPromise.getFuture();
    }

    protected void chooseDispatch(Quantum task) {
      if (state == PromiseState.FULFILLED) {
        AsyncEngine.getLocalEngine().schedule(task);
      } else if (firstWaitingTask != null) {
        if (otherWaitingTasks == null) {
          otherWaitingTasks = new ArrayList<>();
        }
        otherWaitingTasks.add(task);
      } else {
        firstWaitingTask = task;
      }
    }
  }
}
