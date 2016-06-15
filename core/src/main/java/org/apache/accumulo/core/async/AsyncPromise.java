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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A writable, single assignment container for passing values. */
public class AsyncPromise<T> {
  private static final Logger log=LoggerFactory.getLogger(AsyncPromise.class);
  
  private final DelayTolerance promiseTolerance;
  
  private static enum PromiseState {
    UNFULFILLED,
    FULFILLED,
    AWAITING_FUTURE
  };
  private PromiseState state=PromiseState.UNFULFILLED;  
  private T promisedValue;
  private Throwable promisedProblem;
  private EventSink firstWaitingTask;
  private EventSink secondWaitingTask;
  private List<EventSink> otherWaitingTasks;
  
  public AsyncPromise() {
    this(AsyncEngine.get().jobTolerance());
  }
  
  public AsyncPromise(DelayTolerance promiseTolerance) {
    this.promiseTolerance=promiseTolerance;
  }
  
    /** Return a future to receive data from this promise. */
  public AsyncFuture<T> getFuture() {
    return new PromisedFuture(promiseTolerance);
  }
  
  /** 
   * Set the promise to {@code value}, and schedule all follow-on actions.  After {@code setValue(...)} has been called, 
   * any subsequent calls to {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be
   * ignored.
   */
  public void setValue(T value) {
    if (state==PromiseState.UNFULFILLED) {
      state=PromiseState.FULFILLED;
      this.promisedValue=value;
      dispatch();
    }
  }

  /** 
   * Set the promise to {@code problem}, and schedule all follow-on actions.  After {@code setError(...)} has been called, 
   * any subsequent calls to {@code setValue(...)}, {@code setError(...)}, or {@code setFuture(...)} will silently be
   * ignored.
   */
  public void setError(Throwable problem) {
    if (state==PromiseState.UNFULFILLED) {
      state=PromiseState.FULFILLED;
      this.promisedProblem=problem;
      dispatch();
    }
  }

  /** 
   * Have this promise follow the state of the specified future, triggering follow-on actions when the future
   * becomes available.  After {@code setError(...)} has been called, any subsequent calls to {@code setValue(...)}, 
   * {@code setError(...)}, or {@code setFuture(...)} will silently be ignored.
   */
  public void setFuture(AsyncFuture<? extends T> future) {
    if (state==PromiseState.UNFULFILLED) {
      state=PromiseState.AWAITING_FUTURE;
      future.escalateTo(promiseTolerance).handle((value, problem) -> {
        this.promisedValue=value;
        this.promisedProblem=problem;
        state=PromiseState.FULFILLED;
        dispatch();
        return null;
      });
    }
  }
  
  /** Upon receipt of an event, send it to all of our sinks. */
  protected void dispatch() {
    AsyncEngine engine=AsyncEngine.get();
    if (firstWaitingTask!=null) {
      engine.schedule(firstWaitingTask.tolerance, firstWaitingTask);
    }
    if (secondWaitingTask!=null) {
      engine.schedule(secondWaitingTask.tolerance, secondWaitingTask);
    }
    if (otherWaitingTasks!=null) {
      for (EventSink task:otherWaitingTasks) {
        engine.schedule(task.tolerance, task);
      }
    }
  }
      
  private abstract class EventSink implements Runnable {
    private final DelayTolerance tolerance;
    EventSink(DelayTolerance tolerance) {
      this.tolerance=tolerance;
    }
  }
  
  private class PromisedFuture implements AsyncFuture<T> {
    private final DelayTolerance tolerance;
    
    PromisedFuture(DelayTolerance tolerance) {
      this.tolerance=tolerance;
    }
    
    @Override
    public AsyncFuture<T> escalateTo(DelayTolerance maxTolerance) {
      if (tolerance.compareTo(maxTolerance)<0) {
        return this;
      } else {
        return new PromisedFuture(maxTolerance);
      }
    }

    @Override
    public <U> AsyncFuture<U> then(AsyncReaction<? super T, ? extends U> reaction) {
      if (state==PromiseState.FULFILLED && promisedProblem!=null) {
        return (AsyncFuture)this;
      }
      AsyncPromise<U> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          if (promisedProblem!=null) {
            resultPromise.setError(promisedProblem);
          } else {
            try {
              resultPromise.setValue(reaction.apply(promisedValue));
            } catch (Throwable th) {
              resultPromise.setError(th);
            }
          }
        }
      });
      return resultPromise.getFuture();
    }

    @Override
    public <U> AsyncFuture<U> thenSchedule(AsyncReaction<? super T, ? extends AsyncFuture<? extends U>> reaction) {
      if (state==PromiseState.FULFILLED && promisedProblem!=null) {
        return (AsyncFuture)this;
      }
      AsyncPromise<U> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          if (promisedProblem!=null) {
            resultPromise.setError(promisedProblem);
          } else {
            try {
              resultPromise.setFuture(reaction.apply(promisedValue));
            } catch (Throwable th) {
              resultPromise.setError(th);
            }
          }
        }
      });
      return resultPromise.getFuture();      
    }

    @Override
    public AsyncFuture<T> except(AsyncReaction<Throwable, ? extends T> reaction) {
      if (state==PromiseState.FULFILLED && promisedProblem==null) {
        return this;
      }
      AsyncPromise<T> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          if (promisedProblem==null) {
            resultPromise.setValue(promisedValue);
          } else {
            try {
              resultPromise.setValue(reaction.apply(promisedProblem));
            } catch (Throwable th) {
              resultPromise.setError(th);
            }
          }
        }
      });
      return resultPromise.getFuture();
    }

    @Override
    public AsyncFuture<T> exceptSchedule(AsyncReaction<Throwable, ? extends AsyncFuture<? extends T>> reaction) {
      if (state==PromiseState.FULFILLED && promisedProblem==null) {
        return this;
      }
      AsyncPromise<T> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          if (promisedProblem==null) {
            resultPromise.setValue(promisedValue);
          } else {
            try {
              resultPromise.setFuture(reaction.apply(promisedProblem));
            } catch (Throwable th) {
              resultPromise.setError(th);
            }
          }
        }
      });
      return resultPromise.getFuture();
    }
    
    
    @Override
    public <U> AsyncFuture<U> handle(AsyncHandler<? super T, ? extends U> reaction) {
      AsyncPromise<U> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          try {
              resultPromise.setValue(reaction.apply(promisedValue, promisedProblem));
          } catch (Throwable th) {
            resultPromise.setError(th);
          }
        }
      });
      return resultPromise.getFuture();      
    }

    @Override
    public <U> AsyncFuture<U> handleSchedule(AsyncHandler<? super T, ? extends AsyncFuture<? extends U>> reaction) {
      AsyncPromise<U> resultPromise=new AsyncPromise<>();
      chooseDispatch(new EventSink(tolerance) {
        @Override
        public void run() {
          try {
              resultPromise.setFuture(reaction.apply(promisedValue, promisedProblem));
          } catch (Throwable th) {
            resultPromise.setError(th);
          }
        }
      });
      return resultPromise.getFuture();      
    }

    protected void chooseDispatch(EventSink task) {
      if (state==PromiseState.FULFILLED) {
        AsyncEngine.get().schedule(task.tolerance, task);
      } else if (secondWaitingTask!=null) {
        otherWaitingTasks.add(task);
      } else if (firstWaitingTask!=null) {
        secondWaitingTask=task;
        otherWaitingTasks=new ArrayList<>();
      } else {
        firstWaitingTask=task;
      }
    }    
  }
}
