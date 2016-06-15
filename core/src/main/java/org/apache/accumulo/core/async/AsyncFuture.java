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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

/** A not-thread-safe, asynchronously delayed result. */
public interface AsyncFuture<T> {
  /** Construct a new future with {@link DelayTolerance} which is the lower of this future's tolerance and the
   * specified tolerance.
   */
  public AsyncFuture<T> escalateTo(DelayTolerance maxTolerance);
  
  /** Schedule an action to be executed when the result from this future is available (with value). */
  public <U> AsyncFuture<U> then(AsyncReaction<? super T, ? extends U> reaction);
  
  /** Schedule an action to be executed when the result from this future is available (with value). */
  default public <U> AsyncFuture<U> then(Callable<? extends U> reaction) {return this.<U>then(dummy -> reaction.call());}
    
  /** Schedule an action to be executed when the result from this future is available (with value). */
  public <U> AsyncFuture<U> thenSchedule(AsyncReaction<? super T, ? extends AsyncFuture<? extends U>> reaction);
  
  /** Schedule an action to be executed when the the result from this future is available (with exception). */
  public AsyncFuture<T> except(AsyncReaction<Throwable, ? extends T> reaction);
  
  /** Schedule an action to be executed when the result from this future is available (with exception). */
  public AsyncFuture<T> exceptSchedule(AsyncReaction<Throwable, ? extends AsyncFuture<? extends T>> reaction);
  
  /** Schedule an action to be executed when the result from this future is available (with value or exception). */
  public <U> AsyncFuture<U> handle(AsyncHandler<? super T, ? extends U> reaction);
  
  /** Schedule an action to be executed when the result from this future is available (with value or exception). */
  public <U> AsyncFuture<U> handleSchedule(AsyncHandler<? super T, ? extends AsyncFuture<? extends U>> reaction);
  
  /** Return a future which is availabile immediately with the specified value. */
  public static <T> AsyncFuture<T> immediate(T value) {
    AsyncPromise<T> promise=new AsyncPromise<>();
    promise.setValue(value);
    return promise.getFuture();
  }
  
  /** Return a future which is available immediately with the specified error. */
  public static <T> AsyncFuture<T> immediateError(Throwable problem) {
    AsyncPromise<T> promise=new AsyncPromise<>();
    promise.setError(problem);
    return promise.getFuture();
  }
  
  /** Return a future which becomes available once any of the specified futures become available. */
  public static <T> AsyncFuture<T> anyOf(Collection<AsyncFuture<T>> futures) {
    AsyncPromise<T> resultPromise=new AsyncPromise<>();
    futures.forEach(future -> resultPromise.setFuture(future));
    return resultPromise.getFuture();
  }
  
  /** Return a future which becomes available once any of the specified futures become available. */
  public static <T> AsyncFuture<T> anyOf(AsyncFuture<T>... futures) {
    return anyOf(Arrays.asList(futures));
  }
  
  /** Return a future which becomes available once all of the specified futures become available. */
  public static <T> AsyncFuture<List<T>> allAsList(Collection<AsyncFuture<T>> futures) {
    AsyncPromise<List<T>> resultPromise=new AsyncPromise();
    Object[] resultArray=new Object[futures.size()];
    int[] remainingCount=new int[]{resultArray.length};
    int pos=0;
    Iterator<AsyncFuture<T>> it=futures.iterator();
    while (it.hasNext()) {
      final int curPos=pos;
      it.next().handle((value, problem) -> {
        if (problem==null) {
          resultArray[curPos]=value;
          remainingCount[0]--;
          if (remainingCount[0]==0) {
            resultPromise.setValue(new ArrayList(Arrays.asList(resultArray)));
          }
        } else {
          resultPromise.setError(problem);
        }
        return null;
      });
      ++pos;
    }
    return resultPromise.getFuture();
  }
}
