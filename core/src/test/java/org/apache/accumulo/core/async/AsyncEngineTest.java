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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncEngineTest {
  static AsyncEngine engine;
  
  @BeforeClass
  public static void initialize() {
    engine=new AsyncEngine();
    engine.start();
  }
  
  @AfterClass
  public static void teardown() {
    engine.interrupt();
  }
  
  /** Do nothing for a while. */
  static AsyncFuture<Void> sleep(long duration, TimeUnit unit) {
    AsyncPromise<Void> resultPromise=new AsyncPromise<>();
    long startNanos=System.nanoTime();
    AsyncEngine.get().addPoll(()->{
      if (System.nanoTime()>startNanos+TimeUnit.NANOSECONDS.convert(duration, unit)) {
        resultPromise.setValue(null);
        return false;
      } else {
        return true;
      }
    });
    return resultPromise.getFuture();
  }
  
  @Test
  public void testSleep() throws Exception {    
    Future<Void> result=engine.submit(DelayTolerance.TOLERANT, () -> {
      System.out.println("Sleeping for 5 sec");
      return sleep(5, TimeUnit.SECONDS).then( () -> {
        System.out.println("Wait complete, shutting down");
        return null;
      });
    });
    result.get();
  }
  
  @Test
  public void testErrorPropagation() throws Exception {
    try {
      engine.submit(DelayTolerance.TOLERANT, () -> AsyncFuture.<Void>immediateError(new RuntimeException())).get();
      Assert.fail();
    } catch (ExecutionException ex) {
      Assert.assertEquals(RuntimeException.class, ex.getCause().getClass());
    }
  }
  
  @Test
  public void testMultiplePromise() throws Exception {
    Future<Void> result=engine.submit(DelayTolerance.TOLERANT, () -> {
      AsyncPromise<Integer> promise=new AsyncPromise<>();
      AsyncFuture<Void> jobFuture=promise.getFuture().then((Integer v)->{
        Assert.assertEquals(0, (int)v);
        return null;
      });
      promise.setValue(0);
      promise.setValue(1);
      return jobFuture;
    });
    result.get();
  }
}
