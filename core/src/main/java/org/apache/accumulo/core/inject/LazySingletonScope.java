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
package org.apache.accumulo.core.inject;

import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Implementation of {@link LazySingleton}. */
public class LazySingletonScope implements Scope {
  private final ConcurrentHashMap<Key,Future<?>> instances = new ConcurrentHashMap<>();

  @Override
  public <T> Provider<T> scope(final Key<T> key, final Provider<T> prvdr) {
    return new Provider<T>() {
      @Override
      public T get() {
        // FIXME: This simplistic implementation risks deadlocks during construction in the face of circular dependencies.
        // It is difficult to avoid this if both @LazySingleton and @Singleton are used in the same project
        SettableFuture<T> sf = SettableFuture.create();
        if (instances.putIfAbsent(key, sf) == sf) {
          // We won the race to create the instance
          try {
            sf.set(prvdr.get());
          } catch (Exception ex) {
            sf.setException(ex);
          }
        }

        try {
          return ((Future<T>) instances.get(key)).get();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted during construction", ie);
        } catch (ExecutionException ex) {
          if (ex.getCause() instanceof RuntimeException) {
            throw ((RuntimeException) ex.getCause());
          } else {
            throw new IllegalStateException(ex);
          }
        }
      }
    };
  }
}
