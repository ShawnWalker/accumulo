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

public interface AsyncScheduler {
  /**
   * Configurable priority and other scheduling parameters for a task. Implementations of {@link AsyncScheduler} should subclass {@code Priority} to parse any
   * options into a form better suited to efficient scheduling.
   */
  public static class Priority {
    public final ImmutableMap<String,String> options;

    Priority(ImmutableMap<String,String> options) {
      this.options = options;
    }

    @Override
    public int hashCode() {
      return options.hashCode();
    }

    @Override
    public boolean equals(Object rhsObject) {
      if (!(rhsObject instanceof Priority)) {
        return false;
      } else {
        return options.equals(((Priority) rhsObject).options);
      }
    }
  }

  /** A unit of work with corresponding priority. */
  public static class Quantum {
    public final Priority priority;
    public final Runnable r;

    Quantum(Priority priority, Runnable r) {
      this.priority = priority;
      this.r = r;
    }
  }

  /** Create a new {@code Priority} from a map of options. */
  default public Priority initialize(ImmutableMap<String,String> options) {
    return new Priority(options);
  }

  /** Return the next {@link Quantum} to be executed. */
  public Quantum poll();

  /** Add the specified quantum to the scheduler's queues. */
  public void enqueue(Quantum work);
}
