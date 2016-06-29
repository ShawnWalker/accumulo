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

import com.google.inject.Injector;
import java.util.Collection;

/**
 * The {@code LifecycleManager} allows one to invoke {@code @PreDestroy} methods on instances created by an {@link Injector}. This allows one to release
 * resources of long-lived objects in preparation for process shutdown.
 */
public interface LifecycleManager {

  /** Call all {@code @PreDestroy} methods of injected objects. Return any exceptions raised. */
  public Collection<Exception> shutdown();

  /** Call all {@code @PreDestroy} methods of injected objects. Return any exceptions raised. */
  public static Collection<Exception> shutdown(Injector injector) {
    return injector.getInstance(LifecycleManager.class).shutdown();
  }
}
