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

import com.google.inject.TypeLiteral;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import java.lang.annotation.Annotation;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Arranges to run all @PostConstruct and @PreDestroy methods.
 */
public class LifecycleManagerImpl implements TypeListener, LifecycleManager {
  private final WeakHashMap<Object,PredestroyTracker> remainingInstances = new WeakHashMap<>();
  private final AtomicLong instanceCounter = new AtomicLong();

  @Override
  public Collection<Exception> shutdown() {
    List<PredestroyTracker> shutdownItems = new ArrayList<>();
    synchronized (remainingInstances) {
      shutdownItems.addAll(remainingInstances.values());
    }
    Collections.sort(shutdownItems);

    return shutdownItems.stream().flatMap(sdi -> sdi.destroy()).collect(Collectors.toList());
  }

  @Override
  public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> encounter) {
    Collection<Method> pcMethods = getMethodsWithAnnotation(typeLiteral, PostConstruct.class, encounter);
    if (!pcMethods.isEmpty()) {
      encounter.register(new PostConstructCaller<>(typeLiteral, pcMethods, encounter));
    }

    Collection<Method> pdMethods = getMethodsWithAnnotation(typeLiteral, PreDestroy.class, encounter);
    if (!pdMethods.isEmpty()) {
      encounter.register(new PreDestroyListener<>(typeLiteral, pdMethods, encounter));
    }
  }

  /**
   * Find all declared methods of {@code rawType} which possess the annotation {@code ann}. Filter out methods requiring parameters.
   */
  private static <I> Collection<Method> getMethodsWithAnnotation(TypeLiteral<I> typeLiteral, Class<? extends Annotation> ann, TypeEncounter<I> errorReporter) {
    List<Method> methods = new ArrayList<>();
    for (Class<?> considerClass = typeLiteral.getRawType(); considerClass != Object.class; considerClass = considerClass.getSuperclass()) {
      for (Method method : considerClass.getDeclaredMethods()) {
        if (method.isAnnotationPresent(ann)) {
          if (method.getParameterCount() != 0) {
            errorReporter.addError("@%s method %s.%s must take 0 arguments", ann.getSimpleName(), typeLiteral.toString(), method.getName());
          }
          methods.add(method);
        }
      }
    }
    return methods;
  }

  /** An {@link InjectionListener} which calls all {@code @PostConstruct} methods on instances it encounters. */
  protected static class PostConstructCaller<T> implements InjectionListener<T> {
    private final TypeLiteral<T> typeLiteral;
    private final Collection<Method> pcMethods;

    PostConstructCaller(TypeLiteral<T> typeLiteral, Collection<Method> pcMethods, TypeEncounter<T> encounter) {
      this.typeLiteral = typeLiteral;
      this.pcMethods = pcMethods;
      for (Method m : pcMethods) {
        try {
          m.setAccessible(true);
        } catch (Exception ex) {
          encounter.addError(ex);
        }
      }
    }

    @Override
    public void afterInjection(T injectee) {
      for (Method m : pcMethods) {
        try {
          m.invoke(injectee);
        } catch (Exception ex) {
          throw new IllegalStateException(String.format("Call of @PostConstruct on %s.%s failed", typeLiteral.toString(), m.getName()), ex);
        }
      }
    }
  }

  /** Information about an instance which we still expect to destroy. */
  protected static class PredestroyTracker implements Comparable<PredestroyTracker> {
    private final Collection<Method> pdMethods;
    private final WeakReference<Object> instanceRef;
    private final long instanceId;

    PredestroyTracker(Collection<Method> pdMethods, long instanceId, Object instance) {
      this.pdMethods = pdMethods;
      this.instanceId = instanceId;
      this.instanceRef = new WeakReference<>(instance);
    }

    public Stream<Exception> destroy() {
      Object instance = instanceRef.get();
      if (instance == null)
        return Stream.empty();
      return pdMethods.stream().flatMap(method -> {
        try {
          method.invoke(instance);
          return Stream.empty();
        } catch (Exception ex) {
          return Stream.of(ex);
        }
      });
    }

    @Override
    public int compareTo(PredestroyTracker o) {
      // Compare in reverse order of creation.
      return Long.compare(o.instanceId, this.instanceId);
    }
  }

  /**
   * An {@link InjectionListener} which registers all injected objects with {@code @PreDestroy} methods.
   */
  protected class PreDestroyListener<T> implements InjectionListener<T> {
    private final TypeLiteral<T> typeLiteral;
    private final Collection<Method> pdMethods;

    PreDestroyListener(TypeLiteral<T> typeLiteral, Collection<Method> pdMethods, TypeEncounter<T> errorReporter) {
      this.typeLiteral = typeLiteral;
      this.pdMethods = pdMethods;
      for (Method m : pdMethods) {
        try {
          m.setAccessible(true);
        } catch (Exception ex) {
          errorReporter.addError("Failed to set method %s.%s accessible in PreDestroyListener: %s", typeLiteral, m.getName(), ex);
        }
      }
    }

    @Override
    public void afterInjection(T injectee) {
      synchronized (remainingInstances) {
        remainingInstances.put(injectee, new PredestroyTracker(pdMethods, instanceCounter.getAndIncrement(), injectee));
      }
    }
  }
}
