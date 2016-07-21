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
package org.apache.accumulo.foundation.inject;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.matcher.Matchers;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import com.google.inject.util.Modules;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Builder for an injector which respects {@code @Requires} annotations and allows substitution.
 */
public class InjectorBuilder {
  private final Set<Class<? extends Module>> specifiedModules;
  private final Set<Class<? extends Module>> removedModules;
  private final List<Module> additionalInstances;

  private InjectorBuilder(Injector baseInjector) {
    this.specifiedModules = new HashSet<>();
    this.removedModules = new HashSet<>();
    this.additionalInstances = new ArrayList<>();
  }

  /** Construct a new root injector which shares no common functionality with any other preexisting Injector. */
  public static InjectorBuilder newRoot() {
    return new InjectorBuilder(null).add(InjectModule.class);
  }

  /** Add specific module instances to the {@code InjectorBuilder}. */
  public InjectorBuilder addRaw(Module rawModule) {
    this.additionalInstances.add(rawModule);
    return this;
  }

  /** Add modules to the builder for inclusion in the eventual {@link Injector}. */
  public InjectorBuilder add(Class<? extends Module> module) {
    specifiedModules.add(module);
    return this;
  }

  /** Create a binding for a specific instance. */
  public <T> InjectorBuilder bindInstance(Key<T> key, T instance) {
    return addRaw(new AbstractModule() {
      @Override
      protected void configure() {
        bind(key).toInstance(instance);
      }
    });
  }

  /** Create a binding for a specific instance. */
  public <T> InjectorBuilder bindInstance(Class<T> clazz, T instance) {
    return bindInstance(Key.get(clazz), instance);
  }

  /**
   * Specify a module substitution. When the {@link Injector} is built, instead of instantiating the {@code target} module, instead insert the
   * {@code substitution} module.
   */
  public InjectorBuilder substitute(Class<? extends Module> target, Module substitution) {
    this.removedModules.add(target);
    return addRaw(substitution);
  }

  /** Create an {@link Injector} with the given modules. */
  public Injector build() {
    return build(Stage.PRODUCTION);
  }

  /** Create an {@link Injector} with the given modules. */
  public Injector build(Stage stage) {
    return Guice.createInjector(stage, instantiateModules());
  }

  /**
   * From this {@code InjectorBuilder}, construct the collection of {@link Module}s which would become part of the built Injector. This can be useful if one
   * wishes to use e.g. {@link com.google.inject.util.Modules#override}.
   */
  public List<Module> instantiateModules() {
    Set<Class<? extends Module>> closure = new HashSet<>(new TransitiveClosureSet(specifiedModules));
    ImmutableList.Builder<Module> instances = ImmutableList.builder();

    for (Class<? extends Module> m : closure) {
      if (removedModules.contains(m)) {
        continue;
      }
      try {
        instances.add(m.newInstance());
      } catch (InstantiationException | IllegalAccessException ex) {
        throw new IllegalStateException("Failed to instantiate " + m.getSimpleName(), ex);
      }
    }
    instances.addAll(additionalInstances);

    return instances.build();
  }

  /**
   * Combine the functionality of {@link com.google.inject.util.Modules#override} and {@link BoundFieldModule} to ease testing.
   */
  public Injector buildTestInjector(Object testClassInstance, Module... addlModules) {
    List<Module> overrides = new ArrayList<>();
    overrides.add(BoundFieldModule.of(testClassInstance));
    overrides.addAll(Arrays.asList(addlModules));
    Injector injector = Guice.createInjector(Stage.PRODUCTION, Modules.override(instantiateModules()).with(overrides));
    injector.injectMembers(testClassInstance);
    return injector;
  }

  /**
   * Basic functionality we wish all Injectors to have.
   */
  protected static class InjectModule extends AbstractModule {
    @Override
    protected void configure() {
      binder().requireExplicitBindings();
      binder().disableCircularProxies();
      bind(LifecycleManager.class).to(LifecycleManagerImpl.class);
      LifecycleManagerImpl managerImpl = new LifecycleManagerImpl();
      bind(LifecycleManagerImpl.class).toInstance(managerImpl);
      bindListener(Matchers.any(), managerImpl);
    }
  }

  /** Follow {@code @Requires} annotations, build the transitive closure of the needed modules. */
  protected final class TransitiveClosureSet extends AbstractSet<Class<? extends Module>> {
    private final Set<Class<? extends Module>> closureSet = new HashSet<>();

    public TransitiveClosureSet() {}

    public TransitiveClosureSet(Collection<Class<? extends Module>> modules) {
      addAll(modules);
    }

    /**
     * Add a module type and all of its transitive dependencies to the set.
     */
    @Override
    public boolean add(Class<? extends Module> newModuleType) {
      if (!closureSet.add(newModuleType)) {
        return false;
      }

      // Recursively build dependencies
      Requires reqs = newModuleType.getAnnotation(Requires.class);
      if (reqs != null) {
        addAll(Arrays.asList(reqs.value()));
      }
      return true;
    }

    @Override
    public Iterator<Class<? extends Module>> iterator() {
      return Collections.unmodifiableSet(closureSet).iterator();
    }

    @Override
    public int size() {
      return closureSet.size();
    }
  }
}
