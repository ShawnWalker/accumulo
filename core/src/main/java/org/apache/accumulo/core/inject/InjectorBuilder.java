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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static java.util.stream.Collectors.toList;

/**
 * Builder for an injector which respects {@code @Requires} annotations and allows substitution.
 */
public class InjectorBuilder {
  private final Set<Class<? extends Module>> specifiedModules;
  private final Set<Class<? extends Module>> removedModules;

  private final List<Module> additionalInstances;
  private final Map<Class<? extends Module>,Class<? extends Module>> requestedOverrides;

  private InjectorBuilder() {
    this.specifiedModules = new HashSet<>();
    this.removedModules = new HashSet<>();
    this.additionalInstances = new ArrayList<>();
    this.requestedOverrides = new HashMap<>();
  }

  /** Construct a new root injector which shares no common functionality with any other preexisting Injector. */
  public static InjectorBuilder newRoot() {
    return new InjectorBuilder().add(InjectModule.class);
  }

  /** Add specific module instances to the {@code InjectorBuilder}. */
  public InjectorBuilder addInstances(Collection<? extends Module> rawModules) {
    this.additionalInstances.addAll(rawModules);
    return this;
  }

  /** Add specific module instances to the {@code InjectorBuilder}. */
  public InjectorBuilder add(Module... rawModules) {
    return addInstances(Arrays.asList(rawModules));
  }

  /** Add modules to the builder for inclusion in the eventual {@link Injector}. */
  public InjectorBuilder add(Collection<Class<? extends Module>> modules) {
    specifiedModules.addAll(modules);
    return this;
  }

  /** Add modules to the builder for inclusion in the eventual {@link Injector}. */
  public InjectorBuilder add(Class<? extends Module>... modules) {
    return add(Arrays.asList(modules));
  }

  /** Remove modules from the builder. This is done after transitive closure is calculated. */
  public InjectorBuilder remove(Collection<Class<? extends Module>> modules) {
    this.removedModules.addAll(modules);
    return this;
  }

  /** Remove modules from the builder. This is done after transitive closure is calculated. */
  public InjectorBuilder remove(Class<? extends Module>... modules) {
    return remove(Arrays.asList(modules));
  }

  /**
   * Specify a module override. When the {@link Injector} is built, we will not include the {@code target} module, instead including the {@code replacement}
   * module. This is done before transitive closure is calculated.
   */
  public InjectorBuilder override(Class<? extends Module> target, Class<? extends Module> replacement) {
    requestedOverrides.put(target, replacement);
    return this;
  }

  /**
   * Specify a module substitution. When the {@link Injector} is built, instead of instantiating the {@code target} module, instead insert the
   * {@code substituation} module.
   */
  public InjectorBuilder substitute(Class<? extends Module> target, Module substitution) {
    return remove(target).add(substitution);
  }

  /** Create an {@link Injector} with the given modules. */
  public Injector build() {
    return build(Stage.PRODUCTION);
  }

  /** Create an {@link Injector} with the given modules. */
  public Injector build(Stage stage) {
    List<Module> instances = new ArrayList<>(new ReplacementTransitiveClosureSet(specifiedModules).stream().filter(m -> !removedModules.contains(m)).map(m -> {
      try {
        return m.newInstance();
      } catch (InstantiationException | IllegalAccessException ex) {
        throw new IllegalStateException("Failed to instantiate " + m.getSimpleName(), ex);
      }
    }).collect(toList()));
    instances.addAll(additionalInstances);

    return Guice.createInjector(stage, instances);
  }

  /** For a given module type, calculate the replacement type. */
  protected Class<? extends Module> calculateReplacement(Class<? extends Module> newModuleType) {
    // Resolve overrides
    LinkedHashSet<Class<? extends Module>> consideredReplacements = new LinkedHashSet<>();
    while (requestedOverrides.containsKey(newModuleType)) {
      if (consideredReplacements.contains(newModuleType)) {
        StringBuilder errorBuilder = new StringBuilder();
        errorBuilder.append("Circular replacement chain requested while resolving InjectorBuilder: ");
        for (Class<? extends Module> clazz : consideredReplacements) {
          errorBuilder.append(clazz.getSimpleName());
          errorBuilder.append(" -> ");
        }
        errorBuilder.append(newModuleType.getSimpleName());
        throw new IllegalStateException(errorBuilder.toString());
      }
      consideredReplacements.add(newModuleType);
      newModuleType = requestedOverrides.get(newModuleType);
    }

    return newModuleType;
  }

  /** Follow {@code @Requires} annotations, build the transitive closure of the needed modules. */
  protected final class ReplacementTransitiveClosureSet extends AbstractSet<Class<? extends Module>> {
    private final Set<Class<? extends Module>> closureSet = new HashSet<>();

    public ReplacementTransitiveClosureSet() {}

    public ReplacementTransitiveClosureSet(Collection<Class<? extends Module>> modules) {
      addAll(modules);
    }

    /**
     * Add a module type and all of its transitive dependencies to the set. This also tracks and properly performs module replacements.
     */
    @Override
    public boolean add(Class<? extends Module> newModuleType) {
      newModuleType = calculateReplacement(newModuleType);

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
