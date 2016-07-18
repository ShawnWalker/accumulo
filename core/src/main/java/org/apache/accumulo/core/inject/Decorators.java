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

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scope;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.ScopedBindingBuilder;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.inject.Provider;
import javax.inject.Qualifier;

/**
 * Utility for building decorator chains.
 *
 * General use:
 *
 * Start with objects which implement an interface (say, {@code Foo}), and also inject an instance of that instance with the {@link Decoratee} annotation:
 *
 * {@code public class FooDecorator implements Foo
 *
 * @Inject @Decoratee Foo chainNext; ... } }
 *
 *  Then in the modules, establish the chain:
 *
 * {@code install(DecoratorModuleBuilder.of(Foo.class).buildChain(FooBase.class, FooDecorator1.class, FooDecorator2.class)); }
 *
 */
public class Decorators<T> {
  private final Key<T> targetKey;

  protected Decorators(Key<T> targetKey) {
    this.targetKey = targetKey;
  }

  public static <T> Decorators<T> of(Key<T> targetKey) {
    return new Decorators<>(targetKey);
  }

  public static <T> Decorators<T> of(TypeLiteral<T> typeLiteral) {
    return new Decorators<>(Key.get(typeLiteral));
  }

  public static <T> Decorators<T> of(Class<T> clazz) {
    return new Decorators<>(Key.get(clazz));
  }

  /** Set the base object of the decorator chain being built. */
  public DecoratorChainBuilder setBase(Class<? extends T> clazz) {
    return setBase(Key.get(clazz));
  }

  /** Set the base object of the decorator chain being built. */
  public DecoratorChainBuilder setBase(TypeLiteral<? extends T> typeLiteral) {
    return setBase(Key.get(typeLiteral));
  }

  /** Set the base class of the decorator chain being built. */
  public DecoratorChainBuilder setBase(Key<? extends T> baseKey) {
    return new DecoratorChainBuilder(baseKey);
  }

  public class DecoratorChainBuilder {
    private final Key<? extends T> base;
    private final List<TypeLiteral<? extends T>> decorators = new ArrayList<>();

    DecoratorChainBuilder(Key<? extends T> base) {
      this.base = base;
    }

    /** Add another decorator onto the decorator chain. */
    public DecoratorChainBuilder decorateWith(TypeLiteral<? extends T> decorator) {
      this.decorators.add(decorator);
      return this;
    }

    /** Add another decorator onto the decorator chain. */
    public DecoratorChainBuilder decorateWith(Class<? extends T> decorator) {
      return DecoratorChainBuilder.this.decorateWith(TypeLiteral.get(decorator));
    }

    /** Create a module which builds the decorator chain. */
    public Module build() {
      return new DecoratorModule(base, decorators, (ScopedBindingBuilder sbb) -> {});
    }

    /** Create a module which builds the decorator chain, specifying a scope for the final built object. */
    public Module buildIn(Class<? extends Annotation> scopeAnnotation) {
      return new DecoratorModule(base, decorators, (ScopedBindingBuilder sbb) -> {
        sbb.in(scopeAnnotation);
      });
    }

    /** Create a module which builds the decorator chain, specifying a scope for the final built object. */
    public Module buildIn(Scope scope) {
      return new DecoratorModule(base, decorators, (ScopedBindingBuilder sbb) -> {
        sbb.in(scope);
      });
    }

    /** Create a module which builds the decorator chain, specifying that the final object should be built as an eager singleton. */
    public Module buildAsEagerSingleton() {
      return new DecoratorModule(base, decorators, (ScopedBindingBuilder sbb) -> {
        sbb.asEagerSingleton();
      });
    }
  }

  private class DecoratorModule extends AbstractModule {
    private final Key<? extends T> base;
    private final List<TypeLiteral<? extends T>> decorators;
    private final Consumer<ScopedBindingBuilder> scoper;

    private DecoratorModule(Key<? extends T> base, List<TypeLiteral<? extends T>> decorators, Consumer<ScopedBindingBuilder> scoper) {
      this.base = base;
      this.decorators = decorators;
      this.scoper = scoper;
    }

    @Override
    protected void configure() {
      Provider<Injector> injectorProvider = getProvider(Injector.class);
      Provider<? extends T> chainSoFar = getProvider(base);
      for (TypeLiteral<? extends T> decorator : decorators) {
        chainSoFar = new DecoratorProvider<>(targetKey, injectorProvider, chainSoFar, decorator);
      }
      scoper.accept(bind(targetKey).toProvider(chainSoFar));
    }
  }

  private static class DecoratorProvider<T> implements Provider<T> {
    private final Key<T> targetKey;
    private final Provider<Injector> injectorProvider;
    private final Provider<? extends T> decoratee;
    private final TypeLiteral<? extends T> implType;
    private Provider<T> decoratorProvider;

    public DecoratorProvider(Key<T> targetKey, Provider<Injector> injectorProvider, Provider<? extends T> decoratee, TypeLiteral<? extends T> implType) {
      this.targetKey = targetKey;
      this.injectorProvider = injectorProvider;
      this.decoratee = decoratee;
      this.implType = implType;
    }

    @Override
    synchronized public T get() {
      if (this.decoratorProvider == null) {
        Key decoratorKey = Key.get(implType, Decorator.class);
        Injector childInjector = injectorProvider.get().createChildInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(targetKey.getTypeLiteral()).annotatedWith(Decoratee.class).toProvider(decoratee);
            bind(decoratorKey).to(implType);
          }
        });
        decoratorProvider = childInjector.getProvider(decoratorKey);
      }
      return decoratorProvider.get();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.CONSTRUCTOR)
    @Qualifier
    private static @interface Decorator {}
  }
}
