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
import java.util.Arrays;
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
public class DecoratorModuleBuilder<T> {
  private final Key<T> targetKey;

  protected DecoratorModuleBuilder(Key<T> targetKey) {
    this.targetKey = targetKey;
  }

  public static <T> DecoratorModuleBuilder<T> of(Key<T> targetKey) {
    return new DecoratorModuleBuilder<>(targetKey);
  }

  public static <T> DecoratorModuleBuilder<T> of(TypeLiteral<T> typeLiteral) {
    return new DecoratorModuleBuilder<>(Key.get(typeLiteral));
  }

  public static <T> DecoratorModuleBuilder<T> of(Class<T> clazz) {
    return new DecoratorModuleBuilder<>(Key.get(clazz));
  }

  public DecoratorModule buildChain(Class<? extends T> base, Class<? extends T>... decorators) {
    Key<? extends T> baseKey = Key.get(base);
    return DecoratorModuleBuilder.this.buildChain(baseKey, decorators);
  }

  public DecoratorModule buildChain(Key<? extends T> base, Class<? extends T>... decorators) {
    TypeLiteral<? extends T>[] decoratorTypes = new TypeLiteral[decorators.length];
    for (int i = 0; i < decorators.length; ++i) {
      decoratorTypes[i] = TypeLiteral.get(decorators[i]);
    }
    return buildChain(base, decoratorTypes);
  }

  public DecoratorModule buildChain(Key<? extends T> base, TypeLiteral<? extends T>... decorators) {
    return new DecoratorModule(base, Arrays.asList(decorators));
  }

  public class DecoratorModule extends AbstractModule {
    private final Key<? extends T> base;
    private final List<TypeLiteral<? extends T>> decorators;
    private Consumer<ScopedBindingBuilder> scoper = (ScopedBindingBuilder sbb) -> {};

    private DecoratorModule(Key<? extends T> base, List<TypeLiteral<? extends T>> decorators) {
      this.base = base;
      this.decorators = decorators;
    }

    @Override
    protected void configure() {
      Provider<Injector> injectorProvider = getProvider(Injector.class);
      Provider<? extends T> chainSoFar = getProvider(base);
      for (TypeLiteral<? extends T> decorator : decorators) {
        chainSoFar = new DecoratorProvider<T>(targetKey, injectorProvider, chainSoFar, decorator);
      }
      scoper.accept(bind(targetKey).toProvider(chainSoFar));
    }

    public Module in(Class<? extends Annotation> scopeAnnotation) {
      scoper = sbb -> sbb.in(scopeAnnotation);
      return this;
    }

    public Module in(Scope scope) {
      scoper = sbb -> sbb.in(scope);
      return this;
    }

    public Module asEagerSingleton() {
      scoper = sbb -> sbb.asEagerSingleton();
      return this;
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
