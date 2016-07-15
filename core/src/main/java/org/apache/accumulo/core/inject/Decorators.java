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
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.ScopedBindingBuilder;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.inject.Provider;

/** 
 * Utility for building decorator chains.
 * 
 * General use:
 * 
 * Start with objects which implement an interface (say, {@code Foo}), and also inject an instance of that
 * instance with the {@link Decoratee} annotation:
 * 
 * {@code
 * public class FooDecorator implements Foo {
 *   @Inject @Decoratee Foo chainNext;
 *   ...
 * }
 * }
 * 
 * Then in the modules, establish the chain:
 * 
 * {@code
 * public class FooModule extends AbstractModule {
 *   @Override
 *   protected void configure() {
 *     Decorators.bind(Foo.class)
 *       .toChain(BaseFooImpl.class, FirstFooDecorator.class, SecondFooDecorator.class, ...)
 *       .in(Singleton.class);
 *   }
 * }
 */
public class Decorators {
  public static <T> DecoratorBindingBuilder<T> bind(Binder binder, Class<T> clazz) {
    return bind(binder, Key.get(clazz));
  }
  public static <T> DecoratorBindingBuilder<T> bind(Binder binder, TypeLiteral<T> typeLiteral) {
    return bind(binder, Key.get(typeLiteral));
  }
  public static <T> DecoratorBindingBuilder<T> bind(Binder binder, Key<T> targetKey) {
    return new DecoratorBindingBuilder<>(binder, targetKey);
  }
  
  public static class DecoratorBindingBuilder<T> {
    private final Binder binder;
    private final Key<T> targetKey;
    
    DecoratorBindingBuilder(Binder binder, Key<T> targetKey) {
      this.binder=binder;
      this.targetKey=targetKey;
    }
    
    public ScopedBindingBuilder toChain(Class<? extends T> base, Class<? extends T>... decorators) {
      Key<? extends T> baseKey=Key.get(base);
      return toChain(baseKey, decorators);
    }

    public ScopedBindingBuilder toChain(Key<? extends T> base, Class<? extends T>... decorators) {
      TypeLiteral<? extends T>[] decoratorTypes=new TypeLiteral[decorators.length];
      for (int i=0;i<decorators.length;++i) {
        decoratorTypes[i]=TypeLiteral.get(decorators[i]);
      }
      return toChain(base, decoratorTypes);
    }

    public ScopedBindingBuilder toChain(Key<? extends T> base, TypeLiteral<? extends T>... decorators) {
      Provider<Injector> injectorProvider=binder.getProvider(Injector.class);
      Provider<? extends T> chainSoFar=binder.getProvider(base);
      for (TypeLiteral<? extends T> decorator:decorators) {
        chainSoFar=new DecoratorProvider<>(injectorProvider, targetKey, chainSoFar, decorator);
      }
      return binder.bind(targetKey).toProvider(chainSoFar);
    }
  }
  
  private static class DecoratorProvider<T> implements Provider<T> {
    private final Provider<Injector> injectorProvider;
    private final Key<T> target;
    private final Provider<? extends T> decoratee;
    private final TypeLiteral<? extends T> implType;
    private Provider<T> decoratorProvider;

    public DecoratorProvider(Provider<Injector> injectorProvider, Key<T> target, Provider<? extends T> decoratee, TypeLiteral<? extends T> implType) {
      this.injectorProvider=injectorProvider;
      this.target = target;
      this.decoratee=decoratee;
      this.implType=implType;
    }

    @Override
    synchronized public T get() {
      if (this.decoratorProvider==null) {
        Key decoratorKey=Key.get(implType, Decorator.class);
        Injector childInjector=injectorProvider.get().createChildInjector(new AbstractModule() {
          @Override
          protected void configure() {
            bind(target.getTypeLiteral()).annotatedWith(DecorateeImpl.forKey(target)).toProvider(decoratee);
            bind(decoratorKey).to(implType);
          }
        });
        decoratorProvider=childInjector.getProvider(decoratorKey);
      }
      return decoratorProvider.get();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.CONSTRUCTOR)
    @BindingAnnotation
    private static @interface Decorator {
    }

    static final class DecorateeImpl implements Decoratee {

      private final Class<? extends Annotation> bindingAnnotation;

      public DecorateeImpl(Class<? extends Annotation> bindingAnnotation) {
        super();
        this.bindingAnnotation = bindingAnnotation;
      }

      @Override
      public Class<? extends Annotation> value() {
        return bindingAnnotation;
      }

      @Override
      public Class<? extends Annotation> annotationType() {
        return Decoratee.class;
      }

      @Override
      public boolean equals(Object rhsObject) {
        if (!(rhsObject instanceof Decoratee)) {
          return false;
        }
        return value().equals(((Decoratee) rhsObject).value());
      }

      @Override
      public int hashCode() {
        return (127 * "value".hashCode()) ^ value().hashCode();
      }

      public static Decoratee forKey(Key k) {
        Class<? extends Annotation> annotationType = k.getAnnotationType();
        return new DecorateeImpl(annotationType == null ? NoBindingAnnotation.class : annotationType);
      }

      @Override
      public String toString() {
        return "@"+Decoratee.class.getSimpleName()+"("+value()+")";
      }
    }
  }
}
