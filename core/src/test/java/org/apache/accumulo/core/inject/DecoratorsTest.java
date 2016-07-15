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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.junit.Assert;
import org.junit.Test;

public class DecoratorsTest {
  public static interface Appender {
    public void append(StringBuilder sb);
  }
  
  @Singleton
  public static class AppendADecorator implements Appender {
    @Inject
    @Decoratee Appender decoratee;
            
    @Override
    public void append(StringBuilder sb) {
      decoratee.append(sb);
      sb.append('a');
    }
  }
  
  @Singleton
  public static class AppendBDecorator implements Appender {
    private final Appender decoratee;
    
    @Inject
    AppendBDecorator(@Decoratee Appender decoratee) {
      this.decoratee=decoratee;
    }

    @Override
    public void append(StringBuilder sb) {
      decoratee.append(sb);
      sb.append('b');
    }
  }
  
  @Singleton
  public static class BaseAppender implements Appender {
    @Override
    public void append(StringBuilder sb) {
    }    
  }
    
  @Test
  public void buildTest() {
    Injector injector=Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(Appender.class).annotatedWith(Names.named("foo")).to(BaseAppender.class);
        Decorators.bind(binder(), Appender.class)
                .toChain(Key.get(Appender.class, Names.named("foo")),
                        AppendADecorator.class,
                        AppendBDecorator.class,
                        AppendADecorator.class,
                        AppendADecorator.class)
                .in(Singleton.class);
      }
    });
    
    Appender app=injector.getInstance(Appender.class);
    
    StringBuilder sb=new StringBuilder();
    app.append(sb);
    Assert.assertEquals("abaa", sb.toString());
    
    Appender app2=injector.getInstance(Appender.class);
    Assert.assertTrue(app==app2);
  }
}
