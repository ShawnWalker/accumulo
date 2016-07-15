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
import com.google.inject.Key;
import javax.inject.Inject;

/** As a kludge to support porting, provide static access to the injector. */
public class StaticFactory {
  @Inject
  private static Injector injector;
  
  public static <T> T getInstance(Key<T> key) {
    if (injector==null) {
      throw new IllegalStateException("Injector not yet initialized");
    }
    return injector.getInstance(key);
  }
    
  public static <T> T getInstance(Class<T> clazz) {
    return getInstance(Key.get(clazz));
  }
}
