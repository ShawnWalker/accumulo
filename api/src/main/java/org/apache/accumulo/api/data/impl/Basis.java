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
package org.apache.accumulo.api.data.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/** Internal detail of defining keys; represents type information of a single field in the key. */
public interface Basis {

  /** Ordering of the field. */
  public SuccessorOrder order();

  /** Type of a range of elements of this field. */
  public Class<? extends Range> rangeType();

  /** Type of the field. */
  public default Class<?> type() {
    ParameterizedType pt = (ParameterizedType) rangeType().getGenericSuperclass();
    Type[] params = pt.getActualTypeArguments();
    return (Class<?>) params[0];
  }

  /** Exemplar of an empty range. */
  public default Range<?, ?, ?> emptyRange() {
    try {
      return rangeType().getDeclaredConstructor(type(), type()).newInstance(order().minimumValue(), order().minimumValue());
    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Exemplar of the full range. */
  public default Range<?, ?, ?> fullRange() {
    try {
      return rangeType().getDeclaredConstructor(type(), type()).newInstance(order().minimumValue(), null);
    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Ensure that the collection of fields matches the type described by the basis. */
  public static void validate(Basis[] basis, Object[] fields) throws IllegalArgumentException {
    if (basis.length != fields.length) {
      throw new IllegalArgumentException("Field count mismatch; expected " + basis.length + " fields, found " + fields.length);
    }
    for (int i = 0; i < basis.length; ++i) {
      if (!basis[i].type().isAssignableFrom(fields[i].getClass())) {
        throw new IllegalArgumentException("Field type mismatch in position " + i + "; expected " + basis[i].type().getSimpleName() + ", found " + fields[i].getClass().getSimpleName());
      }
    }
  }

  /** Ensure that the collection of fields matches the type described by the basis. */
  public static void validate(Basis[] basis, Range[] ranges) throws IllegalArgumentException {
    if (basis.length != ranges.length) {
      throw new IllegalArgumentException("Field count mismatch; expected " + basis.length + " fields, found " + ranges.length);
    }
    for (int i = 0; i < basis.length; ++i) {
      if (!basis[i].rangeType().isAssignableFrom(ranges[i].getClass())) {
        throw new IllegalArgumentException("Field type mismatch in position " + i + "; expected " + basis[i].type().getSimpleName() + ", found " + ranges[i].getClass().getSimpleName());
      }
    }
  }
  
}
