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

import java.util.Arrays;

/** A tuple of a collection of types, in dictionary order. This is the abstract implementation of {@link Key}. */
public abstract class Tuple<Impl extends Tuple<Impl>> implements Comparable<Impl> {
  protected final Basis[] basis;
  protected final Object[] fields;

  protected Tuple(Basis[] basis, Object[] fields) {
    super();
    Basis.validate(basis, fields);
    this.basis = basis;
    this.fields = fields;
  }

  /** Construct an instance of the implementation given the fields. */
  protected abstract Impl construct(Object[] fields);

  protected Object get(int index) {
    return fields[index];
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (!(rhsObject instanceof Tuple)) {
      return false;
    }
    Tuple rhs = (Tuple) rhsObject;
    return basis == rhs.basis && Arrays.equals(fields, rhs.fields);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(fields);
  }

  protected static <Impl extends Tuple<Impl>> SuccessorOrder<Impl> order(final Impl prototype) {
    // Precalculate min/max value.
    Object[] minValueFields = new Object[prototype.basis.length];
    for (int i = 0; i < prototype.basis.length; ++i) {
      minValueFields[i] = prototype.basis[i].order().minimumValue();
    }
    final Impl minValue = prototype.construct(minValueFields);
    Object[] maxValueFields = new Object[prototype.basis.length];
    for (int i = 0; i < prototype.basis.length; ++i) {
      Object fieldMax = prototype.basis[i].order().maximumValue();
      if (fieldMax == null) {
        maxValueFields = null;
        break;
      }
      maxValueFields[i] = fieldMax;
    }
    final Impl maxValue = maxValueFields == null ? null : prototype.construct(maxValueFields);
    return new SuccessorOrder<Impl>() {
      @Override
      public Impl minimumValue() {
        return minValue;
      }

      @Override
      public Impl maximumValue() {
        return maxValue;
      }

      @Override
      public Impl successor(Impl instance) {
        Object[] newFields = Arrays.copyOf(instance.fields, instance.fields.length);
        for (int i = prototype.basis.length - 1; i >= 0; --i) {
          if (prototype.basis[i].order().maximumValue() == null || !newFields[i].equals(prototype.basis[i].order().maximumValue())) {
            newFields[i] = prototype.basis[i].order().successor(newFields[i]);
            return prototype.construct(newFields);
          }
          newFields[i] = prototype.basis[i].order().minimumValue();
        }
        return null;
      }

      @Override
      public boolean isFinite(Impl begin, Impl end) {
        for (int i = prototype.basis.length - 1; i >= 0; --i) {
          if (!prototype.basis[i].order().isFinite(begin.fields[i], end.fields[i])) {
            return false;
          }
          if (prototype.basis[i].order().maximumValue() == null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public int compare(Impl o1, Impl o2) {
        return o1.compareTo(o2);
      }
    };
  }

  @Override
  public int compareTo(Impl other) {
    for (int i = 0; i < basis.length; ++i) {
      int cmp;
      if ((cmp = basis[i].order().compare(fields[i], other.fields[i])) != 0) {
        return cmp;
      }
    }
    return 0;
  }
  
}
