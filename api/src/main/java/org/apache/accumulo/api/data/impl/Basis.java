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

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

/** Internal detail of defining keys; represents type information of a single field in the key. */
public interface Basis {
  /** Prototype of a range set of elements for this field; must be nonempty. */
  public RangeSet setPrototype();
  
  /** Type of the set type. */
  public default Class<? extends RangeSet> setType() {
    return setPrototype().getClass();
  }  
  
  /** Construct an instance of the empty range set. */
  public default RangeSet emptySet() {
    return setPrototype().construct(Collections.emptySortedSet());
  }
  
  /** Construct an instance of the complete range set. */
  public default RangeSet fullSet() {
    return setPrototype().construct(new TreeSet(Collections.singleton(order().minimumValue())));
  }
  
  /** Prototype of a nonempty range of elements in this field. */
  public default Range rangePrototype() {
    return (Range)setPrototype().iterator().next();
  }
    
  /** Range type for this field. */
  public default Class<? extends Range> rangeType() {
    return rangePrototype().getClass();
  }
  
  /** Prototype of an element of this field. */
  public default Object fieldPrototype() {
    return rangePrototype().getLowerBound();
  }
  
  /** Type of field. */
  public default Class<?> fieldType() {
    return fieldPrototype().getClass();
  }
  
  /** Order type for this field. */
  public default SuccessorOrder order() {
    return setPrototype().order();
  }
  
  /** Construct a set from a range. */
  public default RangeSet constructSet(Range r) {
    SortedSet ss=new TreeSet(order());
    if (r.isEmpty()) {
      return setPrototype().construct(ss);
    }
    ss.add(r.getLowerBound());
    if (r.getUpperBound()==null) {
      return setPrototype().construct(ss);
    }
    ss.add(r.getUpperBound());
    return setPrototype().construct(ss);
  }
  
  /** Exemplar of an empty range. */
  public default Range emptyRange() {
    return rangePrototype().construct(order().minimumValue(), order().minimumValue());
  }

  /** Exemplar of the full range. */
  public default Range<?, ?> fullRange() {
    return rangePrototype().construct(order().minimumValue(), null);
  }

  /** Ensure that the collection of fields matches the type described by the basis. */
  public static boolean isValid(Basis[] basis, Object[] fields) {
    if (basis.length != fields.length) {
      return false;
    }
    for (int i = 0; i < basis.length; ++i) {
      if (!basis[i].fieldType().isAssignableFrom(fields[i].getClass())) {
        return false;
      }
    }
    return true;
  }

  /** Ensure that the collection of fields matches the type described by the basis. */
  public static boolean isValid(Basis[] basis, Range[] ranges) {
    if (basis.length != ranges.length) {
      return false;
    }
    for (int i = 0; i < basis.length; ++i) {
      if (!basis[i].rangeType().isAssignableFrom(ranges[i].getClass())) {
        return false;
      }
    }
    return true;
  }
  
}
