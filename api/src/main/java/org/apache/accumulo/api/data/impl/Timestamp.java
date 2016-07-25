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

public class Timestamp {
  public static final Class<?> TYPE=Long.class;
  
  public static final SuccessorOrder<Long> ORDER = new SuccessorOrder<Long>() {
    @Override
    public Long successor(Long instance) {
      return instance == Long.MIN_VALUE ? null : instance - 1;
    }

    @Override
    public Long minimumValue() {
      return Long.MAX_VALUE;
    }

    @Override
    public Long maximumValue() {
      return Long.MIN_VALUE;
    }

    @Override
    public boolean isFinite(Long begin, Long end) {
      return true;
    }
    
    @Override
    public int compare(Long o1, Long o2) {
      return -Long.compare(o1, o2);
    }
  };

  public static class Range extends org.apache.accumulo.api.data.impl.Range<Long, Range, RangeSet> {
    public static final Range EMPTY = new Range(Long.MAX_VALUE, Long.MAX_VALUE);
    public static final Range ALL = new Range(Long.MAX_VALUE, null);

    public Range(Long lowerBound, Long upperBound) {
      super(lowerBound, upperBound, ORDER);
    }

    @Override
    protected Range construct(Long lowerBound, Long upperBound) {
      return new Range(lowerBound, upperBound);
    }
    
    @Override
    protected RangeSet constructSet(SortedSet<Long> breakPoints) {
      return new RangeSet(breakPoints);
    }
  }

  public static class RangeSet extends org.apache.accumulo.api.data.impl.RangeSet<Long, Range, RangeSet> {
    public static final RangeSet EMPTY=new RangeSet(Collections.emptySortedSet());
    public static final RangeSet ALL=new RangeSet(new TreeSet<>(Collections.singleton(ORDER.minimumValue())));
    
    protected RangeSet(SortedSet<Long> breakPoints) {
      super(breakPoints, Timestamp.ORDER);
    }

    @Override
    protected RangeSet construct(SortedSet<Long> breakPoints) {
      return new RangeSet(breakPoints);
    }

    @Override
    protected Range constructRange(Long lowerBound, Long upperBound) {
      return new Range(lowerBound, upperBound);
    }
  }
  
}
