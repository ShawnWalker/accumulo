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
package org.apache.accumulo.api.data;

import java.util.SortedSet;

public class DeletionMarker {
  public static final Class<?> TYPE=Boolean.class;
  public static final SuccessorOrder<Boolean> ORDER = new SuccessorOrder<Boolean>() {
    @Override
    public Boolean successor(Boolean instance) {
      return instance ? null : false;
    }

    @Override
    public Boolean minimumValue() {
      return true;
    }

    @Override
    public Boolean maximumValue() {
      return false;
    }

    @Override
    public int compare(Boolean o1, Boolean o2) {
      return o1.equals(o2) ? 0 : o1 ? -1 : 1;
    }

    @Override
    public boolean isFinite(Boolean begin, Boolean end) {
      return begin.equals(end)?true:begin;
    }
  };
  
  public static class Range extends org.apache.accumulo.api.data.Range<Boolean, Range, RangeSet> {
    public static final Range EMPTY=new Range(true, true);
    public static final Range ALL=new Range(true, null);

    Range(Boolean begin, Boolean end) {
      super(begin, end, ORDER);
    }

    @Override
    protected Range construct(Boolean lowerBound, Boolean upperBound) {
      return new Range(lowerBound, upperBound);
    }

    @Override
    protected RangeSet constructSet(SortedSet<Boolean> breakPoints) {
      return new RangeSet(breakPoints);
    }
  }
  
  public static class RangeSet extends org.apache.accumulo.api.data.RangeSet<Boolean, Range, RangeSet> {
    
    RangeSet(SortedSet<Boolean> breakPoints) {
      super(breakPoints, DeletionMarker.ORDER);
    }
    
    @Override
    protected RangeSet construct(SortedSet<Boolean> breakPoints) {
      return new RangeSet(breakPoints);
    }

    @Override
    protected Range constructRange(Boolean lowerBound, Boolean upperBound) {
      return new Range(lowerBound, upperBound);
    }
  }
}
