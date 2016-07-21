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

import java.util.Arrays;
import java.util.Objects;
import org.apache.accumulo.api.data.Key.Dimension;

/** Utility class for constructing instances of {@link Key.Set}. */
public class Query {
  /** Scan over only those keys matching all conditions. */
  public static Key.Set and(Iterable<Key.Set> keySets) {
    Key.Set result=Key.Set.ALL;
    for (Key.Set keySet:keySets) {
      result=result.intersectionWith(keySet);
    }
    return result;
  }
  
  /** Scan over only those keys matching all conditions. */
  public static Key.Set and(Key.Set... keySets) {
    return and(Arrays.asList(keySets));
  }
  
  /** Scan over those keys matching any conditions. */
  public static Key.Set or(Iterable<Key.Set> keySets) {
    Key.Set result=Key.Set.EMPTY;
    for (Key.Set keySet:keySets) {
      result=result.unionWith(keySet);
    }
    return result;
  }
  
  /** Scan over those keys matching any conditions. */
  public static Key.Set or(Key.Set... keySets) {
    return or(Arrays.asList(keySets));
  }
  
  /** 
   * Return the inverse of the specified set.  Note that the implementation of this is very expensive, and so should
   * be used sparingly.
   */
  public static Key.Set not(Key.Set keySet) {
    return keySet.complement();
  }
  
  /** Construct a cylinder; that is, an object who's projection onto all axes except the specified axis is full. */
  protected static Key.Set cylinder(Dimension dim, Range<?,?,?> range) {
    Range[] projections=Arrays.copyOf(Key.Box.ALL.projections, Key.Box.ALL.projections.length);
    projections[dim.ordinal()]=range;
    return new Key.Box(projections).asSet();
  }
  
  public static final BytesQuery ROW=new BytesQuery(Dimension.ROW);
  public static final BytesQuery FAMILY=new BytesQuery(Dimension.FAMILY);
  public static final BytesQuery QUALIFIER=new BytesQuery(Dimension.QUALIFIER);
  public static final BytesQuery VISIBILITY=new BytesQuery(Dimension.VISIBILITY);
  public static final RangeQuery<Long, Timestamp.Range> TIMESTAMP=new RangeQuery<Long, Timestamp.Range>(Timestamp.ORDER) {
    @Override
    protected Key.Set cylinder(Long begin, Long end) {
      return Query.cylinder(Dimension.TIMESTAMP, new Timestamp.Range(begin, end));
    }
  };
  public static final RangeQuery<Boolean, DeletionMarker.Range> DELETION=new RangeQuery<Boolean, DeletionMarker.Range>(DeletionMarker.ORDER) {
    @Override
    protected Key.Set cylinder(Boolean begin, Boolean end) {
      return Query.cylinder(Dimension.DELETION, new DeletionMarker.Range(begin, end));
    }
  };
          
  static abstract class RangeQuery<T, RangeType extends Range<T,?,?>> {
    private final SuccessorOrder<T> order;
    RangeQuery(SuccessorOrder<T> order) {
      this.order=order;
    }
    /** Construct a cylinder in the appropriate dimension on the given range endpoints. */
    protected abstract Key.Set cylinder(T begin, T end);
    
    /** The specified field exactly equals {@code value}. */
    public Key.Set equalTo(T value) {
      return cylinder(Objects.requireNonNull(value), order.successor(value));
    }
    
    /** The specified field exactly equals {@code value}. */
    public Key.Set eq(T value) {return equalTo(value);}

    /** The specified field is smaller than {@code value}. */
    public Key.Set lessThan(T value) {
      return cylinder(order.minimumValue(), Objects.requireNonNull(value));
    }
    
    public Key.Set lt(T value) {return lessThan(value);}
    
    public Key.Set lessThanOrEqualTo(T value) {
      return cylinder(order.minimumValue(), order.successor(Objects.requireNonNull(value)));
    }
    
    public Key.Set lte(T value) {return lessThanOrEqualTo(value);}
    
    public Key.Set greaterThan(T value) {
      return cylinder(Objects.requireNonNull(order.successor(value)), null);
    }
    
    public Key.Set gt(T value) {return greaterThan(value);}
    
    public Key.Set greaterThanOrEqualTo(T value) {
      return cylinder(Objects.requireNonNull(order.successor(value)), null);
    }
    public Key.Set gte(T value) {return greaterThanOrEqualTo(value);};
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThan(upperBound))}. */
    public Key.Set strictlyBetween(T lowerBound, T upperBound) {
      return cylinder(Objects.requireNonNull(order.successor(lowerBound)), upperBound);
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public Key.Set within(T lowerBound, T upperBound) {
      return cylinder(lowerBound, order.successor(Objects.requireNonNull(upperBound)));
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThan(upperBound))}. */
    public Key.Set closedOpen(T lowerBound, T upperBound) {
      return cylinder(Objects.requireNonNull(lowerBound), Objects.requireNonNull(upperBound));
    }
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public Key.Set openClosed(T lowerBound, T upperBound) {
      return cylinder(Objects.requireNonNull(order.successor(lowerBound)), order.successor(Objects.requireNonNull(upperBound)));
    }
  }
  
  public static class BytesQuery extends RangeQuery<Bytes, Bytes.Range> {
    private final Dimension dim;
    BytesQuery(Dimension dim) {
      super(Bytes.ORDER);
      this.dim=dim;
    }
    
    @Override
    protected Key.Set cylinder(Bytes begin, Bytes end) {
      return Query.cylinder(dim, new Bytes.Range(begin, end));
    }
    
    /** Calculate the {@link Bytes} which is just larger than all byte strings starting with {@code prefix}. */
    private static Bytes prefixEnd(Bytes prefix) {
      byte[] work=prefix.toByteArray();
      int endPos;
      for (endPos=work.length;endPos>0;--endPos) {
        if (work[endPos-1]!=-1) {
          ++work[endPos];
          break;
        }
      }
      return endPos==0?null:Bytes.wrap(work, 0, endPos);
    }
    public Key.Set startsWith(Bytes prefix) {
      return cylinder(Objects.requireNonNull(prefix), prefixEnd(prefix));
    }
  }
}
