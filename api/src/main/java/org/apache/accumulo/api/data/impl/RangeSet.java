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
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;

public abstract class RangeSet<T, RangeType extends Range<T, RangeType>, 
        ImplType extends RangeSet<T, RangeType, ImplType>> implements Iterable<RangeType> {
  private final SortedSet<T> breakPoints;
  private final SuccessorOrder<T> order;
  
  protected RangeSet(SuccessorOrder<T> order) {
    this.breakPoints=Collections.emptySortedSet();
    this.order=order;
  }
  
  protected RangeSet(SortedSet<T> breakPoints, SuccessorOrder<T> order) {
    this.breakPoints=Objects.requireNonNull(breakPoints);
    this.order=order;
  }
    
  /** Implemented by subclass.  Construct an instance of the subclass with the given breakpoints. */
  protected abstract ImplType construct(SortedSet<T> breakPoints);
  
  /** Implemented by subclass.  Construct an instance of the range type with the given breakpoints. */
  protected abstract RangeType constructRange(T lowerBound, T upperBound);
  
  public SuccessorOrder<T> order() {
    return order;
  }
  
  //
  // Collection theoretic
  //
  
  /** True if this set is empty. */
  public boolean isEmpty() {
    return breakPoints.isEmpty();
  }
  
  /** True if this set contains {@code point}. */
  public boolean contains(T point) {
    return breakPoints.headSet(point).size()%2==1;
  }
  
  /** True if this set is finite. */
  public boolean isFinite() {
    for (RangeType r:this) {
      if (!r.isFinite()) {
        return false;
      }
    }
    return true;
  }

  /** 
   * Return the first (minimum) element in this set.
   * @throws NoSuchElementException if this set is empty
   */
  public T first() {
    return breakPoints.first();
  }
  
  //
  // Set-theoretic
  //
  
  /** Compute the complement of this range set. */
  public ImplType complement() {
    TreeSet<T> newSet=new TreeSet<>(breakPoints);
    if (newSet.contains(order.minimumValue())) {
      newSet.remove(order.minimumValue());
    } else {
      newSet.add(order.minimumValue());
    }
    return construct(newSet);
  }
  
  /** Compute the union of {@code this} and {@code others}. */
  public ImplType unionWith(ImplType... others) {
    return construct(mergeOp((before,after)->(before==0)!=(after==0), this, others));
  }
  
  /** Compute the intersection of {@code this} and {@code others}. */
  public ImplType intersectionWith(ImplType... others) {
    return construct(mergeOp((before, after)->(before==1+others.length)!=(after==1+others.length), this, others));
  }
  
  /** Compute the symmetric difference of {@code this} and {@code others}. */
  public ImplType symmetricDifferenceWith(ImplType... others) {
    return construct(mergeOp((before, after)->((before+after)%2==1), this, others));
  }
  
  @Override
  public Iterator<RangeType> iterator() {
    return new Iterator<RangeType>() {
      RangeType nextRange;
      Iterator<T> items=breakPoints.iterator();

      {
        calculateNext();
      }

      void calculateNext() {
        if (!items.hasNext()) {
          nextRange=null;
          return;
        }
        T lowerBound=items.next();
        T upperBound=items.hasNext()?items.next():null;
        nextRange=constructRange(lowerBound, upperBound);
      };

      @Override
      public boolean hasNext() {
        return nextRange!=null;
      }

      @Override
      public RangeType next() {
        if (nextRange==null) {
          throw new NoSuchElementException();
        }
        RangeType result=nextRange;
        calculateNext();
        return result;
      }
    };
  }

  /** Create a view of this set allowing iteration over the (possibly infintely many) members of the set. */
  public Iterable<T> elements() {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return new Iterator<T>() {
          T nextElement;
          Iterator<T> elementIterator;
          Iterator<RangeType> rangeIterator=RangeSet.this.iterator();
          {
            calculateNext();
          }
          
          void calculateNext() {
            while (true) {
              if (elementIterator==null || !elementIterator.hasNext()) {
                if (rangeIterator.hasNext()) {
                  elementIterator=rangeIterator.next().iterator();
                } else {
                  nextElement=null;
                  return;
                }
              } else {
                nextElement=elementIterator.next();
                return;
              }
            }
          }
          
          @Override
          public boolean hasNext() {
            return nextElement!=null;
          }

          @Override
          public T next() {
            if (nextElement==null) {
              throw new NoSuchElementException();
            }
            T result=nextElement;
            calculateNext();
            return result;
          }
          
        };
      }
    };
  }
  
  @Override
  public String toString() {
    if (isEmpty()) {
      return "(empty)";
    }
    StringBuilder sb=new StringBuilder();
    for (RangeType r:this) {
      sb.append(r.toString());
    }
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object rhsObject) {
    if (rhsObject==null || getClass()!=rhsObject.getClass()) {
      return false;
    }
    RangeSet rhs=(RangeSet)rhsObject;
    return breakPoints.equals(rhs.breakPoints);
  }
  
  @Override
  public int hashCode() {
    return breakPoints.hashCode();
  }
  
  /** 
   * Add in the forward difference of the indicator function.  The indicator function would return 1 for each element
   * in the set, and 0 for each element not in the set.  Its forward difference finds the edges of this, returning +1
   * whenever there is a transition from absent to present, and -1 whenever there is a transition from present to absent.
   */
  protected void addEdges(SortedMap<T, Integer> counts) {
    int sign=1;
    for (T point:breakPoints) {
      if (!counts.containsKey(point)) {
        counts.put(point, 0);
      }
      counts.put(point, counts.get(point)+sign);
      sign=-sign;
    }
  }
  
  protected static <T> SortedSet<T> accumulate(BiPredicate<Integer, Integer> edgeDetector, SortedMap<T, Integer> breakCounts) {    
    SortedSet<T> result=new TreeSet<>(breakCounts.comparator());
    int accumulator=0;
    for (Map.Entry<T, Integer> entry:breakCounts.entrySet()) {
      int oldAccum=accumulator;
      accumulator+=entry.getValue();
      if (edgeDetector.test(oldAccum, accumulator)) {
        result.add(entry.getKey());
      }
    }
    return result;
  }
  
  /** 
   * Abstract algorithm used by {@link #unionWith(org.apache.accumulo.api.data.AbstractRangeSet...)}, 
   * {@link #intersectionWith(org.apache.accumulo.api.data.AbstractRangeSet...) }
   */
  protected static <T> SortedSet<T> mergeOp(BiPredicate<Integer, Integer> edgeDetector, RangeSet first, RangeSet... others) {
    SortedMap<T, Integer> breakCounts=new TreeMap<>(first.breakPoints.comparator());
    first.addEdges(breakCounts);
    for (RangeSet s:others) {
      s.addEdges(breakCounts);
    }
    return accumulate(edgeDetector, breakCounts);
  }
}
