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

import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class Range<T, Impl extends Range<T, Impl, SetType>, SetType extends RangeSet<T, Impl, SetType>> implements Iterable<T> {
  private final SuccessorOrder<T> baseOrder;
  private final SuccessorOrder<T> order;
  protected final T lowerBound;
  protected final T upperBound;
  
  protected Range(T lowerBound, T upperBound, SuccessorOrder<T> order) {
    this.baseOrder=order;
    this.order=SuccessorOrder.nullsLast(order);
    this.lowerBound=Objects.requireNonNull(lowerBound);
    this.upperBound=upperBound;
    if (this.order.compare(lowerBound, upperBound)>0) {
      throw new IllegalArgumentException("lowerBound larger than upperBound while constructing Range");
    }
  }
  
  /** Implemented by subclass.  Construct an instance of the subclass. */
  protected abstract Impl construct(T lowerBound, T upperBound);
  
  /** Implemented by subclass.  Construct a set from this range. */
  protected abstract SetType constructSet(SortedSet<T> breakPoints);
  
  public SetType asSet() {
    SortedSet<T> breakSet=new TreeSet<>(order());
    if (isEmpty()) return constructSet(breakSet);
    
    breakSet.add(getLowerBound());
    if (getUpperBound()!=null) {
      breakSet.add(getUpperBound());
    }
    return constructSet(breakSet);
  }
  
  /** Return the underlying order on this range. */
  public SuccessorOrder<T> order() {
    return baseOrder;
  }
  
  /** Calculate the intersection of two ranges. */
  public Impl intersectionWith(Impl other) {
    T newLowerBound=order.compare(this.lowerBound, other.lowerBound)<0?other.lowerBound:this.lowerBound;
    T newUpperBound=order.compare(this.upperBound, other.upperBound)<0?this.upperBound:other.upperBound;
    return order.compare(newLowerBound, newUpperBound)<=0?construct(newLowerBound, newUpperBound):construct(lowerBound, lowerBound);
  }
  
  /** Calculate the range containing two ranges. */
  public Impl boundWith(Impl other) {
    if (this.isEmpty()) return other;
    if (other.isEmpty()) return (Impl)this;
    T newLowerBound=order.compare(this.lowerBound, other.lowerBound)<0?this.lowerBound:other.lowerBound;
    T newUpperBound=order.compare(this.upperBound, other.upperBound)<0?other.upperBound:this.upperBound;
    return construct(newLowerBound, newUpperBound);
  }
  
  /** True if this range contains {@code point}. */
  public boolean contains(T point) {
    return order.compare(lowerBound, point)<=0 && order.compare(point, upperBound)<0;
  }
  
  /** True if this set is empty. */
  public boolean isEmpty() {
    return order.compare(lowerBound, upperBound)==0;
  }
  
  /** Retrieve the lower bound of this range. */
  public T getLowerBound() {
    return lowerBound;
  }
  
  /** Retrieve the upper bound of this range, or {@code null} if the range is unbounded. */
  public T getUpperBound() {
    return upperBound;
  }
  
  /** True if this set contains finitely many members. */
  public boolean isFinite() {
    return order.isFinite(lowerBound, upperBound);
  }
  
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private T nextItem=lowerBound;

      @Override
      public boolean hasNext() {
        return order.compare(nextItem, upperBound)<0;
      }

      @Override
      public T next() {
        T result=nextItem;
        nextItem=order.successor(nextItem);
        return result;
      }
    };
  }
  
  
  @Override
  public boolean equals(Object rhsObject) {
    if (getClass()!=rhsObject.getClass()) {
      return false;
    }
    Range<T, Impl, SetType> rhs=(Range<T, Impl, SetType>)rhsObject;
    return (this.isEmpty() && rhs.isEmpty()) || 
            (order.compare(this.lowerBound, rhs.lowerBound)==0 && order.compare(this.upperBound, rhs.upperBound)==0);
  }
  
  @Override
  public String toString() {
    if (isEmpty()) {
      return "[)";
    }
    return "["+lowerBound.toString()+", "+(Objects.equals(upperBound, order.maximumValue())?"+ifty":upperBound.toString())+")";
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(lowerBound, upperBound);
  }
}
