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
import java.util.Collections;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/** Building block for {@link KeySet}, represents a box in the product space.  Totally ordered in the lexicographical ordering on sets. */
public abstract class ProductBox<ElemImpl extends Tuple<ElemImpl>, BoxImpl extends ProductBox<ElemImpl, BoxImpl, SetImpl>, SetImpl extends ProductBoxSet<ElemImpl, BoxImpl, SetImpl>> {

  protected final Basis[] basis;
  protected final Range[] projections;

  protected ProductBox(Basis[] basis, Range[] projections) {
    super();
    this.basis = basis;
    this.projections = projections;
    Basis.validate(basis, projections);
  }

  /** Implemented by subclass.  Construct an element instance. */
  protected abstract ElemImpl constructTuple(Object[] fields);

  /** Implemented by subclass.  Construct an instance of the subclass. */
  protected abstract BoxImpl constructBox(Range[] projections);

  /** Implemented by subclass.  Construct a set with the specified boxes. */
  protected abstract SetImpl constructSet(Set<BoxImpl> boxes);

  /** Return a view of this {@code Box} as a {@link #BoxSet} */
  public SetImpl asSet() {
    if (isEmpty()) {
      return constructSet(Collections.<BoxImpl>emptySet());
    } else {
      return constructSet(Collections.singleton((BoxImpl) this));
    }
  }

  /** True if this box contains the specified element. */
  public boolean contains(ElemImpl element) {
    for (int i = 0; i < projections.length; ++i) {
      if (!projections[i].contains(element.get(i))) {
        return false;
      }
    }
    return true;
  }

  /** Return the first (minimum) element in this box, or throws {@link NoSuchElementException.} */
  public ElemImpl first() {
    if (isEmpty()) {
      throw new NoSuchElementException();
    }
    Object[] fields = new Object[basis.length];
    for (int i = 0; i < basis.length; ++i) {
      fields[i] = projections[i].getLowerBound();
    }
    return constructTuple(fields);
  }

  /**
   * Return the smallest element in the box no smaller than {@code point}, or {@code null} if there is
   * no such point.
   */
  public ElemImpl nextSeek(ElemImpl point) {
    if (isEmpty()) {
      return null;
    }
    Object[] fields = new Object[basis.length];
    // Find the first dimension where the point falls outside our box's projection.
    int badDim;
    for (badDim = 0; badDim < basis.length; ++badDim) {
      fields[badDim] = point.get(badDim);
      if (!projections[badDim].contains(point.get(badDim))) {
        break;
      }
    }
    // All remaining dimensions start at the beginning.
    for (int i = badDim + 1; i < basis.length; ++i) {
      fields[i] = projections[i].getLowerBound();
    }
    // In the first bad dimension, the easy case is that we're too early
    if (basis[badDim].order().compare(fields[badDim], projections[badDim].getLowerBound()) < 0) {
      fields[badDim] = projections[badDim].getLowerBound();
      return constructTuple(fields);
    }
    // Otherwise, we've got to increase the previous dimension.  But that might push out out of the
    // projection in that dimension as well, and so we'd need to propagate up even further.
    for (int j = badDim - 1; j >= 0; --j) {
      Object dimSucc = basis[j].order().successor(fields[j]);
      if (dimSucc == null) {
        // We're at the maximum element in this dimension, so propagate up.
        fields[j] = projections[j].getLowerBound();
        continue;
      } else {
        fields[j] = dimSucc;
      }
      if (projections[j].contains(fields[j])) {
        // Took successor, still in box.
        return constructTuple(fields);
      } else {
        // Moved out of box, propagate back.
        fields[j] = projections[j].getLowerBound();
      }
    }
    // Propagation went past the beginning dimension; there's no more room to advance, so we're done.
    return null;
  }

  /** True if this box is empty. */
  public boolean isEmpty() {
    for (int i = 0; i < projections.length; ++i) {
      if (projections[i].isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** Calculate the projection of this box onto the {@link index} axis. */
  Range projectTo(int index) {
    if (isEmpty()) {
      return basis[index].emptyRange();
    } else {
      return projections[index];
    }
  }

  /** Calculate the intersection of two boxes. */
  public BoxImpl intersectionWith(BoxImpl other) {
    Range[] ranges = new Range[basis.length];
    for (int i = 0; i < basis.length; ++i) {
      ranges[i] = projections[i].intersectionWith(other.projections[i]);
    }
    return constructBox(ranges);
  }

  /**
   * Calculate the complement of this box.  This is moderately expensive, as the complement of a box may take up to
   * 2*(basis.length) boxes to represent.
   */
  public SetImpl complement() {
    if (isEmpty()) {
      Range[] ranges = new Range[basis.length];
      for (int i = 0; i < ranges.length; ++i) {
        ranges[i] = basis[i].fullRange();
      }
      return constructBox(ranges).asSet();
    }
    Set<BoxImpl> complBoxes = new HashSet<>();
    for (int i = 0; i < basis.length; ++i) {
      RangeSet projComplement = projections[i].asSet().complement();
      for (Range r : (Iterable<Range>) projComplement) {
        Range[] newProjections = new Range[basis.length];
        for (int j = 0; j < basis.length; ++j) {
          if (i == j) {
            newProjections[j] = r;
          } else {
            newProjections[j] = basis[j].fullRange();
          }
        }
        complBoxes.add(constructBox(newProjections));
      }
    }
    // Take us out.
    complBoxes.remove((BoxImpl) this);
    return constructSet(complBoxes);
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (rhsObject == null || rhsObject.getClass() != getClass()) {
      return false;
    }
    BoxImpl rhs = (BoxImpl) rhsObject;
    if (basis != rhs.basis) {
      return false;
    }
    if (isEmpty()) {
      return rhs.isEmpty();
    }
    for (int i = 0; i < projections.length; ++i) {
      if (!projections[i].equals(rhs.projections[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(projections);
  }
  
}
