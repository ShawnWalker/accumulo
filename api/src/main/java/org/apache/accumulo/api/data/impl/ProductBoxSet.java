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

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/** Abstract implementation of {@link KeySet}, as the union of a collection of boxes. */
public abstract class ProductBoxSet<ElemImpl extends Tuple<ElemImpl>, BoxImpl extends ProductBox<ElemImpl, BoxImpl, SetImpl>, SetImpl extends ProductBoxSet<ElemImpl, BoxImpl, SetImpl>> {

  protected final Basis[] basis;
  protected final Set<BoxImpl> boxes;

  protected ProductBoxSet(Basis[] basis, Set<BoxImpl> boxes) {
    this.basis = basis;
    for (BoxImpl box : boxes) {
      if (box.isEmpty()) {
        throw new IllegalArgumentException("Empty box while constructing BoxSet");
      }
    }
    this.boxes = boxes;
  }

  /** Implemented by subclass.  Construct an instance of the subclass. */
  protected abstract BoxImpl constructBox(Range[] projections);

  /** Implemented by subclass.  Construct a set with the specified boxes. */
  protected abstract SetImpl constructSet(Set<BoxImpl> boxes);

  /** True if the set is empty. */
  public boolean isEmpty() {
    return boxes.isEmpty();
  }

  /** True if the set contains the specified point. */
  public boolean contains(ElemImpl point) {
    for (BoxImpl box : boxes) {
      if (box.contains(point)) {
        return true;
      }
    }
    return false;
  }

  /** Determine the minimal element of this set, or throw {@link NoSuchElementException} if none exists. */
  public ElemImpl first() {
    if (boxes.isEmpty()) {
      throw new NoSuchElementException();
    }
    Iterator<BoxImpl> iter = boxes.iterator();
    ElemImpl smallestFirst = iter.next().first();
    while (iter.hasNext()) {
      ElemImpl boxFirst = iter.next().first();
      if (boxFirst.compareTo(smallestFirst) < 0) {
        smallestFirst = boxFirst;
      }
    }
    return smallestFirst;
  }

  /**
   * Return the smallest element in the box no smaller than {@code point}, or {@code null} if there is
   * no such point.
   */
  public ElemImpl nextSeek(ElemImpl point) {
    // Or equivalently, the minimum of the nextSeek(...) from each of our boxes.
    ElemImpl result = null;
    for (BoxImpl box : boxes) {
      ElemImpl boxPoint = box.nextSeek(point);
      if (boxPoint != null && (result == null || boxPoint.compareTo(result) < 0)) {
        result = boxPoint;
      }
    }
    return result;
  }

  /** Return the complement of this set.  Note: implementation is very expensive. */
  public SetImpl complement() {
    // Start with everything
    Range[] projections = new Range[basis.length];
    for (int i = 0; i < basis.length; ++i) {
      projections[i] = basis[i].fullRange();
    }
    SetImpl current = constructBox(projections).asSet();
    for (BoxImpl box : boxes) {
      current = current.intersectionWith(box.complement());
    }
    return current;
  }

  /** Calculate the union of this set with {@code other}. */
  public SetImpl unionWith(SetImpl other) {
    Set<BoxImpl> newBoxes = new HashSet<>();
    newBoxes.addAll(this.boxes);
    newBoxes.addAll(other.boxes);
    return constructSet(newBoxes);
  }

  /** Calculate the intersection of this set with {@code other}. */
  public SetImpl intersectionWith(SetImpl other) {
    Set<BoxImpl> newBoxes = new HashSet<>();
    for (BoxImpl boxLeft : boxes) {
      for (BoxImpl boxRight : other.boxes) {
        BoxImpl isectBox = boxLeft.intersectionWith(boxRight);
        if (!isectBox.isEmpty()) {
          newBoxes.add(isectBox);
        }
      }
    }
    return constructSet(newBoxes);
  }

  /** Calculate the set difference upon removing {@code other} from {@code this} */
  public SetImpl subtract(SetImpl other) {
    return intersectionWith(other.complement());
  }

  /** Calculate the symmetric difference of this set with {@code other}. */
  public SetImpl symmetricDifferenceWith(SetImpl other) {
    return this.subtract(other).unionWith(other.subtract((SetImpl) this));
  }

  /** Calculate a box which bounds {@code this}. */
  public BoxImpl boundingBox() {
    Range[] ranges = new Range[basis.length];
    for (int i = 0; i < basis.length; ++i) {
      ranges[i] = basis[i].emptyRange();
    }
    for (BoxImpl box : boxes) {
      for (int i = 0; i < basis.length; ++i) {
        ranges[i] = ranges[i].boundWith(box.projectTo(i));
      }
    }
    return constructBox(ranges);
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (rhsObject == null || rhsObject.getClass() != getClass()) {
      return false;
    }
    SetImpl rhs = (SetImpl) rhsObject;
    return this.symmetricDifferenceWith(rhs).isEmpty();
  }

  @Override
  public int hashCode() {
    return boundingBox().hashCode();
  }

  /** Calculate the projection of this BoxSet onto the given axis. */
  protected RangeSet projectTo(int index) {
    if (isEmpty()) {
      return basis[index].emptyRange().asSet();
    }
    Iterator<BoxImpl> iterator = boxes.iterator();
    RangeSet firstProjection = iterator.next().projectTo(index).asSet();
    RangeSet[] projections = new RangeSet[boxes.size() - 1];
    for (int i = 1; i < boxes.size(); ++i) {
      projections[i - 1] = iterator.next().projectTo(index).asSet();
    }
    return firstProjection.unionWith(projections);
  }
  
}
