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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/** Abstract implementation of {@link Key} and {@link Key.Set} */
public class Product {
  /** Internal detail of defining keys; represents type information of a single field in the key. */
  public static interface Basis {

    /** Type of the field. */
    public Class<?> type();

    /** Ordering of the field. */
    public SuccessorOrder order();

    /** Type of a range of elements of this field. */
    public Class<? extends Range> rangeType();

    /** Exemplar of an empty range. */
    public Range<?, ?, ?> emptyRange();

    /** Exemplar of the full range. */
    public Range<?, ?, ?> fullRange();
    
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

  /** Abstract implementation of {@link Key}.  A tuple of a collection of types, in dictionary order. */
  public static abstract class Tuple<Impl extends Tuple<Impl>> implements Comparable<Impl> {

    protected final Product.Basis[] basis;
    protected final Object[] fields;

    Tuple(Product.Basis[] basis, Object[] fields) {
      super();
      Product.Basis.validate(basis, fields);
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

  /** Building block for {@link KeySet}, represents a box in the product space.  Totally ordered in the lexicographical ordering on sets. */
  public static abstract class Box<
          ElemImpl extends Tuple<ElemImpl>, 
          BoxImpl extends Box<ElemImpl, BoxImpl, SetImpl>, 
          SetImpl extends BoxSet<ElemImpl, BoxImpl, SetImpl>> 
  {
    protected final Product.Basis[] basis;
    protected final Range[] projections;

    Box(Product.Basis[] basis, Range[] projections) {
      super();
      this.basis = basis;
      this.projections = projections;
      Product.Basis.validate(basis, projections);
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
        return constructSet(Collections.singleton((BoxImpl)this));
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
    
    /** Calculate the complement of this box.  Warning: this implementation is very expensive. */
    public SetImpl complement() {
      if (isEmpty()) {
        Range[] ranges=new Range[basis.length];
        for (int i=0;i<ranges.length;++i) {
          ranges[i]=basis[i].fullRange();
        }
        return constructBox(ranges).asSet();
      }
      
      // Cut each dimension into up to 3 ranges: range before our projection, our projection, range after our projection.
      List<List<Range>> rangeLists=new ArrayList<>(basis.length);
      for (int i=0;i<basis.length;++i) {
        List<Range> subranges=new ArrayList<>();
        subranges.add(projections[i]);
        
        RangeSet full=basis[i].fullRange().asSet();
        RangeSet taken=projections[i].asSet();
        RangeSet diff=full.symmetricDifferenceWith(taken);
        for (Range r:(Iterable<Range>)diff) {
          subranges.add(r);
        }
        rangeLists.add(subranges);
      }
      
      // Recursively enumerate all combinations of the projections.
      Set<BoxImpl> complBoxes=new HashSet<>();
      buildComplement(complBoxes, rangeLists, new Range[basis.length], 0);
      
      // Take us out.
      complBoxes.remove((BoxImpl)this);
      return constructSet(complBoxes);
    }
    
    private void buildComplement(Set<BoxImpl> built, List<List<Range>> parts, Range[] working, int index) {
      if (index==basis.length) {
        BoxImpl box=constructBox(Arrays.copyOf(working, working.length));
        if (!box.isEmpty()) {
          built.add(box);
        }
      } else {
        List<Range> indexList=parts.get(index);
        for (Range r:indexList) {
          working[index]=r;
          buildComplement(built, parts, working, index+1);
        }
      }
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
  
  /** Abstract implementation of {@link KeySet}, as the union of a collection of boxes. */
  public static abstract class BoxSet<
          ElemImpl extends Tuple<ElemImpl>, 
          BoxImpl extends Box<ElemImpl, BoxImpl, SetImpl>, 
          SetImpl extends BoxSet<ElemImpl, BoxImpl, SetImpl>> {
    protected final Basis[] basis;
    protected final Set<BoxImpl> boxes;
    
    BoxSet(Basis[] basis, Set<BoxImpl> boxes) {
      this.basis=basis;
      for (BoxImpl box:boxes) {
        if (box.isEmpty()) {
          throw new IllegalArgumentException("Empty box while constructing BoxSet");
        }
      }
      this.boxes=boxes;
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
      for (BoxImpl box:boxes) {
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
      Iterator<BoxImpl> iter=boxes.iterator();
      ElemImpl smallestFirst=iter.next().first();
      while (iter.hasNext()) {
        ElemImpl boxFirst=iter.next().first();
        if (boxFirst.compareTo(smallestFirst)<0) {
          smallestFirst=boxFirst;
        }
      }
      return smallestFirst;
    }
    
    /** Return the complement of this set.  Note: implementation is very expensive. */
    public SetImpl complement() {
      // Start with everything
      Range[] projections=new Range[basis.length];
      for (int i=0;i<basis.length;++i) {
        projections[i]=basis[i].fullRange();
      }
      SetImpl current=constructBox(projections).asSet();
      for (BoxImpl box:boxes) {
        current=current.intersectionWith(box.complement());
      }
      return current;
    }
    
    /** Calculate the union of this set with {@code other}. */
    public SetImpl unionWith(SetImpl other) {
      Set<BoxImpl> newBoxes=new HashSet<>();
      newBoxes.addAll(this.boxes);
      newBoxes.addAll(other.boxes);
      return constructSet(newBoxes);
    }
    
    /** Calculate the intersection of this set with {@code other}. */
    public SetImpl intersectionWith(SetImpl other) {
      Set<BoxImpl> newBoxes=new HashSet<>();
      for (BoxImpl boxLeft:boxes) {
        for (BoxImpl boxRight:other.boxes) {
          BoxImpl isectBox=boxLeft.intersectionWith(boxRight);
          if (!isectBox.isEmpty()) {
            newBoxes.add(isectBox);
          }
        }
      }
      return constructSet(newBoxes);
    }
    
    /** Calculate the symmetric difference of this set with {@code other}. */
    public SetImpl symmetricDifferenceWith(SetImpl other) {
      // Ugh, please don't die.
      SetImpl leftDifference=this.complement().intersectionWith(other);
      SetImpl rightDifference=other.complement().intersectionWith((SetImpl)this);
      return leftDifference.unionWith(rightDifference);
    }
    
    /** Calculate a box which bounds this. */
    public BoxImpl boundingBox() {
      Range[] ranges=new Range[basis.length];
      for (int i=0;i<basis.length;++i) {
        ranges[i]=basis[i].emptyRange();
      }
      for (BoxImpl box:boxes) {
        for (int i=0;i<basis.length;++i) {
          ranges[i]=ranges[i].boundWith(box.projectTo(i));
        }
      }
      return constructBox(ranges);
    }
    
    @Override
    public boolean equals(Object rhsObject) {
      if (rhsObject==null || rhsObject.getClass()!=getClass()) {
        return false;
      }
      SetImpl rhs=(SetImpl)rhsObject;
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
      Iterator<BoxImpl> iterator=boxes.iterator();
      RangeSet firstProjection=iterator.next().projectTo(index).asSet();
      RangeSet[] projections=new RangeSet[boxes.size()-1];
      for (int i=1;i<boxes.size();++i) {
        projections[i-1]=iterator.next().projectTo(index).asSet();
      }
      return firstProjection.unionWith(projections);
    }
  }
}
