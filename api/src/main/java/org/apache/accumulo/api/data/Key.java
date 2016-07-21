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
 * distributed under the License is distributed on an "AS IS" Dimension.BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.api.data;

import java.util.Collections;
import java.util.Objects;
import org.apache.accumulo.api.data.Product.Basis;

public final class Key extends Product.Tuple<Key> {
  public static final Key MIN_VALUE=new Key(
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Timestamp.ORDER.minimumValue(), 
          DeletionMarker.ORDER.minimumValue());
  public static final SuccessorOrder<Key> ORDER=Product.Tuple.order(MIN_VALUE);
  
  public static enum Dimension implements Product.Basis {
    ROW(Bytes.TYPE, Bytes.ORDER, Bytes.Range.class, Bytes.Range.EMPTY, Bytes.Range.ALL),
    FAMILY(Bytes.TYPE, Bytes.ORDER, Bytes.Range.class, Bytes.Range.EMPTY, Bytes.Range.ALL),
    QUALIFIER(Bytes.TYPE, Bytes.ORDER, Bytes.Range.class, Bytes.Range.EMPTY, Bytes.Range.ALL),
    VISIBILITY(Bytes.TYPE, Bytes.ORDER, Bytes.Range.class, Bytes.Range.EMPTY, Bytes.Range.ALL),
    TIMESTAMP(Timestamp.TYPE, Timestamp.ORDER, Timestamp.Range.class, Timestamp.Range.EMPTY, Timestamp.Range.ALL),
    DELETION(DeletionMarker.TYPE, DeletionMarker.ORDER, DeletionMarker.Range.class, DeletionMarker.Range.EMPTY, DeletionMarker.Range.ALL);

    public static final Basis[] BASIS=values();
    private final Class<?> type;
    private final SuccessorOrder order;
    private final Class<? extends Range> rangeType;
    private final Range<?,?,?> emptyRange;
    private final Range<?,?,?> fullRange;
    
    Dimension(Class<?> type, SuccessorOrder<?> order, Class<? extends Range> rangeType, Range<?,?,?> emptyRange, Range<?,?,?> fullRange) {
      this.type=type;
      this.order=order;
      this.rangeType=rangeType;
      this.emptyRange=emptyRange;
      this.fullRange=fullRange;
    }
    
    @Override public Class<?> type() {return type;}
    @Override public SuccessorOrder order() {return order;}
    @Override public Class<? extends Range> rangeType() {return rangeType;}
    @Override public Range<?,?,?> emptyRange() {return emptyRange;}
    @Override public Range<?,?,?> fullRange() {return fullRange;}
  };
    
  Key(Object[] fields) {
    super(Dimension.BASIS, fields);
  }
  
  public Key(Bytes row, Bytes family, Bytes qualifier, Bytes visibility, long timestamp, boolean deletion) {
    this(new Object[]{
      Objects.requireNonNull(row),
      Objects.requireNonNull(family),
      Objects.requireNonNull(qualifier),
      Objects.requireNonNull(visibility),
      timestamp,
      deletion
    });
  }
  
  @Override
  protected Key construct(Object[] fields) {
    return new Key(fields);
  }
  
  public static class Builder {
    private Bytes row=Bytes.EMPTY;
    private Bytes family=Bytes.EMPTY;
    private Bytes qualifier=Bytes.EMPTY;
    private Bytes visibility=Bytes.EMPTY;
    private Long timestamp;
    private boolean deleted=false;
    
    public Builder row(Bytes row) {
      this.row=row;
      return this;
    }
    public Builder family(Bytes family) {
      this.family=family;
      return this;
    }
    public Builder qualifier(Bytes qualifier) {
      this.qualifier=qualifier;
      return this;
    }
    public Builder visibility(Bytes visibility) {
      this.visibility=visibility;
      return this;
    }
    public Builder timestamp(long timestamp) {
      this.timestamp=timestamp;
      return this;
    }
    public Builder deleted(boolean deleted) {
      this.deleted=deleted;
      return this;
    }
    public Key build() {
      return new Key(row, family, qualifier, visibility, timestamp, deleted);
    }
  }
  
  public Object get(Dimension dim) {
    return get(dim.ordinal());
  }
  
  public Bytes getRow() {
    return (Bytes)get(Dimension.ROW);
  }
  
  public Bytes getFamily() {
    return (Bytes)get(Dimension.FAMILY);
  }
  
  public Bytes getQualifier() {
    return (Bytes)get(Dimension.QUALIFIER);
  }
  
  public Bytes getVisibility() {
    return (Bytes)get(Dimension.VISIBILITY);
  }
  
  public long getTimestamp() {
    return (Long)get(Dimension.TIMESTAMP);
  }
  
  public boolean isDeleted() {
    return (boolean)get(Dimension.DELETION);
  }
  
  @Override
  public String toString() {
    return String.format("%s %s:%s [%s] %d %s",
            getRow(), getFamily(), getQualifier(), getVisibility(), getTimestamp(), isDeleted());
  }
  
  static class Box extends Product.Box<Key, Box, Set> {
    public static final Box EMPTY=new Box(new Range[]{
      Bytes.Range.EMPTY, 
      Bytes.Range.EMPTY,
      Bytes.Range.EMPTY,
      Bytes.Range.EMPTY, 
      Timestamp.Range.EMPTY, 
      DeletionMarker.Range.EMPTY});
    public static final Box ALL=new Box(new Range[]{
      Bytes.Range.ALL, 
      Bytes.Range.ALL, 
      Bytes.Range.ALL, 
      Bytes.Range.ALL, 
      Timestamp.Range.ALL, 
      DeletionMarker.Range.ALL});
    Box(Range[] projections) {
      super(Dimension.BASIS, projections);
    }
    
    @Override
    protected Box constructBox(Range[] projections) {
      return new Box(projections);
    }

    @Override
    protected Key constructTuple(Object[] fields) {
      return new Key(fields);
    }
    
    @Override
    protected Set constructSet(java.util.Set<Box> boxes) {
      return new Set(boxes);
    }    
  }
  
  public static class Set extends Product.BoxSet<Key, Box, Set> {
    public static final Set EMPTY=new Set(Collections.EMPTY_SET);
    public static final Set ALL=new Set(Collections.singleton(Box.ALL));
    
    Set(java.util.Set<Box> boxes) {
      super(Dimension.BASIS, boxes);
    }
    
    @Override
    protected Set constructSet(java.util.Set<Box> boxes) {
      return new Set(boxes);
    }
    
    @Override
    protected Box constructBox(Range[] projections) {
      return new Box(projections);
    }
    
    public Bytes.RangeSet projectToRow() {
      return (Bytes.RangeSet)projectTo(Dimension.ROW.ordinal());      
    }
    
    public Bytes.RangeSet projectToFamily() {
      return (Bytes.RangeSet)projectTo(Dimension.FAMILY.ordinal());
    }
  }
}
