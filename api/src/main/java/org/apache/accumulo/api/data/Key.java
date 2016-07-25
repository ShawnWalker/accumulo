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

import org.apache.accumulo.api.data.impl.KeyDimension;
import org.apache.accumulo.api.data.impl.DeletionMarker;
import org.apache.accumulo.api.data.impl.SuccessorOrder;
import org.apache.accumulo.api.data.impl.Timestamp;
import java.util.Objects;
import java.util.SortedSet;
import org.apache.accumulo.api.data.impl.Tuple;

public final class Key extends Tuple<Key> {
  public static final Key MIN_VALUE=new Key(
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Bytes.EMPTY, 
          Timestamp.ORDER.minimumValue(), 
          DeletionMarker.ORDER.minimumValue());
  public static final SuccessorOrder<Key> ORDER=Tuple.order(KeyDimension.BASIS);
  
    
  Key(Object[] fields) {
    super(KeyDimension.BASIS, fields);
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
    
  public static class Builder {
    private Bytes row=Bytes.EMPTY;
    private Bytes family=Bytes.EMPTY;
    private Bytes qualifier=Bytes.EMPTY;
    private Bytes visibility=Bytes.EMPTY;
    private Long timestamp;
    private boolean deleted=false;
    
    public Builder() {}
    public Builder(Bytes row, Bytes family, Bytes qualifier, Bytes visibility, Long timestamp, boolean deleted) {
      this.row=row;
      this.family=family;
      this.qualifier=qualifier;
      this.visibility=visibility;
      this.timestamp=timestamp;
      this.deleted=deleted;
    }
    
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
    
    public Builder row(String row) {return row(Bytes.copyOf(row));}
    public Builder family(String family) {return family(Bytes.copyOf(family));}
    public Builder qualifier(String qualifier) {return qualifier(Bytes.copyOf(qualifier));}
    public Builder visibility(String visibility) {return visibility(Bytes.copyOf(visibility));}
    
    public Key build() {
      return new Key(row, family, qualifier, visibility, timestamp, deleted);
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  /** Construct a new key that's a copy of this key, perhaps with some changes. */
  public Builder rebuildWith() {
    return new Builder(getRow(), getFamily(), getQualifier(), getVisibility(), getTimestamp(), isDeleted());
  }
  
  public Object get(KeyDimension dim) {
    return get(dim.ordinal());
  }
  
  public Bytes getRow() {
    return (Bytes)get(KeyDimension.ROW);
  }
  
  public Bytes getFamily() {
    return (Bytes)get(KeyDimension.FAMILY);
  }
  
  public Bytes getQualifier() {
    return (Bytes)get(KeyDimension.QUALIFIER);
  }
  
  public Bytes getVisibility() {
    return (Bytes)get(KeyDimension.VISIBILITY);
  }
  
  public long getTimestamp() {
    return (Long)get(KeyDimension.TIMESTAMP);
  }
  
  public boolean isDeleted() {
    return (boolean)get(KeyDimension.DELETION);
  }
  
  @Override
  public String toString() {
    return String.format("%s %s:%s [%s] %d %s",
            getRow(), getFamily(), getQualifier(), getVisibility(), getTimestamp(), isDeleted());
  }
  
  @Override
  protected Key construct(Object[] fields) {
    return new Key(fields);
  }
  
  public static class Range extends org.apache.accumulo.api.data.impl.Range<Key, Range, RangeSet> {
    public Range(Key lowerBound, Key upperBound) {
      super(lowerBound, upperBound, Key.ORDER);
    }
    
    @Override protected Range construct(Key lowerBound, Key upperBound) {return new Range(lowerBound, upperBound);}
    @Override protected RangeSet constructSet(SortedSet<Key> breakPoints) {return new RangeSet(breakPoints);}
  }
  
  public static class RangeSet extends org.apache.accumulo.api.data.impl.RangeSet<Key, Range, RangeSet> {
    RangeSet(SortedSet<Key> breakPoints) {
      super(breakPoints, Key.ORDER);
    }
    @Override protected RangeSet construct(SortedSet<Key> breakPoints) {return new RangeSet(breakPoints);}
    @Override protected Range constructRange(Key lowerBound, Key upperBound) {return new Range(lowerBound, upperBound);}
  }
}
