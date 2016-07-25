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

import java.util.Collections;
import java.util.Set;
import org.apache.accumulo.api.data.impl.DeletionMarker;
import org.apache.accumulo.api.data.impl.ProductBox;
import org.apache.accumulo.api.data.impl.ProductBoxSet;
import org.apache.accumulo.api.data.impl.Range;
import org.apache.accumulo.api.data.impl.Timestamp;

public class KeySet extends ProductBoxSet<Key, KeySet.Box, KeySet> {
  public static final KeySet EMPTY = new KeySet(Collections.EMPTY_SET);
  public static final KeySet ALL = new KeySet(Collections.singleton(Box.ALL));

  protected KeySet(Set<Box> boxes) {
    super(Key.Dimension.BASIS, boxes);
  }

  /** Determine which rows this set will encounter. */
  public Bytes.RangeSet projectToRow() {
    return (Bytes.RangeSet) projectTo(Key.Dimension.ROW.ordinal());
  }

  /** Determine which column families this set will encounter. */
  public Bytes.RangeSet projectToFamily() {
    return (Bytes.RangeSet) projectTo(Key.Dimension.FAMILY.ordinal());
  }
  
  /** Determine which column qualifiers this set will encounter. */
  public Bytes.RangeSet projectToQualifier() {
    return (Bytes.RangeSet) projectTo(Key.Dimension.QUALIFIER.ordinal());
  }
  
  public Bytes.RangeSet projectToVisibility() {
    return (Bytes.RangeSet) projectTo(Key.Dimension.VISIBILITY.ordinal());
  }
  
  public Timestamp.RangeSet projectToTimestamp() {
    return (Timestamp.RangeSet) projectTo(Key.Dimension.TIMESTAMP.ordinal());
  }
  
  /** 
   * Construct a cylinder whose projection onto the dimension {@code dimension} is {@code range}, and whose projection
   * onto all other dimensions is full.
   */
  public static KeySet cylinder(Key.Dimension dimension, Range range) {
    Range[] projections=new Range[Key.Dimension.BASIS.length];
    for (int i=0;i<Key.Dimension.BASIS.length;++i) {
      if (i==dimension.ordinal()) {
        projections[i]=range;
      } else {
        projections[i]=Key.Dimension.BASIS[i].fullRange();
      }
    }
    return new KeySet(Collections.singleton(new Box(projections)));
  }
  

  @Override
  protected KeySet constructSet(Set<Box> boxes) {
    return new KeySet(boxes);
  }

  @Override
  protected Box constructBox(Range[] projections) {
    return new Box(projections);
  }  

  public static class Box extends ProductBox<Key, Box> {
    public static final Box ALL = new Box(new Range[]{Bytes.Range.ALL, Bytes.Range.ALL, Bytes.Range.ALL, Bytes.Range.ALL, Timestamp.Range.ALL, DeletionMarker.Range.ALL});

    public Box(Range[] projections) {
      super(Key.Dimension.BASIS, projections);
    }

    @Override
    protected Box constructBox(Range[] projections) {
      return new Box(projections);
    }

    @Override
    protected Key constructTuple(Object[] fields) {
      return new Key((Bytes) fields[0], (Bytes) fields[1], (Bytes) fields[2], (Bytes) fields[3], (Long) fields[4], (Boolean) fields[5]);
    }
  }
}
