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

import org.apache.accumulo.api.data.Bytes;
import org.apache.accumulo.api.data.Key;

class KeyBox extends ProductBox<Key, KeyBox, KeySet> {
  
  public static final KeyBox EMPTY = new KeyBox(new Range[]{Bytes.Range.EMPTY, Bytes.Range.EMPTY, Bytes.Range.EMPTY, Bytes.Range.EMPTY, Timestamp.Range.EMPTY, DeletionMarker.Range.EMPTY});
  public static final KeyBox ALL = new KeyBox(new Range[]{Bytes.Range.ALL, Bytes.Range.ALL, Bytes.Range.ALL, Bytes.Range.ALL, Timestamp.Range.ALL, DeletionMarker.Range.ALL});

  public KeyBox(Range[] projections) {
    super(KeyDimension.BASIS, projections);
  }

  Range[] getProjections() {
    return projections;
  }

  @Override
  protected KeyBox constructBox(Range[] projections) {
    return new KeyBox(projections);
  }

  @Override
  protected Key constructTuple(Object[] fields) {
    return new Key(fields);
  }

  @Override
  protected KeySet constructSet(java.util.Set<KeyBox> boxes) {
    return new KeySet(boxes);
  }
  
}
