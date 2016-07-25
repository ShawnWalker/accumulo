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
import org.apache.accumulo.api.data.Bytes;
import org.apache.accumulo.api.data.Key;
import org.apache.accumulo.api.data.impl.ProductBoxSet;
import org.apache.accumulo.api.data.impl.Range;

/**
 *
 * @author sawalk4
 */
public class KeySet extends ProductBoxSet<Key, KeyBox, KeySet> {
  
  public static final KeySet EMPTY = new KeySet(Collections.EMPTY_SET);
  public static final KeySet ALL = new KeySet(Collections.singleton(KeyBox.ALL));

  public KeySet(java.util.Set<KeyBox> boxes) {
    super(KeyDimension.BASIS, boxes);
  }

  @Override
  protected KeySet constructSet(java.util.Set<KeyBox> boxes) {
    return new KeySet(boxes);
  }

  @Override
  protected KeyBox constructBox(Range[] projections) {
    return new KeyBox(projections);
  }

  public Bytes.RangeSet projectToRow() {
    return (Bytes.RangeSet) projectTo(KeyDimension.ROW.ordinal());
  }

  public Bytes.RangeSet projectToFamily() {
    return (Bytes.RangeSet) projectTo(KeyDimension.FAMILY.ordinal());
  }
  
}
