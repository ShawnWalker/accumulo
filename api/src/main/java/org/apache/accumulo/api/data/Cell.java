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

import java.util.Objects;

public class Cell implements Comparable<Cell> {
  private final Key key;
  private final Bytes value;
  
  public Cell(Key key, Bytes value) {
    this.key=Objects.requireNonNull(key);
    this.value=Objects.requireNonNull(value);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (!(rhsObject instanceof Cell)) {
      return false;
    }
    Cell rhs=(Cell)rhsObject;
    return Objects.equals(key, rhs.key) && Objects.equals(value, rhs.value);
  }
  
  public Key getKey() {
    return key;
  }
  
  public Bytes getValue() {
    return value;
  }

  @Override
  public int compareTo(Cell o) {
    int cmp;
    if ((cmp=key.compareTo(o.key))!=0) return cmp;
    return value.compareTo(o.value);
  }
}
