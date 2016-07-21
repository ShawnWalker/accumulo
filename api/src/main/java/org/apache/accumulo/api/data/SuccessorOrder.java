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

import java.util.Comparator;

/** 
 * Representation of an ordering where all elements (with the possible exception of a single maximum value
 * posses immediate successors.
 */
public interface SuccessorOrder<T> extends Comparator<T> {
  /** Return the minimum value of T. */
  public T minimumValue();

  /** Return the maximum value of T, or {@code null} if there is none. */
  public T maximumValue();
          
  /** Return the immediate successor of T, or {@code null} if there is none. */
  public T successor(T instance);
    
  /** 
   * True if {@code end} can be reached by no more than {@link Integer#MAX_VALUE} successive iterations of {@link #successor(java.lang.Object)},
   * starting at {@code begin}.
   */
  public boolean isFinite(T begin, T end);
  
  /** Construct a new ordering where nulls compare as larger than any other item, and are unreachable via successor(). */
  public static <T> SuccessorOrder<T> nullsLast(final SuccessorOrder<T> base) {
    return new SuccessorOrder<T>() {      
      @Override
      public T successor(T instance) {
        return instance==null?null:base.successor(instance);
      }

      @Override
      public int compare(T o1, T o2) {
        return o1==null?(o2==null?0:1):
                o2==null?-1:
                base.compare(o1, o2);
      }

      @Override
      public T minimumValue() {
        return base.minimumValue();
      }

      @Override
      public T maximumValue() {
        return null;
      }

      @Override
      public boolean isFinite(T begin, T end) {
        if (begin==null) {
          if (end==null) return true;
          return false;
        } else if (end==null &&  base.maximumValue()==null) {
            return false;
        } else {
          return base.isFinite(begin, end);
        }
      }
    };
  }
}
