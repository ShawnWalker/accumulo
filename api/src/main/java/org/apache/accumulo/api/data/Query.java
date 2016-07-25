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

import java.util.Arrays;
import java.util.Objects;
import org.apache.accumulo.api.data.impl.Timestamp;

/** 
 * Utility class for constructing instances of {@link KeySet}. 
 */
public class Query {
  /** Match any key. */
  public static KeySet any() {
    return KeySet.ALL;
  }
  
  /** Scan over only those keys matching all conditions. */
  public static KeySet and(Iterable<KeySet> keySets) {
    KeySet result=KeySet.ALL;
    for (KeySet keySet:keySets) {
      result=result.intersectionWith(keySet);
    }
    return result;
  }
  
  /** Scan over only those keys matching all conditions. */
  public static KeySet and(KeySet... keySets) {
    return and(Arrays.asList(keySets));
  }
  
  /** Scan over those keys matching any conditions. */
  public static KeySet or(Iterable<KeySet> keySets) {
    KeySet result=KeySet.EMPTY;
    for (KeySet keySet:keySets) {
      result=result.unionWith(keySet);
    }
    return result;
  }
  
  /** Scan over those keys matching any conditions. */
  public static KeySet or(KeySet... keySets) {
    return or(Arrays.asList(keySets));
  }
  
  /** 
   * Return the inverse of the specified set.
   */
  public static KeySet not(KeySet keySet) {
    return keySet.complement();
  }
    
  public static final BytesQuery ROW=new BytesQuery(org.apache.accumulo.api.data.Key.Dimension.ROW);
  public static final BytesQuery FAMILY=new BytesQuery(org.apache.accumulo.api.data.Key.Dimension.FAMILY);
  public static final BytesQuery QUALIFIER=new BytesQuery(org.apache.accumulo.api.data.Key.Dimension.QUALIFIER);
  public static final BytesQuery VISIBILITY=new BytesQuery(org.apache.accumulo.api.data.Key.Dimension.VISIBILITY);
          
  public static class BytesQuery {
    //
    // Singleton
    //
    public KeySet equalTo(Bytes value) {
      return cylinder(Objects.requireNonNull(value), Bytes.ORDER.successor(value));
    }
    public KeySet equalTo(String value) {return equalTo(Bytes.copyOf(value));}
    public KeySet eq(Bytes value) {return equalTo(value);}
    public KeySet eq(String value) {return equalTo(Bytes.copyOf(value));}
    
    //
    // Rays.
    //    
    public KeySet lessThan(Bytes value) {
      return cylinder(Bytes.ORDER.minimumValue(), Objects.requireNonNull(value));
    }
    public KeySet lessThanOrEqualTo(Bytes value) {
      return cylinder(Bytes.ORDER.minimumValue(), Bytes.ORDER.successor(Objects.requireNonNull(value)));
    }
    public KeySet greaterThan(Bytes value) {
      return cylinder(Bytes.ORDER.successor(Objects.requireNonNull(value)), null);
    }
    public KeySet greaterThanOrEqualTo(Bytes value) {
      return cylinder(Objects.requireNonNull(value), null);
    }

    public KeySet lessThan(String value) {return lessThan(Bytes.copyOf(value));}
    public KeySet lessThanOrEqualTo(String value) {return lessThanOrEqualTo(Bytes.copyOf(value));}
    public KeySet greaterThan(String value) {return greaterThan(Bytes.copyOf(value));}
    public KeySet greaterThanOrEqualTo(String value) {return greaterThanOrEqualTo(Bytes.copyOf(value));}
    
    public KeySet lt(Bytes value) {return lessThan(value);}
    public KeySet lt(String value) {return lessThan(value);}
    public KeySet lte(Bytes value) {return lessThanOrEqualTo(value);}
    public KeySet lte(String value) {return lessThanOrEqualTo(value);}
    public KeySet gt(Bytes value) {return greaterThan(value);}
    public KeySet gt(String value) {return greaterThan(value);}
    public KeySet gte(Bytes value) {return greaterThanOrEqualTo(value);};
    public KeySet gte(String value) {return greaterThanOrEqualTo(value);};

    //
    // Intervals.
    //
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThan(upperBound))}. */
    public KeySet betweenExclusive(Bytes lowerBound, Bytes upperBound) {
      return cylinder(Objects.requireNonNull(Bytes.ORDER.successor(lowerBound)), upperBound);
    }
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThan(upperBound))}. */
    public KeySet betweenExclusive(String lowerBound, String upperBound) {
      return betweenExclusive(Bytes.copyOf(lowerBound), Bytes.copyOf(upperBound));
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet betweenInclusive(Bytes lowerBound, Bytes upperBound) {
      return cylinder(lowerBound, Bytes.ORDER.successor(Objects.requireNonNull(upperBound)));
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet betweenInclusive(String lowerBound, String upperBound) {
      return betweenInclusive(Bytes.copyOf(lowerBound), Bytes.copyOf(upperBound));
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThan(upperBound))}. */
    public KeySet closedOpen(Bytes lowerBound, Bytes upperBound) {
      return cylinder(Objects.requireNonNull(lowerBound), Objects.requireNonNull(upperBound));
    }

    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThan(upperBound))}. */
    public KeySet closedOpen(String lowerBound, String upperBound) {
      return closedOpen(Bytes.copyOf(lowerBound), Bytes.copyOf(upperBound));
    }
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet openClosed(Bytes lowerBound, Bytes upperBound) {
      return cylinder(Objects.requireNonNull(Bytes.ORDER.successor(lowerBound)), Bytes.ORDER.successor(Objects.requireNonNull(upperBound)));
    }
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet openClosed(String lowerBound, String upperBound) {
      return openClosed(Bytes.copyOf(lowerBound), Bytes.copyOf(upperBound));
    }
    
    //
    // Misc.
    //
    public KeySet startsWith(Bytes prefix) {
      return cylinder(Objects.requireNonNull(prefix), prefixEnd(prefix));
    }
    public KeySet startsWith(String prefix) {return startsWith(Bytes.copyOf(prefix));}
    
    //
    // Impl.
    //
    
    protected final org.apache.accumulo.api.data.Key.Dimension dim;
    private BytesQuery(org.apache.accumulo.api.data.Key.Dimension dim) {
      this.dim=dim;
    }
    
    protected KeySet cylinder(Bytes begin, Bytes end) {
      return KeySet.cylinder(dim, new Bytes.Range(begin, end));
    }
    
    /** Calculate the {@link Bytes} which is just larger than all byte strings starting with {@code prefix}. */
    protected static Bytes prefixEnd(Bytes prefix) {
      byte[] work=prefix.toByteArray();
      int endPos;
      for (endPos=work.length;endPos>0;--endPos) {
        if (work[endPos-1]!=-1) {
          ++work[endPos];
          break;
        }
      }
      return endPos==0?null:Bytes.wrap(work, 0, endPos);
    }    
  }
  
  /** 
   * For clarity, we provide query operators matching natural ordering of long, despite the fact that timestamps are
   * internally ordered in reverse.
   */
  public static class TimestampQuery {    
    //
    // Singleton
    //
    public KeySet equalTo(long timestamp) {
      return cylinder(timestamp, Timestamp.ORDER.successor(timestamp));
    }    
    public KeySet eq(long timestamp) {return equalTo(timestamp);}
    
    //
    // Rays
    //
    public KeySet lessThan(long timestamp) {
      return cylinder(Objects.requireNonNull(Timestamp.ORDER.successor(timestamp)), null);
    }
    public KeySet lessThanOrEqualTo(long timestamp) {
      return cylinder(timestamp, null);
    }
    public KeySet greaterThan(long timestamp) {
      return cylinder(Timestamp.ORDER.minimumValue(), timestamp);
    }
    public KeySet greaterThanOrEqualTo(long timestamp) {
      return cylinder(Timestamp.ORDER.minimumValue(), Timestamp.ORDER.successor(timestamp));
    }

    public KeySet lt(long timestamp) {return lessThan(timestamp);}
    public KeySet lte(long timestamp) {return lessThanOrEqualTo(timestamp);}
    public KeySet gt(long timestamp) {return greaterThan(timestamp);}
    public KeySet gte(long timestamp) {return greaterThanOrEqualTo(timestamp);}
    
    //
    // Intervals
    //
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThan(upperBound))}. */
    public KeySet betweenExclusive(long start, long end) {
      return cylinder(Timestamp.ORDER.successor(end), start);
    }

    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet betweenInclusive(long start, long end) {
      return cylinder(end, Timestamp.ORDER.successor(start));
    }
    
    /** Equivalent to {@code and(greaterThanOrEqualTo(lowerBound), lessThan(upperBound))}. */
    public KeySet closedOpen(long start, long end) {
      return cylinder(Timestamp.ORDER.successor(end), Timestamp.ORDER.successor(start));
    }
    
    /** Equivalent to {@code and(greaterThan(lowerBound), lessThanOrEqualTo(upperBound))}. */
    public KeySet openClosed(long start, long end) {
      return cylinder(end, start);
    }
    
    protected KeySet cylinder(long end, Long start) {
      return KeySet.cylinder(org.apache.accumulo.api.data.Key.Dimension.TIMESTAMP, new Timestamp.Range(end, start));
    }    
  }
}
