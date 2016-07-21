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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An immutable byte string, in dictionary order by (unsigned) byte values. <br/>
 * {@code Bytes} contains two "optimizations" to avoid unnecessary copying of byte arrays:
 * <ul>
 * <li>{@code Bytes.substr()} returns a {@code Bytes} which shares content with the original.</li>
 * <li>{@code Bytes.immediateSuccessor()} returns a {@code Bytes} which shares content with the original.</li>
 * </ul>
 */
public final class Bytes implements Comparable<Bytes> {
  public static final Class<?> TYPE=Bytes.class;
  public static final Bytes EMPTY = new Bytes(new byte[0]);
  public static final SuccessorOrder<Bytes> ORDER=new SuccessorOrder<Bytes>() {
    @Override
    public Bytes successor(Bytes instance) {
      return instance.successor();
    }

    @Override
    public int compare(Bytes o1, Bytes o2) {
      return o1.compareTo(o2);
    }

    @Override
    public Bytes minimumValue() {
      return EMPTY;
    }

    @Override
    public Bytes maximumValue() {
      return null;
    }

    @Override
    public boolean isFinite(Bytes begin, Bytes end) {
      return begin.distanceTo(end)!=null;
    }
  };
          
  final byte[] content;
  final int start;
  final int end;
  final int endPadding;

  protected Bytes(byte[] content) {
    this(content, 0);
  }

  protected Bytes(byte[] content, int offset) {
    this(content, offset, content.length);
  }

  protected Bytes(byte[] content, int start, int end) {
    this(content, start, end, 0);
  }

  protected Bytes(byte[] content, int start, int end, int endPadding) {
    this.content = content;
    this.start = Math.max(0, start);
    this.end = Math.min(end, content.length);
    this.endPadding = Math.max(0, endPadding);
  }

  /** Create a {@code Bytes} from a {@code String}. */
  public static Bytes copyOf(String string, Charset charset) {
    return new Bytes(string.getBytes(charset));
  }

  /** Create a {@code Bytes} from a {@code String} using the UTF-8 character set. */
  public static Bytes copyOf(String string) {
    return Bytes.copyOf(string, StandardCharsets.UTF_8);
  }

  /** Create {@code Bytes} from a byte array. */
  public static Bytes copyOf(byte[] content) {
    return copyOf(content, 0, content.length);
  }

  /**
   * Create {@code Bytes} from a byte array.
   *
   * @param content
   *          byte array
   * @param offset
   *          Index of first byte in array to copy (inclusive).
   * @param length
   *          Number of bytes to copy.
   */
  public static Bytes copyOf(byte[] content, int offset, int length) {
    return new Bytes(Arrays.copyOfRange(content, offset, offset + length));
  }

  /**
   * Create a {@code Bytes} from a byte array, without copying. As {@code Bytes} may share state between different instances, modification of the underlying
   * byte array after wrapping it may lead to unpredictable consequences.
   */
  public static Bytes wrap(byte[] content) {
    return wrap(content, 0, content.length);
  }

  /**
   * Create a {@code Bytes} from a byte array, without copying. As {@code Bytes} may share state between different instances, modification of the underlying
   * byte array after wrapping it may lead to unpredictable consequences.
   */
  public static Bytes wrap(byte[] content, int offset, int length) {
    return new Bytes(content, offset, offset + length, 0);
  }

  /** Get the specified byte from the {@code Bytes}. */
  public byte byteAt(int i) {
    if (0 > i || i >= end - start + endPadding) {
      throw new IndexOutOfBoundsException();
    }
    return i >= end - start ? 0 : content[start + i];
  }

  /** True if this {@code Bytes} is empty. */
  public boolean isEmpty() {
    return length() == 0;
  }

  /** Number of bytes in the {@code Bytes}. */
  public int length() {
    return end - start + endPadding;
  }

  /**
   * Search for the first occurrence of the specified byte, returning the index it's found at, or -1 if not found.
   *
   * @param b
   *          byte to search for
   * @param searchStart
   *          index to start search at.
   */
  public int indexOf(byte b, int searchStart) {
    for (int pos = searchStart + start; pos < end; ++pos) {
      if (content[pos] == b) {
        return pos - start;
      }
    }
    return b == 0 && endPadding >= 0 ? length() : -1;
  }

  /**
   * Search for the first occurrence of the specified byte, returning the index it's found at, or -1 if not found.
   */
  public int indexOf(byte b) {
    return indexOf(b, 0);
  }

  /**
   * Construct a substring.
   *
   * @param from
   *          Index of first byte to take (inclusive).
   * @param to
   *          Index of last byte to take (exclusive).
   */
  public Bytes substr(int from, int to) {
    if (from < 0 || to < from) {
      throw new IndexOutOfBoundsException();
    }
    int lbound = Math.min(start + from, end + endPadding);
    int ubound = Math.min(start + to, end + endPadding);
    return new Bytes(content, lbound >= end ? end : lbound, ubound >= end ? end : ubound, Math.max(0, ubound - end));
  }

  /**
   * Construct a substring taking an entire suffix.
   *
   * @param from
   *          Index of first byte to take (inclusive).
   */
  public Bytes substr(int from) {
    return substr(from, from + length());
  }

  /** Split the {@code Bytes} into separate pieces, dividing at each instance of {@code splitBytes}. */
  public Bytes[] split(byte splitByte) {
    return split(Collections.singleton(splitByte), Integer.MAX_VALUE);
  }

  /**
   * Split the {@code Bytes} into separate pieces, dividing at each instance of {@code splitBytes}, returning no more than {@code limit} pieces.
   */
  public Bytes[] split(byte splitByte, int limit) {
    return split(Collections.singleton(splitByte), limit);
  }

  /** Split the {@code Bytes} into separate pieces, dividing at each instance of {@code splitBytes}. */
  public Bytes[] split(Set<Byte> splitBytes) {
    return split(splitBytes, Integer.MAX_VALUE);
  }

  /**
   * Split the {@code Bytes} into separate pieces, dividing at each instance of {@code splitBytes}, returning no more than {@code limit} pieces.
   */
  public Bytes[] split(Set<Byte> splitBytes, int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException();
    }

    int splitPoints = 0;
    for (int i = 0; i < length() && splitPoints + 1 < limit; ++i) {
      if (splitBytes.contains(byteAt(i))) {
        ++splitPoints;
      }
    }

    Bytes[] parts = new Bytes[splitPoints + 1];
    int lastOcc = -1;
    int splitsFound = 0;
    for (int i = 0; i < length() && splitsFound + 1 < parts.length; ++i) {
      if (splitBytes.contains(byteAt(i))) {
        parts[splitsFound++] = substr(1 + lastOcc, i);
        lastOcc = i;
      }
    }
    assert splitsFound + 1 == parts.length;
    parts[splitsFound] = substr(1 + lastOcc);
    return parts;
  }

  /** Concatenate two {@code Bytes}. */
  public Bytes concat(Bytes rhs) {
    byte[] newContent = new byte[length() + rhs.length()];
    System.arraycopy(content, start, newContent, 0, end - start);
    System.arraycopy(rhs.content, rhs.start, newContent, length(), rhs.end - rhs.start);
    return new Bytes(newContent);
  }

  /** Create a copy of the contents of this {@code Bytes} as a byte array. */
  public byte[] toByteArray() {
    byte[] result = new byte[length()];
    System.arraycopy(content, start, result, 0, end - start);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length(); ++i) {
      char c = (char) (0xff & byteAt(i));
      if (0x20 < c && c < 0x7e) {
        sb.append(c);
      } else {
        sb.append(String.format("\\x%02x", (int) c));
      }
    }
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(toByteArray());
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (!(rhsObject instanceof Bytes)) {
      return false;
    }
    return compareTo((Bytes) rhsObject) == 0;
  }

  //
  // Ordering operations.
  //
  
  public Bytes successor() {
    return new Bytes(content, start, end, endPadding + 1);
  }
  
  public Long distanceTo(Bytes other) {
    if (other.length()<this.length()) {return null;}
    if (!other.substr(0, this.length()).equals(this)) {return null;}
    for (int i=this.length();i<other.length();++i) {
      if (other.byteAt(i)!=0) {
        return null;
      }
    }
    return (long)(other.length()-this.length());
  }

  @Override
  public int compareTo(Bytes rhs) {
    int commonLength = Math.min(this.length(), rhs.length());
    for (int i = 0; i < commonLength; ++i) {
      int leftByte = 0xff & this.byteAt(i);
      int rightByte = 0xff & rhs.byteAt(i);
      if (leftByte != rightByte) {
        return leftByte - rightByte;
      }
    }
    // Given equal prefixes, comparison is on length.
    return this.length() - rhs.length();
  }

  public static class Range extends org.apache.accumulo.api.data.Range<Bytes, Range, RangeSet> {
    public static final Range EMPTY = new Range(Bytes.EMPTY, Bytes.EMPTY);
    public static final Range ALL = new Range(Bytes.ORDER.minimumValue(), null);

    protected Range(Bytes lowerBound, Bytes upperBound) {
      super(lowerBound, upperBound, Bytes.ORDER);
    }

    @Override
    protected Range construct(Bytes lowerBound, Bytes upperBound) {
      return new Range(lowerBound, upperBound);
    }

    @Override
    protected RangeSet constructSet(SortedSet<Bytes> breakPoints) {
      return new RangeSet(breakPoints);
    }
  }

  public static class RangeSet extends org.apache.accumulo.api.data.RangeSet<Bytes, Range, RangeSet> {
    public static final RangeSet EMPTY=new RangeSet(Collections.emptySortedSet());
    public static final RangeSet ALL=new RangeSet(new TreeSet<>(Collections.singleton(Bytes.EMPTY)));
    
    protected RangeSet(SortedSet<Bytes> breakPoints) {
      super(breakPoints, Bytes.ORDER);
    }

    @Override
    protected RangeSet construct(SortedSet<Bytes> breakPoints) {
      return new RangeSet(breakPoints);
    }

    @Override
    protected Bytes.Range constructRange(Bytes lowerBound, Bytes upperBound) {
      return new Bytes.Range(lowerBound, upperBound);
    }
  }
}
