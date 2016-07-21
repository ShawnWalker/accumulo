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

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 */
public class BytesTest {

  @Test
  public void paddingBehavesJustAsExtraZerosWould() {
    Bytes orig = Bytes.copyOf("asdf");
    assertEquals(4, orig.length());

    Bytes succ1 = orig.successor();
    assertEquals(5, succ1.length());

    Bytes tail = succ1.substr(4);
    assertEquals(1, tail.length());
    assertEquals(Bytes.copyOf(new byte[] {0, 1}).substr(0, 1), tail);
    assertArrayEquals(new byte[] {0}, tail.toByteArray());

    assertEquals(Bytes.copyOf("a\0\0b\0"), Bytes.copyOf("a").successor().successor().concat(Bytes.copyOf("b")).successor());
  }

  @Test
  public void splitOnZeros() {
    Bytes sample = Bytes.copyOf("A").successor().concat(Bytes.copyOf("B").successor());
    assertArrayEquals(new Bytes[] {Bytes.copyOf("A"), Bytes.copyOf("B"), Bytes.EMPTY}, sample.split((byte) 0));
    assertArrayEquals(new Bytes[] {Bytes.copyOf("A"), Bytes.copyOf("B\0")}, sample.split((byte) 0, 2));
  }
}
