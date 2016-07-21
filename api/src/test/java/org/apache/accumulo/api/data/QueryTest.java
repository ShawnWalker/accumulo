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

import java.util.HashSet;
import java.util.Set;
import static org.apache.accumulo.api.data.Query.FAMILY;
import static org.apache.accumulo.api.data.Query.ROW;
import static org.apache.accumulo.api.data.Query.and;
import static org.apache.accumulo.api.data.Query.not;
import static org.apache.accumulo.api.data.Query.or;
import org.junit.Test;
import org.junit.Assert;

public class QueryTest {
  private static final Key.Set QUERY=
    and(
      or(
          FAMILY.eq(Bytes.copyOf("fam1")), FAMILY.eq(Bytes.copyOf("fam2")), FAMILY.eq(Bytes.copyOf("fam3")), FAMILY.eq(Bytes.copyOf("fam4"))
      ),
      ROW.within(Bytes.copyOf("1"), Bytes.copyOf("2")),
      ROW.gt(Bytes.copyOf("1")),
      ROW.lte(Bytes.copyOf("2")),
      not(ROW.eq(Bytes.copyOf("2")))
    );
  
  @Test
  public void testDiscrete() {
    Assert.assertTrue(QUERY.projectToFamily().isFinite());
    Assert.assertTrue(!QUERY.projectToRow().isFinite());
    
    Set<Bytes> families=new HashSet<>();
    for (Bytes element:QUERY.projectToFamily().elements()) {
      families.add(element);
    }
    Assert.assertEquals(4, families.size());
    for (int i=0;i<4;++i) {
      Assert.assertTrue(families.contains(Bytes.copyOf("fam"+(i+1))));
    }
  }
  
  @Test
  public void testContains() {
    Assert.assertTrue(QUERY.contains(new Key.Builder().row(Bytes.copyOf("123")).family(Bytes.copyOf("fam3")).qualifier(Bytes.EMPTY).timestamp(0).build()));
    Assert.assertTrue(!QUERY.contains(new Key.Builder().row(Bytes.copyOf("123")).family(Bytes.copyOf("fam")).qualifier(Bytes.EMPTY).timestamp(0).build()));
    Assert.assertTrue(!QUERY.contains(new Key.Builder().row(Bytes.copyOf("223")).family(Bytes.copyOf("fam3")).qualifier(Bytes.EMPTY).timestamp(0).build()));
  }
  
  @Test
  public void testNot() {
    Key.Set negation=not(QUERY);
    Assert.assertTrue(!negation.isEmpty());
    Assert.assertTrue(and(QUERY, negation).isEmpty());
    Assert.assertTrue(negation.contains(negation.first()));
  }
}
