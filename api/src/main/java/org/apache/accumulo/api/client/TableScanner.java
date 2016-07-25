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
package org.apache.accumulo.api.client;

import java.util.stream.Stream;
import org.apache.accumulo.api.data.Cell;
import org.apache.accumulo.api.data.KeySet;
import org.apache.accumulo.api.security.Authorizations;

public interface TableScanner extends Stream<Cell> {
  public static interface Builder {
    /** Specify set of keys to scan, as a set of boxes. Default is to scan over all keys. */
    public Builder over(KeySet keys);
    
    /** Specify isolation level for scan. Default is {@link IsolationLevel#NONE}. */
    public Builder withIsolation(IsolationLevel isolation);

    /** Specify authorizations to apply to this scan. Default is no authorizations. */
    public Builder withAuthorizations(Authorizations auth);
    
    /** Construct the TableScanner. */
    public TableScanner build();
  }
}
