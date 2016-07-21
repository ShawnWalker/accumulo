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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.accumulo.api.AccumuloException;

public interface AccumuloClient extends AutoCloseable {
  public static interface Builder {    
    /** Set a specific client configuration property. */
    public Builder with(String propName, String value);

    /** Set a collection of client configuration properties. */
    default public Builder with(Iterable<Map.Entry<String, String>> conf) {
      Builder b=this;
      for (Map.Entry<String, String> propPair:conf) {
        b=b.with(propPair.getKey(), propPair.getValue());
      }
      return b;
    }
  }

  /** 
   * Initiate a shutdown of the client. Release all resources consumed by the client.  This implicitly releases any
   * resources used by scanners and writers.
   */
  public CompletableFuture<Void> shutdownAsync();

  /** 
   * Shutdown the client. Release all resources consumed by the client.  This implicitly closes 
   */
  @Override
  public default void close() throws AccumuloException, InterruptedException {
    CompletableFuture<Void> shutdown=shutdownAsync();
    try {
      shutdown.get();
    } catch (InterruptedException ex) {
      throw ex;
    } catch (ExecutionException ex) {
      Throwable cause=ex.getCause();
      if (cause instanceof AccumuloException) {
        throw (AccumuloException)cause;
      } else {
        throw new AccumuloException("Failed to shut down", cause);
      }
    }
  }
  
  /** Construct a scanner. To avoid resource leaks, scanners must be closed after use. */
  public TableScanner scan(String tableName);
  
  /** Construct a writer. */
}
