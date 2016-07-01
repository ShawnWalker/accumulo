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
package org.apache.accumulo.core.file.async;

import org.apache.accumulo.core.async.AsyncEngine;
import org.apache.accumulo.core.async.AsyncEngineMissingException;
import org.apache.accumulo.core.async.AsyncScheduler;

/** Coordinates opening and reading of blocks from files. */
public class BlockBroker extends AsyncEngine {
  public BlockBroker() {
    this(BlockBroker.class.getSimpleName());
  }
  public BlockBroker(String name) {
    this(name, DEFAULT_SCHEDULER.get());
  }
  public BlockBroker(String name, AsyncScheduler scheduler) {
    super(name, scheduler);
  }
  
  public static LocalBlockBroker getLocalEngine() {
    LocalEngine engine=AsyncEngine.getLocalEngine();
    if (!(engine instanceof LocalBlockBroker)) {
      throw new AsyncEngineMissingException();
    }
    return (LocalBlockBroker)engine;
  }

  @Override
  protected LocalBlockBroker constructLocalEngine() {
    return new LocalBlockBroker();
  }
  
  public class LocalBlockBroker extends AsyncEngine.LocalEngine {    
  }
}
