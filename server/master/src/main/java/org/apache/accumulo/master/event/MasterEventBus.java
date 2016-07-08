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
package org.apache.accumulo.master.event;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterEventBus extends EventBus {
  private static final Logger log = LoggerFactory.getLogger(MasterEventBus.class);
  private final EventCounter counter=new EventCounter();
  
  public MasterEventBus() {
    register(this);
  }
          
  @Subscribe
  private void onAnyEvent(Object argument) {
    log.info(argument.toString());
    synchronized (counter) {
      ++counter.value;
      counter.notifyAll();
    }
  }
  
  public long waitForEvents(long millis, long lastEvent) {
    synchronized (counter) {
      // Did something happen since the last time we waited?
      if (lastEvent == counter.value) {
        // no
        if (millis <= 0)
          return counter.value;
        try {
          counter.wait(millis);
        } catch (InterruptedException e) {
          log.debug("ignoring InterruptedException", e);
        }
      }
      return counter.value;
      
    }
  }
  
  public Listener getListener() {
    synchronized (counter) {
      return new Listener(counter.value);
    }
  }
  
  public class Listener {
    long lastEvent;

    Listener(long lastEvent) {
      this.lastEvent = lastEvent;
    }

    public void waitForEvents(long millis) {
      lastEvent = MasterEventBus.this.waitForEvents(millis, lastEvent);
    }
  }

  
  private static class EventCounter {
    long value=0;
  }
}
