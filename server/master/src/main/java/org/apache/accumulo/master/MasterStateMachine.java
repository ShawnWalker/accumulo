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
package org.apache.accumulo.master;

import com.google.common.eventbus.EventBus;
import org.apache.accumulo.core.master.thrift.MasterState;
import static org.apache.accumulo.master.Master.log;
import org.apache.accumulo.master.event.MasterStateChangeEvent;
import org.apache.accumulo.server.util.time.SimpleTimer;

public class MasterStateMachine {
  private static final boolean X = true;
  private static final boolean O = false;
  // @formatter:off
  private static final boolean transitionOK[][] = {
      //                              INITIAL HAVE_LOCK SAFE_MODE NORMAL UNLOAD_META UNLOAD_ROOT STOP
      /* INITIAL */                   {X,     X,        O,        O,      O,         O,          X},
      /* HAVE_LOCK */                 {O,     X,        X,        X,      O,         O,          X},
      /* SAFE_MODE */                 {O,     O,        X,        X,      X,         O,          X},
      /* NORMAL */                    {O,     O,        X,        X,      X,         O,          X},
      /* UNLOAD_METADATA_TABLETS */   {O,     O,        X,        X,      X,         X,          X},
      /* UNLOAD_ROOT_TABLET */        {O,     O,        O,        X,      X,         X,          X},
      /* STOP */                      {O,     O,        O,        O,      O,         X,          X}};
  //@formatter:on

  private final EventBus eventBus;
  private MasterState state = MasterState.INITIAL;
  
  MasterStateMachine(EventBus eventBus) {
    this.eventBus = eventBus;
  }
  
  public synchronized MasterState getMasterState() {
    return state;
  }

  public boolean stillMaster() {
    return getMasterState() != MasterState.STOP;
  }

  
  synchronized void setMasterState(MasterState newState) {
    MasterState oldState = state;
    if (state.equals(newState))
      return;
    if (!transitionOK[state.ordinal()][newState.ordinal()]) {
      log.error("Programmer error: master should not transition from " + state + " to " + newState);
    }
    state = newState;
    eventBus.post(new MasterStateChangeEvent(oldState, newState));
  }
  
}
