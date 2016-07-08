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

import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.master.state.TableCounts;

public class TabletAssigner {
  private final List<TabletGroupWatcher> watchers=new ArrayList<>();
  
  TabletAssigner() {
    
  }
  
  public int assignedOrHosted(String tableId) {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      TableCounts count = watcher.getStats(tableId);
      result += count.hosted() + count.assigned();
    }
    return result;
  }

  public int totalAssignedOrHosted() {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      for (TableCounts counts : watcher.getStats().values()) {
        result += counts.assigned() + counts.hosted();
      }
    }
    return result;
  }

  public int nonMetaDataTabletsAssignedOrHosted() {
    return totalAssignedOrHosted() - assignedOrHosted(MetadataTable.ID) - assignedOrHosted(RootTable.ID);
  }

  public int notHosted() {
    int result = 0;
    for (TabletGroupWatcher watcher : watchers) {
      for (TableCounts counts : watcher.getStats().values()) {
        result += counts.assigned() + counts.assignedToDeadServers();
      }
    }
    return result;
  }
  
}
