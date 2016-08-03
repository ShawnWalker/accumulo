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
package org.apache.accumulo.server;

import com.google.inject.AbstractModule;
import javax.inject.Singleton;
import org.apache.accumulo.core.client.impl.AccumuloClientModule;
import org.apache.accumulo.core.inject.Requires;
import org.apache.accumulo.server.client.ServerInstanceModule;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.zookeeper.ServerZookeeperModule;

/** Dependencies for Accumulo servers. */
@Requires({AccumuloClientModule.class, ServerInstanceModule.class, ServerZookeeperModule.class})
public class AccumuloServerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ServerPrologue.class);
    bind(ServerConfigurationFactory.class).toProvider(ServerPrologue.ServerConfigurationFactoryProvider.class).in(Singleton.class);
    bind(VolumeManager.class).toProvider(ServerPrologue.VolumeManagerProvider.class).in(Singleton.class);
  }
}
