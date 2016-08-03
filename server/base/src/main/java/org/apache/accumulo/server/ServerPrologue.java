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

import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SecurityUtil;

/** Provider for common server objects. */
@Singleton
public class ServerPrologue {
  private final String app;
  private final String hostname;
  private final ServerConfigurationFactory conf;
  private final VolumeManager fs;
  private final Instance instance;

  @Inject
  ServerPrologue(@Named("app") String app, @Named("hostname") String hostname) throws IOException {
    this.app = app;
    this.hostname = hostname;
    this.instance = HdfsZooInstance.getInstance();
    SecurityUtil.serverLogin(SiteConfiguration.getInstance());
    Accumulo.setupLogging(app);
    this.conf = new ServerConfigurationFactory(instance);
    this.fs = VolumeManagerImpl.get();
    Accumulo.init(fs, conf, app);
  }

  @PostConstruct
  void setup() {
    DistributedTrace.enable(hostname, app, conf.getConfiguration());
  }

  @PreDestroy
  void teardown() {
    DistributedTrace.disable();
  }

  @Singleton
  public static class ServerConfigurationFactoryProvider implements Provider<ServerConfigurationFactory> {
    @Inject
    private ServerPrologue prologue;

    @Override
    public ServerConfigurationFactory get() {
      return prologue.conf;
    }
  }

  @Singleton
  public static class VolumeManagerProvider implements Provider<VolumeManager> {
    @Inject
    private ServerPrologue prologue;

    @Override
    public VolumeManager get() {
      return prologue.fs;
    }
  }

  @Singleton
  public static class InstanceProvider implements Provider<Instance> {
    @Inject
    private ServerPrologue prologue;

    @Override
    public Instance get() {
      return prologue.instance;
    }
  }
}
