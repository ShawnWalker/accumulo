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
package org.apache.accumulo.core.conf;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import javax.inject.Singleton;
import org.apache.accumulo.core.inject.DecoratorModuleBuilder;
import org.apache.accumulo.core.inject.Requires;

@Requires(ConfigurationModule.class)
public class SiteConfigurationModule extends AbstractModule {
  /**
   * Equivalent to {@code @Site AccumuloConfiguration}, the bound key for accessing site configuration. This instance of {@link AccumuloConfiguration} loads
   * properties from an XML file, usually accumulo-site.xml.
   * <p>
   * The system property "org.apache.accumulo.config.file" can be used to specify the location of the XML configuration file on the classpath. If the system
   * property is not defined, it defaults to "accumulo-site.xml".
   * <p>
   * This should be implemented by a singleton.
   * <p>
   * <b>Note</b>: Client code should not use this binding, and it may be deprecated in the future.
   */
  public static final Key<AccumuloConfiguration> KEY = Key.get(AccumuloConfiguration.class, Site.class);

  @Override
  protected void configure() {
    install(DecoratorModuleBuilder.of(KEY).buildChain(DefaultConfiguration.class, XmlFileConfiguration.class, SensitiveConfiguration.class).in(Singleton.class));
  }
}
