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

import java.util.Map;
import java.util.function.Predicate;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.accumulo.core.inject.StaticFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AccumuloConfiguration} which loads properties from an XML file, usually accumulo-site.xml. This implementation supports defaulting undefined
 * property values to a parent configuration's definitions.
 * <p>
 * The system property "org.apache.accumulo.config.file" can be used to specify the location of the XML configuration file on the classpath. If the system
 * property is not defined, it defaults to "accumulo-site.xml".
 * <p>
 * This class is a singleton.
 * <p>
 * <b>Note</b>: Client code should not use this class, and it may be deprecated in the future.
 */
@Singleton
public class SiteConfiguration extends AccumuloConfiguration {
  static final Logger log = LoggerFactory.getLogger(SiteConfiguration.class);

  private ConfigurationSource source;
  
  @Inject
  SiteConfiguration(ConfigurationSource source) {
    this.source = source;
  }
  
  @Deprecated
  synchronized public static SiteConfiguration getInstance() {
    return StaticFactory.getInstance(SiteConfiguration.class);
  }

  @Override
  public String get(Property property) {
    return source.get(property);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    source.getProperties(props, filter);
  }
}
