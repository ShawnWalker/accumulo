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
import org.apache.accumulo.core.inject.Decoratee;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class XmlFileConfiguration extends AccumuloConfiguration {
  private final static Logger log = LoggerFactory.getLogger(XmlFileConfiguration.class);
  private final Configuration xmlConfig;

  @Inject
  @Decoratee
  AccumuloConfiguration chainNext;

  XmlFileConfiguration() {
    this.xmlConfig = new Configuration(false);
    String configFile = System.getProperty("org.apache.accumulo.config.file", "accumulo-site.xml");

    if (XmlFileConfiguration.class.getClassLoader().getResource(configFile) == null)
      log.warn(configFile + " not found on classpath", new Throwable());
    else
      xmlConfig.addResource(configFile);
  }

  @Override
  public String get(Property property) {
    String value = xmlConfig.get(property.getKey());

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + property.getKey() + " due to improperly formatted " + property.getType() + ": " + value);
      value = chainNext.get(property);
    }
    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    chainNext.getProperties(props, filter);
    for (Map.Entry<String,String> entry : xmlConfig)
      if (filter.test(entry.getKey()))
        props.put(entry.getKey(), entry.getValue());
  }
}
