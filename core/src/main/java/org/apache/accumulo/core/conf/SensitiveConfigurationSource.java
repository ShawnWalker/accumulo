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

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;
import javax.inject.Inject;
import static org.apache.accumulo.core.conf.SiteConfiguration.log;
import org.apache.accumulo.core.inject.Decoratee;
import org.apache.hadoop.conf.Configuration;

public class SensitiveConfigurationSource implements ConfigurationSource {
  @Inject
  @Decoratee ConfigurationSource chainNext;
  
  @Override
  public String get(Property property) {
    String key = property.getKey();

    // If the property is sensitive, see if CredentialProvider was configured.
    if (property.isSensitive()) {
      Configuration hadoopConf = getHadoopConfiguration();
      if (null != hadoopConf) {
        // Try to find the sensitive value from the CredentialProvider
        try {
          char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
          if (null != value) {
            return new String(value);
          }
        } catch (IOException e) {
          log.warn("Failed to extract sensitive property (" + key + ") from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
        }
      }
    }
    return chainNext.get(property);
  }

  @Override
  public void getProperties(Map<String, String> props, Predicate<String> filter) {
    chainNext.getProperties(props, filter);
    
    // CredentialProvider should take precedence over site
    Configuration hadoopConf = getHadoopConfiguration();
    if (null != hadoopConf) {
      try {
        for (String key : CredentialProviderFactoryShim.getKeys(hadoopConf)) {
          if (!Property.isValidPropertyKey(key) || !Property.isSensitive(key)) {
            continue;
          }

          if (filter.test(key)) {
            char[] value = CredentialProviderFactoryShim.getValueFromCredentialProvider(hadoopConf, key);
            if (null != value) {
              props.put(key, new String(value));
            }
          }
        }
      } catch (IOException e) {
        log.warn("Failed to extract sensitive properties from Hadoop CredentialProvider, falling back to accumulo-site.xml", e);
      }
    }
  }
  
  protected Configuration getHadoopConfiguration() {
    String credProviderPathsValue = chainNext.get(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS);

    if (null != credProviderPathsValue) {
      // We have configuration for a CredentialProvider
      // Try to pull the sensitive password from there
      Configuration conf = new Configuration(CachedConfiguration.getInstance());
      conf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, credProviderPathsValue);
      return conf;
    }

    return null;
  }
  
}
