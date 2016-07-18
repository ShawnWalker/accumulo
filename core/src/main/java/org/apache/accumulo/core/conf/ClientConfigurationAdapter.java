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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import javax.inject.Inject;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.inject.Decoratee;
import org.apache.accumulo.core.rpc.SaslConnectionParams;

public class ClientConfigurationAdapter extends AccumuloConfiguration {
  @Inject
  @Decoratee
  AccumuloConfiguration chainNext;

  @Inject
  ClientConfiguration clientConf;

  @Override
  public String get(Property property) {
    String key = property.getKey();
    if (clientConf.containsKey(key)) {
      return clientConf.getString(key);
    } else if (Property.GENERAL_KERBEROS_PRINCIPAL == property) {
      // Reconstitute the server kerberos property from the client config
      if (clientConf.containsKey(ClientProperty.KERBEROS_SERVER_PRIMARY.getKey())) {
        return clientConf.getString(ClientProperty.KERBEROS_SERVER_PRIMARY.getKey()) + "/_HOST@" + SaslConnectionParams.getDefaultRealm();
      }
    }
    return chainNext.get(property);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    chainNext.getProperties(props, filter);

    Iterator<?> keyIter = clientConf.getKeys();
    while (keyIter.hasNext()) {
      String key = keyIter.next().toString();
      if (filter.test(key))
        props.put(key, clientConf.getString(key));
    }

    // Two client props that don't exist on the server config. Client doesn't need to know about the Kerberos instance from the principle, but servers do
    // Automatically reconstruct the server property when converting a client config.
    if (props.containsKey(ClientProperty.KERBEROS_SERVER_PRIMARY.getKey())) {
      final String serverPrimary = props.remove(ClientProperty.KERBEROS_SERVER_PRIMARY.getKey());
      if (filter.test(Property.GENERAL_KERBEROS_PRINCIPAL.getKey())) {
        // Use the _HOST expansion. It should be unnecessary in "client land".
        props.put(Property.GENERAL_KERBEROS_PRINCIPAL.getKey(), serverPrimary + "/_HOST@" + SaslConnectionParams.getDefaultRealm());
      }
    }
  }
}
