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

import com.google.inject.Injector;
import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.accumulo.core.inject.Decoratee;
import org.apache.accumulo.core.inject.Decorators;
import org.apache.accumulo.core.inject.InjectorBuilder;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SiteConfigurationTest {
  private static boolean isCredentialProviderAvailable;

  @BeforeClass
  public static void checkCredentialProviderAvailable() {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
      isCredentialProviderAvailable = true;
    } catch (Exception e) {
      isCredentialProviderAvailable = false;
    }
  }

  private static class FakeFileConfig extends AccumuloConfiguration {
    private final String cpp;

    @Inject
    @Decoratee
    private AccumuloConfiguration chainNext;

    FakeFileConfig() {
      // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret', 'general.rpc.timeout'=>'timeout'}
      URL keystore = SiteConfigurationTest.class.getResource("/site-cfg.jceks");
      Assert.assertNotNull(keystore);
      String keystorePath = new File(keystore.getFile()).getAbsolutePath();
      this.cpp = "jceks://file" + keystorePath;
    }

    @Override
    public String get(Property property) {
      if (property.equals(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS)) {
        return cpp;
      } else if (property.equals(Property.INSTANCE_SECRET)) {
        return "ignored";
      } else {
        return chainNext.get(property);
      }
    }

    @Override
    public void getProperties(Map<String,String> props, Predicate<String> filter) {
      chainNext.getProperties(props, filter);
      if (filter.test(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey())) {
        props.put(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), cpp);
      }
      if (filter.test(Property.INSTANCE_SECRET.getKey())) {
        props.put(Property.INSTANCE_SECRET.getKey(), "ignored");
      }
    }
  }

  @Inject
  private @Site AccumuloConfiguration siteCfg;
  
  @Test
  public void testOnlySensitivePropertiesExtractedFromCredetialProvider() throws SecurityException, NoSuchMethodException {
    if (!isCredentialProviderAvailable) {
      return;
    }

    Injector inj = InjectorBuilder
        .newRoot()
        .add(ConfigurationModule.class)
        .addRaw(
            Decorators.of(AccumuloConfiguration.class).setBase(DefaultConfiguration.class).decorateWith(FakeFileConfig.class)
                .decorateWith(SensitiveConfiguration.class).buildIn(Singleton.class))
        .buildTestInjector(this);

    Map<String,String> props = new HashMap<>();
    Predicate<String> all = x -> true;
    siteCfg.getProperties(props, all);

    Assert.assertEquals("mysecret", props.get(Property.INSTANCE_SECRET.getKey()));
    Assert.assertEquals(null, props.get("ignored.property"));
    Assert.assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(), props.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
  }
}
