/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto;

import java.util.Map;

import static java.util.Objects.requireNonNull;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;


/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory implements ConnectorFactory {

  private final String name;
  private final ClassLoader classLoader;

  public CarbondataConnectorFactory(String connectorName, ClassLoader classLoader) {
    this.name = connectorName;
    this.classLoader = requireNonNull(classLoader, "classLoader is null");
  }

  @Override public String getName() {
    return name;
  }

  @Override public ConnectorHandleResolver getHandleResolver() {
    return new CarbondataHandleResolver();
  }

  @Override public Connector create(String connectorId, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(config, "config is null");

    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      Bootstrap app = new Bootstrap(new JsonModule(),
          new CarbondataModule(connectorId, context.getTypeManager()));

      Injector injector =
          app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
              .initialize();

      LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
      ConnectorMetadata metadata = injector.getInstance(CarbondataMetadata.class);
      ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
      ConnectorPageSourceProvider connectorPageSource =
          injector.getInstance(ConnectorPageSourceProvider.class);

      return new CarbondataConnector(lifeCycleManager,
          new ClassLoaderSafeConnectorMetadata(metadata, classLoader),
          new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader), classLoader,
          new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}