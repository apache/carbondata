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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeEventListener;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.prestosql.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.hive.HiveAnalyzeProperties;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HiveSchemaProperties;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.azure.HiveAzureModule;
import io.prestosql.plugin.hive.gcs.HiveGcsModule;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastoreModule;
import io.prestosql.plugin.hive.procedure.HiveProcedureModule;
import io.prestosql.plugin.hive.rubix.RubixEnabledConfig;
import io.prestosql.plugin.hive.rubix.RubixInitializer;
import io.prestosql.plugin.hive.rubix.RubixModule;
import io.prestosql.plugin.hive.s3.HiveS3Module;
import io.prestosql.plugin.hive.security.HiveSecurityModule;
import io.prestosql.plugin.hive.security.SystemTableAwareAccessControl;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.carbondata.presto.impl.CarbonTableConfig;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Similar class to InternalHiveConnectorFactory,
 * to handle the module bindings for carbonData connector.
 */
public final class InternalCarbonDataConnectorFactory {
  private InternalCarbonDataConnectorFactory() {
  }

  public static Connector createConnector(String catalogName, Map<String, String> config,
      ConnectorContext context, Module module) {
    return createConnector(catalogName, config, context, module, Optional.empty());
  }

  public static Connector createConnector(String catalogName, Map<String, String> config,
      ConnectorContext context, Module module, Optional<HiveMetastore> metastore) {
    requireNonNull(config, "config is null");

    ClassLoader classLoader = InternalCarbonDataConnectorFactory.class.getClassLoader();
    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      Bootstrap app = new Bootstrap(
          new EventModule(),
          new MBeanModule(),
          new ConnectorObjectNameGeneratorModule(catalogName, "io.prestosql.plugin.carbondata",
              "presto.plugin.carbondata"),
          new JsonModule(),
          new CarbondataModule(),
          new HiveS3Module(),
          new HiveGcsModule(),
          new HiveAzureModule(),
          installModuleIf(RubixEnabledConfig.class, RubixEnabledConfig::isCacheEnabled,
              new RubixModule()),
          new HiveMetastoreModule(metastore),
          new HiveSecurityModule(catalogName),
          new HiveAuthenticationModule(),
          new HiveProcedureModule(),
          new MBeanServerModule(),
          binder -> {
          binder.bind(NodeVersion.class)
              .toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
          binder.bind(NodeManager.class).toInstance(context.getNodeManager());
          binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
          binder.bind(TypeManager.class).toInstance(context.getTypeManager());
          binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
          binder.bind(PageSorter.class).toInstance(context.getPageSorter());
          binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
          configBinder(binder).bindConfig(CarbonTableConfig.class);
          },
          binder -> newSetBinder(binder, EventListener.class), module);

      Injector injector =
          app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
              .initialize();

      if (injector.getInstance(RubixEnabledConfig.class).isCacheEnabled()) {
        // RubixInitializer needs ConfigurationInitializers, hence kept outside RubixModule
        RubixInitializer rubixInitializer = injector.getInstance(RubixInitializer.class);
        rubixInitializer.initializeRubix(context.getNodeManager());
      }

      LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
      HiveMetadataFactory metadataFactory = injector.getInstance(HiveMetadataFactory.class);
      HiveTransactionManager transactionManager =
          injector.getInstance(HiveTransactionManager.class);
      ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
      ConnectorPageSourceProvider connectorPageSource =
          injector.getInstance(ConnectorPageSourceProvider.class);
      ConnectorPageSinkProvider pageSinkProvider =
          injector.getInstance(ConnectorPageSinkProvider.class);
      ConnectorNodePartitioningProvider connectorDistributionProvider =
          injector.getInstance(ConnectorNodePartitioningProvider.class);
      HiveSessionProperties hiveSessionProperties =
          injector.getInstance(HiveSessionProperties.class);
      HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
      HiveAnalyzeProperties hiveAnalyzeProperties =
          injector.getInstance(HiveAnalyzeProperties.class);
      ConnectorAccessControl accessControl = new ClassLoaderSafeConnectorAccessControl(
          new SystemTableAwareAccessControl(injector.getInstance(ConnectorAccessControl.class)),
          classLoader);
      Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {
      }));
      Set<SystemTable> systemTables =
          injector.getInstance(Key.get(new TypeLiteral<Set<SystemTable>>() {
          }));
      Set<EventListener> eventListeners =
          injector.getInstance(Key.get(new TypeLiteral<Set<EventListener>>() {
          })).stream().map(listener -> new ClassLoaderSafeEventListener(listener, classLoader))
              .collect(toImmutableSet());

      return new CarbonDataConnector(lifeCycleManager, metadataFactory, transactionManager,
          new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
          new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
          new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
          new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
          systemTables, procedures, eventListeners, hiveSessionProperties.getSessionProperties(),
          HiveSchemaProperties.SCHEMA_PROPERTIES, hiveTableProperties.getTableProperties(),
          hiveAnalyzeProperties.getAnalyzeProperties(), accessControl, classLoader);
    }
  }
}
