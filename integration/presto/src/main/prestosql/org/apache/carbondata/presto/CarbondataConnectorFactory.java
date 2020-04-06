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

import java.lang.reflect.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hive.MapredCarbonInputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.presto.impl.CarbonTableConfig;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.airlift.units.DataSize;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.hive.ConnectorObjectNameGeneratorModule;
import io.prestosql.plugin.hive.HiveAnalyzeProperties;
import io.prestosql.plugin.hive.HiveCatalogName;
import io.prestosql.plugin.hive.HiveConnector;
import io.prestosql.plugin.hive.HiveConnectorFactory;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HiveProcedureModule;
import io.prestosql.plugin.hive.HiveSchemaProperties;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.gcs.HiveGcsModule;
import io.prestosql.plugin.hive.metastore.HiveMetastoreModule;
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
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;
import sun.reflect.ConstructorAccessor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbondataConnectorFactory extends HiveConnectorFactory {

  private final ClassLoader classLoader;

  static {
    try {
      setCarbonEnum();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public CarbondataConnectorFactory(String connectorName, ClassLoader classLoader) {
    super(connectorName, classLoader, Optional.empty());
    this.classLoader = requireNonNull(classLoader, "classLoader is null");
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(config, "config is null");

    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      Bootstrap app = new Bootstrap(
          new EventModule(),
          new MBeanModule(),
          new ConnectorObjectNameGeneratorModule(catalogName),
          new JsonModule(),
          new CarbondataModule(catalogName),
          new HiveS3Module(),
          new HiveGcsModule(),
          new HiveMetastoreModule(Optional.ofNullable(null)),
          new HiveSecurityModule(),
          new HiveAuthenticationModule(),
          new HiveProcedureModule(),
          new MBeanServerModule(),
          binder -> {
            binder.bind(NodeVersion.class).toInstance(
                new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
            binder.bind(NodeManager.class).toInstance(context.getNodeManager());
            binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
            binder.bind(TypeManager.class).toInstance(context.getTypeManager());
            binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
            binder.bind(PageSorter.class).toInstance(context.getPageSorter());
            binder.bind(HiveCatalogName.class).toInstance(new HiveCatalogName(catalogName));
            configBinder(binder).bindConfig(CarbonTableConfig.class);
          });

      Injector injector = app
          .strictConfig()
          .doNotInitializeLogging()
          .setRequiredConfigurationProperties(config)
          .initialize();

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
      ConnectorAccessControl accessControl =
          new SystemTableAwareAccessControl(injector.getInstance(ConnectorAccessControl.class));
      Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {
      }));

      return new HiveConnector(lifeCycleManager, metadataFactory, transactionManager,
          new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
          new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
          new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
          new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
          ImmutableSet.of(), procedures, hiveSessionProperties.getSessionProperties(),
          HiveSchemaProperties.SCHEMA_PROPERTIES, hiveTableProperties.getTableProperties(),
          hiveAnalyzeProperties.getAnalyzeProperties(), accessControl, classLoader);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Set the Carbon format enum to HiveStorageFormat, its a hack but for time being it is best
   * choice to avoid lot of code change.
   *
   * @throws Exception
   */
  private static void setCarbonEnum() throws Exception {
    for (HiveStorageFormat format : HiveStorageFormat.values()) {
      if (format.name().equals("CARBON")) {
        return;
      }
    }
    Constructor<?>[] declaredConstructors = HiveStorageFormat.class.getDeclaredConstructors();
    declaredConstructors[0].setAccessible(true);
    Field constructorAccessorField = Constructor.class.getDeclaredField("constructorAccessor");
    constructorAccessorField.setAccessible(true);
    ConstructorAccessor ca =
        (ConstructorAccessor) constructorAccessorField.get(declaredConstructors[0]);
    if (ca == null) {
      Method acquireConstructorAccessorMethod =
          Constructor.class.getDeclaredMethod("acquireConstructorAccessor");
      acquireConstructorAccessorMethod.setAccessible(true);
      ca = (ConstructorAccessor) acquireConstructorAccessorMethod.invoke(declaredConstructors[0]);
    }
    addHiveStorageFormatsForCarbondata(ca, "CARBONDATA");
    addHiveStorageFormatsForCarbondata(ca, "ORG.APACHE.CARBONDATA.FORMAT");
    addHiveStorageFormatsForCarbondata(ca, "CARBON");
    return;
  }

  private static void addHiveStorageFormatsForCarbondata(ConstructorAccessor ca, String storedAs)
      throws InstantiationException, InvocationTargetException, NoSuchFieldException,
      IllegalAccessException {
    Object instance = ca.newInstance(new Object[] { storedAs, HiveStorageFormat.values().length,
        "org.apache.hadoop.hive.serde2.LazySimpleSerde", MapredCarbonInputFormat.class.getName(),
        MapredCarbonOutputFormat.class.getName(), new DataSize(256.0D, DataSize.Unit.MEGABYTE) });
    Field values = HiveStorageFormat.class.getDeclaredField("$VALUES");
    values.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(values, values.getModifiers() & ~Modifier.FINAL);

    HiveStorageFormat[] hiveStorageFormats =
        new HiveStorageFormat[HiveStorageFormat.values().length + 1];
    HiveStorageFormat[] src = (HiveStorageFormat[]) values.get(null);
    System.arraycopy(src, 0, hiveStorageFormats, 0, src.length);
    hiveStorageFormats[src.length] = (HiveStorageFormat) instance;
    values.set(null, hiveStorageFormats);
  }

  @Override public ConnectorHandleResolver getHandleResolver() {
    return new CarbonDataHandleResolver();
  }
}
