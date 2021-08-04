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

package org.apache.carbondata.trino;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.hive.CarbonHiveSerDe;
import org.apache.carbondata.hive.MapredCarbonInputFormat;
import org.apache.carbondata.hive.MapredCarbonOutputFormat;
import org.apache.carbondata.trino.impl.CarbonTableConfig;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.airlift.units.DataSize;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorAccessControl;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeEventListener;
import io.trino.plugin.base.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveAnalyzeProperties;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.HiveConnectorFactory;
import io.trino.plugin.hive.HiveHdfsModule;
import io.trino.plugin.hive.HiveMaterializedViewPropertiesProvider;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.InternalHiveConnectorFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.TransactionalMetadataFactory;
import io.trino.plugin.hive.authentication.HiveAuthenticationModule;
import io.trino.plugin.hive.azure.HiveAzureModule;
import io.trino.plugin.hive.gcs.HiveGcsModule;
import io.trino.plugin.hive.metastore.HiveMetastoreModule;
import io.trino.plugin.hive.procedure.HiveProcedureModule;
import io.trino.plugin.hive.rubix.RubixEnabledConfig;
import io.trino.plugin.hive.rubix.RubixModule;
import io.trino.plugin.hive.s3.HiveS3Module;
import io.trino.plugin.hive.security.HiveSecurityModule;
import io.trino.plugin.hive.security.SystemTableAwareAccessControl;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;
import sun.misc.Unsafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.InternalHiveConnectorFactory.bindSessionPropertiesProvider;

/**
 * Build Carbondata Connector
 * It will be called by CarbondataPlugin
 */
public class CarbonDataConnectorFactory extends HiveConnectorFactory {

  static {
    try {
      setCarbonEnum();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final Class<? extends Module> module;

  public CarbonDataConnectorFactory(String name) {
    this(name, EmptyModule.class);
  }

  public CarbonDataConnectorFactory(String name, Class<? extends Module> module) {
    super(name, module);
    checkArgument(!isNullOrEmpty(name), "name is null or empty");
    this.module = module;
  }

  /**
   * Set the Carbon format enum to HiveStorageFormat, its a hack but for time
   * being it is best
   * choice to avoid lot of code change.
   *
   * @throws Exception
   */
  private static void setCarbonEnum() throws Exception {
    for (HiveStorageFormat format : HiveStorageFormat.values()) {
      if (format.name().equals("CARBON") || format.name().equals("ORG.APACHE.CARBONDATA.FORMAT")
          || format.name().equals("CARBONDATA")) {
        return;
      }
    }
    addHiveStorageFormatsForCarbonData("CARBON");
    addHiveStorageFormatsForCarbonData("ORG.APACHE.CARBONDATA.FORMAT");
    addHiveStorageFormatsForCarbonData("CARBONDATA");
  }

  private static void addHiveStorageFormatsForCarbonData(String storedAs) throws Exception {
    Constructor<?> constructor = Unsafe.class.getDeclaredConstructors()[0];
    constructor.setAccessible(true);
    Unsafe unsafe = (Unsafe) constructor.newInstance();
    HiveStorageFormat enumValue =
        (HiveStorageFormat) unsafe.allocateInstance(HiveStorageFormat.class);

    Field nameField = Enum.class.getDeclaredField("name");
    makeAccessible(nameField);
    nameField.set(enumValue, storedAs);

    Field ordinalField = Enum.class.getDeclaredField("ordinal");
    makeAccessible(ordinalField);
    ordinalField.setInt(enumValue, HiveStorageFormat.values().length);

    Field serdeField = HiveStorageFormat.class.getDeclaredField("serde");
    makeAccessible(serdeField);
    serdeField.set(enumValue, CarbonHiveSerDe.class.getName());

    Field inputFormatField = HiveStorageFormat.class.getDeclaredField("inputFormat");
    makeAccessible(inputFormatField);
    inputFormatField.set(enumValue, MapredCarbonInputFormat.class.getName());

    Field outputFormatField = HiveStorageFormat.class.getDeclaredField("outputFormat");
    makeAccessible(outputFormatField);
    outputFormatField.set(enumValue, MapredCarbonOutputFormat.class.getName());

    Field estimatedWriterSystemMemoryUsageField =
        HiveStorageFormat.class.getDeclaredField("estimatedWriterSystemMemoryUsage");
    makeAccessible(estimatedWriterSystemMemoryUsageField);
    estimatedWriterSystemMemoryUsageField.set(enumValue, DataSize.of(256, MEGABYTE));

    Field values = HiveStorageFormat.class.getDeclaredField("$VALUES");
    makeAccessible(values);
    HiveStorageFormat[] hiveStorageFormats =
        new HiveStorageFormat[HiveStorageFormat.values().length + 1];
    HiveStorageFormat[] src = (HiveStorageFormat[]) values.get(null);
    System.arraycopy(src, 0, hiveStorageFormats, 0, src.length);
    hiveStorageFormats[src.length] = enumValue;
    values.set(null, hiveStorageFormats);
  }

  private static void makeAccessible(Field field) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config,
      ConnectorContext context) {
    requireNonNull(config, "config is null");
    ClassLoader classLoader = InternalHiveConnectorFactory.class.getClassLoader();
    try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
      Module moduleInstance =
          (Module) classLoader.loadClass(module.getName()).getConstructor().newInstance();
      Bootstrap app = new Bootstrap(new EventModule(), new MBeanModule(),
          new ConnectorObjectNameGeneratorModule(catalogName, "io.trino.plugin.carbondata",
              "trino.plugin.carbondata"), new JsonModule(), new HiveHdfsModule(),
          new CarbonDataModule(catalogName), new HiveS3Module(), new HiveGcsModule(),
          new HiveAzureModule(),
          installModuleIf(RubixEnabledConfig.class, RubixEnabledConfig::isCacheEnabled,
              new RubixModule()), new HiveMetastoreModule(Optional.empty()),
          new HiveSecurityModule(catalogName), new HiveAuthenticationModule(),
          new HiveProcedureModule(), new MBeanServerModule(), binder -> {
        binder.bind(NodeVersion.class)
            .toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
        binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder());
        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
        binder.bind(PageSorter.class).toInstance(context.getPageSorter());
        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
      }, binder -> configBinder(binder).bindConfig(CarbonTableConfig.class),
          binder -> newSetBinder(binder, EventListener.class),
          binder -> bindSessionPropertiesProvider(binder, HiveSessionProperties.class),
          moduleInstance);

      Injector injector =
          app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
              .initialize();

      LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
      TransactionalMetadataFactory metadataFactory =
          injector.getInstance(TransactionalMetadataFactory.class);
      HiveTransactionManager transactionManager =
          injector.getInstance(HiveTransactionManager.class);
      ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
      ConnectorPageSourceProvider connectorPageSource =
          injector.getInstance(ConnectorPageSourceProvider.class);
      ConnectorPageSinkProvider pageSinkProvider =
          injector.getInstance(ConnectorPageSinkProvider.class);
      ConnectorNodePartitioningProvider connectorDistributionProvider =
          injector.getInstance(ConnectorNodePartitioningProvider.class);
      Set<SessionPropertiesProvider> sessionPropertiesProviders =
          injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {
          }));
      HiveTableProperties hiveTableProperties = injector.getInstance(HiveTableProperties.class);
      HiveAnalyzeProperties hiveAnalyzeProperties =
          injector.getInstance(HiveAnalyzeProperties.class);
      HiveMaterializedViewPropertiesProvider hiveMaterializedViewPropertiesProvider =
          injector.getInstance(HiveMaterializedViewPropertiesProvider.class);
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

      return new HiveConnector(lifeCycleManager, metadataFactory, transactionManager,
          new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
          new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
          new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
          new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
          systemTables, procedures, eventListeners, sessionPropertiesProviders,
          HiveSchemaProperties.SCHEMA_PROPERTIES, hiveTableProperties.getTableProperties(),
          hiveAnalyzeProperties.getAnalyzeProperties(),
          hiveMaterializedViewPropertiesProvider.getMaterializedViewProperties(), accessControl,
          classLoader);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new CarbonDataHandleResolver();
  }
}
