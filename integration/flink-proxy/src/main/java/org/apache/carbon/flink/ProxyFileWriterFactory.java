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

package org.apache.carbon.flink;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.serialization.BulkWriter;

public abstract class ProxyFileWriterFactory<OUT> implements BulkWriter.Factory<OUT> {

  private static final long serialVersionUID = -1449889091046572219L;

  private static final Map<String, Class<? extends ProxyFileWriterFactory>> FACTORY_MAP =
      new ConcurrentHashMap<>();

  public static ProxyFileWriterFactory newInstance(final String factoryType) {
    if (factoryType == null) {
      throw new IllegalArgumentException("Argument [factoryType] is null.");
    }
    final Class<? extends ProxyFileWriterFactory> factoryClass = FACTORY_MAP.get(factoryType);
    if (factoryClass == null) {
      return null;
    }
    try {
      return factoryClass.newInstance();
    } catch (InstantiationException | IllegalAccessException exception) {
      throw new RuntimeException(exception);
    }
  }

  public static void register(
        final String factoryType,
        final Class<? extends ProxyFileWriterFactory> factoryClass
  ) {
    if (factoryType == null) {
      throw new IllegalArgumentException("Argument [factoryType] is null.");
    }
    if (factoryClass == null) {
      throw new IllegalArgumentException("Argument [factoryClass] is null.");
    }
    // TODO 检查参数
    // TODO 检查是否已被注册，重复注册，直接忽略
    FACTORY_MAP.put(factoryType, factoryClass);
  }

  private Configuration configuration;

  public abstract String getType();

  public Configuration getConfiguration() {
    return this.configuration;
  }

  public void setConfiguration(final Configuration configuration) {
    this.configuration = configuration;
  }

  public abstract ProxyFileWriter<OUT> create(String identifier, String path) throws IOException;

  public static class Configuration implements Serializable {

    private static final long serialVersionUID = 8615149992583690295L;

    public Configuration(
        final String databaseName,
        final String tableName,
        final String tablePath,
        final Properties tableProperties,
        final Properties writerProperties,
        final Properties carbonProperties
    ) {
      if (tableName == null) {
        throw new IllegalArgumentException("Argument [tableName] is null.");
      }
      if (tablePath == null) {
        throw new IllegalArgumentException("Argument [tablePath] is null.");
      }
      if (tableProperties == null) {
        throw new IllegalArgumentException("Argument [tableProperties] is null.");
      }
      if (writerProperties == null) {
        throw new IllegalArgumentException("Argument [writerProperties] is null.");
      }
      if (carbonProperties == null) {
        throw new IllegalArgumentException("Argument [carbonProperties] is null.");
      }
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.tablePath = tablePath;
      this.tableProperties = tableProperties;
      this.writerProperties = writerProperties;
      this.carbonProperties = carbonProperties;
    }

    private final String databaseName;

    private final String tableName;

    private final String tablePath;

    private final Properties tableProperties;

    private final Properties writerProperties;

    private final Properties carbonProperties;

    public String getDatabaseName() {
      return this.databaseName;
    }

    public String getTableName() {
      return this.tableName;
    }

    public String getTablePath() {
      return this.tablePath;
    }

    public Properties getTableProperties() {
      return this.tableProperties;
    }

    public Properties getWriterProperties() {
      return this.writerProperties;
    }

    public Properties getCarbonProperties() {
      return this.carbonProperties;
    }

  }

}
