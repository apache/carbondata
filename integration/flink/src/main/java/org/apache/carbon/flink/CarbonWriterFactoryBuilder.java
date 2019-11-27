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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

public abstract class CarbonWriterFactoryBuilder {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonWriterFactoryBuilder.class.getName());

  private static final Map<String, CarbonWriterFactoryBuilder> BUILDER_MAP;

  static {
    final Map<String, CarbonWriterFactoryBuilder> builderMap = new HashMap<>();
    final ServiceLoader<CarbonWriterFactoryBuilder> builderLoader =
        ServiceLoader.load(CarbonWriterFactoryBuilder.class);
    for (CarbonWriterFactoryBuilder builder :builderLoader) {
      builderMap.put(builder.getType(), builder);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Added carbon writer factory builder. " + builder.getClass().getName());
      }
    }
    BUILDER_MAP = Collections.unmodifiableMap(builderMap);
  }

  public static CarbonWriterFactoryBuilder get(final String type) {
    if (type == null) {
      throw new IllegalArgumentException("Argument [type] is null.");
    }
    CarbonWriterFactoryBuilder builder = BUILDER_MAP.get(type);
    if (builder == null) {
      if (type.equalsIgnoreCase(CarbonS3WriterFactoryBuilder.TYPE)) {
        return new CarbonS3WriterFactoryBuilder();
      }
    }
    return builder;
  }

  public abstract String getType();

  public abstract CarbonWriterFactory build(
      String databaseName,
      String tableName,
      String tablePath,
      Properties tableProperties,
      Properties writerProperties,
      Properties carbonProperties
  );

}
