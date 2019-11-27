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

import java.util.Properties;

public final class CarbonS3WriterFactoryBuilder extends CarbonWriterFactoryBuilder {

  public static final String TYPE = "S3";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public CarbonS3WriterFactory build(
      final String databaseName,
      final String tableName,
      final String tablePath,
      final Properties tableProperties,
      final Properties writerProperties,
      final Properties carbonProperties
  ) {
    final CarbonS3WriterFactory factory = new CarbonS3WriterFactory();
    factory.setConfiguration(
        new ProxyFileWriterFactory.Configuration(
            databaseName,
            tableName,
            tablePath,
            tableProperties,
            writerProperties,
            carbonProperties
        )
    );
    return factory;
  }

}
