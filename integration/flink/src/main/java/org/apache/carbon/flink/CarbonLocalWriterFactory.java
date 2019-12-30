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
import java.util.Properties;
import java.util.UUID;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

public final class CarbonLocalWriterFactory extends CarbonWriterFactory {

  private static final long serialVersionUID = 2822670807460968078L;

  @Override
  public String getType() {
    return CarbonLocalWriterFactoryBuilder.TYPE;
  }

  @Override
  protected CarbonLocalWriter create0() throws IOException {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String writeTempPath = writerProperties.getProperty(CarbonLocalProperty.DATA_TEMP_PATH);
    if (writeTempPath == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonLocalProperty.DATA_TEMP_PATH + "] is not set."
      );
    }
    final String writerIdentifier = UUID.randomUUID().toString();
    final String writePath = writeTempPath + writerIdentifier.replace("-", "") + "/";
    final CarbonTable table = this.getTable();
    return new CarbonLocalWriter(this, writerIdentifier, table, writePath);
  }

  @Override
  protected CarbonLocalWriter create0(final String identifier, final String path)
      throws IOException {
    return new CarbonLocalWriter(this, identifier, this.getTable(), path);
  }

}
