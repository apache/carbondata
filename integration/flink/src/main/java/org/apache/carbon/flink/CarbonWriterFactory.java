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

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.flink.core.fs.FSDataOutputStream;

public abstract class CarbonWriterFactory extends ProxyFileWriterFactory<Object[]> {

  public static CarbonWriterFactoryBuilder builder(final String type) {
    return CarbonWriterFactoryBuilder.get(type);
  }

  @Override
  public CarbonWriter create(final FSDataOutputStream out) throws IOException {
    if (!(out instanceof ProxyRecoverableOutputStream)) {
      throw new IllegalArgumentException(
              "Only support " + ProxyRecoverableOutputStream.class.getName() + "."
      );
    }
    this.setCarbonProperties();
    final CarbonWriter writer = this.create0();
    ((ProxyRecoverableOutputStream) out).bind(writer);
    return writer;
  }

  @Override
  public CarbonWriter create(final String identifier, final String path) throws IOException {
    this.setCarbonProperties();
    return this.create0(identifier, path);
  }

  protected abstract CarbonWriter create0() throws IOException;

  protected abstract CarbonWriter create0(String identifier, String path) throws IOException;

  protected CarbonTable getTable() throws IOException {
    final Configuration configuration = this.getConfiguration();
    return CarbonTable.buildFromTablePath(
        configuration.getTableName(),
        configuration.getDatabaseName(),
        configuration.getTablePath(),
        null
    );
  }

  private void setCarbonProperties() {
    final CarbonProperties carbonProperties = CarbonProperties.getInstance();
    for (String propertyName :
            this.getConfiguration().getCarbonProperties().stringPropertyNames()) {
      carbonProperties.addProperty(
          propertyName,
          this.getConfiguration().getCarbonProperties().getProperty(propertyName)
      );
    }
  }

}
