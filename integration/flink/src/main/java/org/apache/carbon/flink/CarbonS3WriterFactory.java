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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Schema;

public final class CarbonS3WriterFactory extends CarbonWriterFactory {

  private static final long serialVersionUID = 2302824357711095245L;

  @Override
  public String getType() {
    return CarbonS3WriterFactoryBuilder.TYPE;
  }

  @Override
  protected CarbonS3Writer create0() throws IOException {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String writeTempPath = writerProperties.getProperty(CarbonS3Property.DATA_TEMP_PATH);
    if (writeTempPath == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.DATA_TEMP_PATH + "] is not set."
      );
    }
    final String writePartition = UUID.randomUUID().toString().replace("-", "");
    final String writePath = writeTempPath + "_" + writePartition + "/";
    final CarbonTable table = this.getTable();
    final CarbonTable clonedTable =
        CarbonTable.buildFromTableInfo(TableInfo.deserialize(table.getTableInfo().serialize()));
    clonedTable.getTableInfo().setTablePath(writePath);
    final org.apache.hadoop.conf.Configuration configuration = this.getS3Configuration();
    final CarbonWriter writer;
    try {
      writer = CarbonWriter.builder()
          .outputPath("")
          .writtenBy("flink")
          .withTable(clonedTable)
          .withTableProperties(this.getTableProperties())
          .withJsonInput(this.getTableSchema(clonedTable))
          .withHadoopConf(configuration)
          .build();
    } catch (InvalidLoadOptionException exception) {
      // TODO
      throw new UnsupportedOperationException(exception);
    }
    return new CarbonS3Writer(this, table, writer, writePath, writePartition, configuration);
  }

  @Override
  protected CarbonS3Writer create0(final String partition) throws IOException {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String writeTempPath = writerProperties.getProperty(CarbonS3Property.DATA_TEMP_PATH);
    if (writeTempPath == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.DATA_TEMP_PATH + "] is not set."
      );
    }
    final String writePath = writeTempPath + "_" + partition + "/";
    final CarbonTable table = this.getTable();
    final org.apache.hadoop.conf.Configuration configuration = this.getS3Configuration();
    return new CarbonS3Writer(this, table, null, writePath, partition, configuration);
  }

  @Override
  protected CarbonTable getTable() throws IOException {
    this.setS3Configuration(this.getS3Configuration());
    return super.getTable();
  }

  private Schema getTableSchema(final CarbonTable table) {
    final List<CarbonColumn> columnList = table.getCreateOrderColumn(table.getTableName());
    final List<ColumnSchema> columnSchemaList = new ArrayList<>(columnList.size());
    for (CarbonColumn column : columnList) {
      columnSchemaList.add(column.getColumnSchema());
    }
    return new Schema(columnSchemaList);
  }

  private Map<String, String> getTableProperties() {
    final Properties tableProperties = this.getConfiguration().getTableProperties();
    final Map<String, String> tablePropertyMap = new HashMap<>(tableProperties.size());
    for (String propertyName : tableProperties.stringPropertyNames()) {
      tablePropertyMap.put(propertyName, tableProperties.getProperty(propertyName));
    }
    return tablePropertyMap;
  }

  private org.apache.hadoop.conf.Configuration getS3Configuration() {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String accessKey = writerProperties.getProperty(CarbonS3Property.ACCESS_KEY);
    final String secretKey = writerProperties.getProperty(CarbonS3Property.SECRET_KEY);
    final String endpoint = writerProperties.getProperty(CarbonS3Property.ENDPOINT);
    if (accessKey == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.ACCESS_KEY + "] is not set."
      );
    }
    if (secretKey == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.SECRET_KEY + "] is not set."
      );
    }
    if (endpoint == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.ENDPOINT + "] is not set."
      );
    }
    final org.apache.hadoop.conf.Configuration configuration
            = new org.apache.hadoop.conf.Configuration(true);
    configuration.set("fs.s3.access.key", accessKey);
    configuration.set("fs.s3.secret.key", secretKey);
    configuration.set("fs.s3.endpoint", endpoint);
    configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("fs.s3a.access.key", accessKey);
    configuration.set("fs.s3a.secret.key", secretKey);
    configuration.set("fs.s3a.endpoint", endpoint);
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return configuration;
  }

  private void setS3Configuration(final org.apache.hadoop.conf.Configuration configuration) {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
  }

}
