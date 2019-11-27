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
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Schema;

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
    final String writePartition = UUID.randomUUID().toString().replace("-", "");
    final String writePath = writeTempPath + "_" + writePartition + "/";
    final CarbonTable table = this.getTable();
    final CarbonTable clonedTable =
        CarbonTable.buildFromTableInfo(TableInfo.deserialize(table.getTableInfo().serialize()));
    clonedTable.getTableInfo().setTablePath(writePath);
    final org.apache.carbondata.sdk.file.CarbonWriter writer;
    try {
      writer = CarbonWriter.builder()
          .outputPath("")
          .writtenBy("flink")
          .withTable(clonedTable)
          .withTableProperties(this.getTableProperties())
          .withJsonInput(this.getTableSchema(clonedTable))
          .build();
    } catch (InvalidLoadOptionException exception) {
      // TODO
      throw new UnsupportedOperationException(exception);
    }
    return new CarbonLocalWriter(this, table, writer, writePath, writePartition);
  }

  @Override
  protected CarbonLocalWriter create0(final String partition) throws IOException {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String writeTempPath = writerProperties.getProperty(CarbonLocalProperty.DATA_TEMP_PATH);
    if (writeTempPath == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonLocalProperty.DATA_TEMP_PATH + "] is not set."
      );
    }
    final String writePath = writeTempPath + "_" + partition + "/";
    final CarbonTable table = this.getTable();
    return new CarbonLocalWriter(this, table, null, writePath, partition);
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

}
