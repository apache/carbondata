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

package org.apache.carbondata.core.metadata.schema.table;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Builder for {@link CarbonTable}
 */
public class CarbonTableBuilder {

  private String tableName;
  private String databaseName;
  private String tablePath;
  private boolean isTransactionalTable;
  private TableSchema tableSchema;

  public CarbonTableBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public CarbonTableBuilder databaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  public CarbonTableBuilder tablePath(String tablePath) {
    Objects.requireNonNull(tablePath, "tablePath should not be null");
    this.tablePath = tablePath;
    return this;
  }

  public CarbonTableBuilder isTransactionalTable(boolean isTransactionalTable) {
    Objects.requireNonNull(isTransactionalTable, "Transactional Table should not be null");
    this.isTransactionalTable = isTransactionalTable;
    return this;
  }

  public CarbonTableBuilder tableSchema(TableSchema tableSchema) {
    Objects.requireNonNull(tableSchema, "tableSchema should not be null");
    this.tableSchema = tableSchema;
    return this;
  }

  public CarbonTable build() {
    Objects.requireNonNull(tablePath, "tablePath should not be null");
    Objects.requireNonNull(tableSchema, "tableSchema should not be null");
    Objects.requireNonNull(isTransactionalTable, "Transactional Table should not be null");


    TableInfo tableInfo = new TableInfo();
    tableInfo.setDatabaseName(databaseName);
    tableInfo.setTableUniqueName(CarbonTable.buildUniqueName(databaseName, tableName));
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTablePath(tablePath);
    tableInfo.setTransactionalTable(isTransactionalTable);
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setDataMapSchemaList(new ArrayList<DataMapSchema>(0));
    return CarbonTable.buildFromTableInfo(tableInfo);
  }
}
