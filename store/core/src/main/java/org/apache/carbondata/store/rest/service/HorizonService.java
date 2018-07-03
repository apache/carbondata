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

package org.apache.carbondata.store.rest.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.store.exception.StoreException;
import org.apache.carbondata.store.master.Master;
import org.apache.carbondata.store.rest.model.dto.Load;
import org.apache.carbondata.store.rest.model.dto.Select;
import org.apache.carbondata.store.rest.model.dto.Table;

public class HorizonService {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(HorizonService.class.getName());

  private static HorizonService instance;

  private Master master;

  private HorizonService() {
    master = Master.getInstance(null);
  }

  public boolean createTable(Table table) throws StoreException {
    TableSchemaBuilder builder = TableSchema.builder();
    builder.tableName(table.getName()).properties(table.getProperties());

    Field[] fields = table.getSchema().getFields();
    // sort_columns
    List<String> sortColumnsList = ServiceUtil.prepareSortColumns(fields, table.getProperties());
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];
    // table schema
    CarbonWriterBuilder.buildTableSchema(fields, builder, sortColumnsList, sortColumnsSchemaList);
    builder.setSortColumns(Arrays.asList(sortColumnsSchemaList));

    TableSchema schema = builder.build();
    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    schema.getSchemaEvolution().getSchemaEvolutionEntryList().add(schemaEvolutionEntry);
    schema.setTableName(table.getName());

    String tablePath = master.getTableFolder(table.getDatabase(), table.getName());
    TableInfo tableInfo = CarbonTable
        .builder()
        .databaseName(table.getDatabase())
        .tableName(table.getName())
        .tablePath(tablePath)
        .tableSchema(schema)
        .isTransactionalTable(true)
        .buildTableInfo();

    try {
      return master.createTable(tableInfo, table.isIfNotExists());
    } catch (IOException e) {
      LOGGER.error(e, "create table failed");
      throw new StoreException(e.getMessage());
    }
  }

  public boolean loadData(Load load) throws StoreException {
    CarbonTable table = master.getTable(load.getDatabaseName(), load.getTableName());
    CarbonLoadModelBuilder modelBuilder = new CarbonLoadModelBuilder(table);
    modelBuilder.setInputPath(load.getInputPath());
    try {
      CarbonLoadModel model =
          modelBuilder.build(load.getOptions(), System.currentTimeMillis(), "0");

      return master.loadData(model, load.isOverwrite());
    } catch (InvalidLoadOptionException e) {
      LOGGER.error(e, "Invalid load options");
      throw new StoreException(e.getMessage());
    } catch (IOException e) {
      LOGGER.error(e, "Failed to load data");
      throw new StoreException(e.getMessage());
    }
  }

  public CarbonRow[] select(Select select) throws StoreException {
    try {
      return master.search(
          master.getTable(select.getDatabaseName(), select.getTableName()),
          select.getProjection(),
          ServiceUtil.parseFilter(select.getFilter()),
          select.getLimit(),
          select.getLimit());
    } catch (IOException e) {
      LOGGER.error(e, "[" + select.getId() + "] select failed");
      throw new StoreException(e.getMessage());
    }
  }

  public static synchronized HorizonService getInstance() {
    if (instance == null) {
      instance = new HorizonService();
    }
    return instance;
  }
}
