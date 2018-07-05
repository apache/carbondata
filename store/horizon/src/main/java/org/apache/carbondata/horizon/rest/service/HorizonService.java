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

package org.apache.carbondata.horizon.rest.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.horizon.antlr.ANTLRNoCaseStringStream;
import org.apache.carbondata.horizon.antlr.FilterVisitor;
import org.apache.carbondata.horizon.antlr.gen.SelectLexer;
import org.apache.carbondata.horizon.antlr.gen.SelectParser;
import org.apache.carbondata.horizon.rest.model.descriptor.LoadDescriptor;
import org.apache.carbondata.horizon.rest.model.descriptor.SelectDescriptor;
import org.apache.carbondata.horizon.rest.model.descriptor.TableDescriptor;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.store.exception.StoreException;
import org.apache.carbondata.store.master.Master;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class HorizonService {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(HorizonService.class.getName());

  private static HorizonService instance;

  private Master master;

  private HorizonService() {
    master = Master.getInstance(null);
  }

  public boolean createTable(TableDescriptor tableDescriptor) throws StoreException {
    TableSchemaBuilder builder = TableSchema.builder();
    builder.tableName(tableDescriptor.getName()).properties(tableDescriptor.getProperties());

    Field[] fields = tableDescriptor.getSchema().getFields();
    // sort_columns
    List<String> sortColumnsList = null;
    try {
      sortColumnsList =
          tableDescriptor.getSchema().prepareSortColumns(tableDescriptor.getProperties());
    } catch (MalformedCarbonCommandException e) {
      throw new StoreException(e.getMessage());
    }
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];
    // tableDescriptor schema
    CarbonWriterBuilder.buildTableSchema(fields, builder, sortColumnsList, sortColumnsSchemaList);
    builder.setSortColumns(Arrays.asList(sortColumnsSchemaList));

    TableSchema schema = builder.build();
    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    schema.getSchemaEvolution().getSchemaEvolutionEntryList().add(schemaEvolutionEntry);
    schema.setTableName(tableDescriptor.getName());

    TableInfo tableInfo = CarbonTable
        .builder()
        .databaseName(tableDescriptor.getDatabase())
        .tableName(tableDescriptor.getName())
        .tablePath(
            master.getTableFolder(tableDescriptor.getDatabase(), tableDescriptor.getName()))
        .tableSchema(schema)
        .isTransactionalTable(true)
        .buildTableInfo();

    try {
      return master.createTable(tableInfo, tableDescriptor.isIfNotExists());
    } catch (IOException e) {
      LOGGER.error(e, "create tableDescriptor failed");
      throw new StoreException(e.getMessage());
    }
  }

  public boolean loadData(LoadDescriptor loadDescriptor) throws StoreException {
    CarbonTable table = master.getTable(
        loadDescriptor.getDatabaseName(), loadDescriptor.getTableName());
    CarbonLoadModelBuilder modelBuilder = new CarbonLoadModelBuilder(table);
    modelBuilder.setInputPath(loadDescriptor.getInputPath());
    try {
      CarbonLoadModel model =
          modelBuilder.build(loadDescriptor.getOptions(), System.currentTimeMillis(), "0");

      return master.loadData(model, loadDescriptor.isOverwrite());
    } catch (InvalidLoadOptionException e) {
      LOGGER.error(e, "Invalid loadDescriptor options");
      throw new StoreException(e.getMessage());
    } catch (IOException e) {
      LOGGER.error(e, "Failed to loadDescriptor data");
      throw new StoreException(e.getMessage());
    }
  }

  public CarbonRow[] select(SelectDescriptor selectDescriptor) throws StoreException {
    try {
      CarbonTable carbonTable = master.getTable(
          selectDescriptor.getDatabaseName(), selectDescriptor.getTableName());
      return master.search(
          carbonTable,
          selectDescriptor.getProjection(),
          parseFilter(selectDescriptor.getFilter(), carbonTable),
          selectDescriptor.getLimit(),
          selectDescriptor.getLimit());
    } catch (IOException e) {
      LOGGER.error(e, "[" + selectDescriptor.getId() + "] select failed");
      throw new StoreException(e.getMessage());
    }
  }

  public static Expression parseFilter(String filter, CarbonTable carbonTable) {
    if (filter == null) {
      return null;
    }
    CharStream input = new ANTLRNoCaseStringStream(filter);
    SelectLexer lexer = new SelectLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SelectParser parser = new SelectParser(tokens);
    SelectParser.ParseFilterContext tree = parser.parseFilter();
    FilterVisitor visitor = new FilterVisitor(carbonTable);
    return visitor.visitParseFilter(tree);
  }

  public static synchronized HorizonService getInstance() {
    if (instance == null) {
      instance = new HorizonService();
    }
    return instance;
  }
}
