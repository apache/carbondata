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

package org.apache.carbondata.horizon;

import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.horizon.rest.model.view.CreateTableRequest;
import org.apache.carbondata.horizon.antlr.Parser;
import org.apache.carbondata.sdk.store.descriptor.TableDescriptor;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilterParseTest {

  private static CarbonTable carbonTable;

  @BeforeClass
  public static void setup() throws MalformedCarbonCommandException {

    CreateTableRequest createTableRequest = CreateTableRequest
        .builder()
        .ifNotExists()
        .databaseName("default")
        .tableName("table_1")
        .comment("first tableDescriptor")
        .column("shortField", "SHORT", "short field")
        .column("intField", "INT", "int field")
        .column("bigintField", "LONG", "long field")
        .column("doubleField", "DOUBLE", "double field")
        .column("stringField", "STRING", "string field")
        .column("timestampField", "TIMESTAMP", "timestamp field")
        .column("decimalField", "DECIMAL", 18, 2, "decimal field")
        .column("dateField", "DATE", "date field")
        .column("charField", "CHAR", "char field")
        .column("floatField", "FLOAT", "float field")
        .tblProperties(CarbonCommonConstants.SORT_COLUMNS, "intField")
        .create();

    TableDescriptor tableDescriptor = createTableRequest.convertToDto();

    TableSchemaBuilder builder = TableSchema
        .builder()
        .tableName(tableDescriptor.getTable().getTableName())
        .properties(tableDescriptor.getProperties());

    Field[] fields = tableDescriptor.getSchema().getFields();
    // sort_columns
    List<String> sortColumnsList =
        tableDescriptor.getSchema().prepareSortColumns(tableDescriptor.getProperties());
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];
    // tableDescriptor schema
    CarbonWriterBuilder.buildTableSchema(fields, builder, sortColumnsList, sortColumnsSchemaList);
    builder.setSortColumns(Arrays.asList(sortColumnsSchemaList));

    TableSchema schema = builder.build();
    SchemaEvolutionEntry schemaEvolutionEntry = new SchemaEvolutionEntry();
    schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis());
    schema.getSchemaEvolution().getSchemaEvolutionEntryList().add(schemaEvolutionEntry);
    schema.setTableName(tableDescriptor.getTable().getTableName());

    carbonTable = CarbonTable
        .builder()
        .databaseName(tableDescriptor.getTable().getDatabaseName())
        .tableName(tableDescriptor.getTable().getTableName())
        .tablePath("")
        .tableSchema(schema)
        .isTransactionalTable(true)
        .build();
  }


  private void checkExpression(String sql1, String sql2) {
    Expression expression = Parser.parseFilter(sql1, carbonTable);
    Assert.assertEquals(sql2, expression.getStatement());
  }

  private void checkExpression(String sql) {
    checkExpression(sql, sql);
  }

  @Test
  public void testFilterParse() {

    // >, >=, <, <=, =, <>, !=
    checkExpression("intField > 10");
    checkExpression("intField >= 10");
    checkExpression("intField < 10");
    checkExpression("intField <= 10");
    checkExpression("intField = 10");
    checkExpression("stringField = 'carbon'");
    checkExpression("intField <> 10");
    checkExpression("stringField <> 'carbon'");
    checkExpression("intField != 10", "intField <> 10");
    checkExpression("stringField != 'carbon'", "stringField <> 'carbon'");

    // is null, is not null
    checkExpression("stringField is null");
    checkExpression("stringField is not null");

    // in, not in
    checkExpression("intField in (10, 20, 30)" );
    checkExpression("stringField in ('spark', 'carbon')" );
    checkExpression("intField not in (10, 20, 30)" );
    checkExpression("stringField not in ('spark', 'carbon')" );

    // between and, not between and
    checkExpression("intField between 10 and 30", "intField >= 10 and intField <= 30" );
    checkExpression("intField not between 10 and 30", "intField > 30 and intField < 10" );

    // and, or
    checkExpression(
        "intField > 10 and stringField = 'carbon'",
        "(intField > 10 and stringField = 'carbon')");
    checkExpression(
        "(intField > 10) and stringField = 'carbon'",
        "(intField > 10 and stringField = 'carbon')");
    checkExpression(
        "(intField > 10) or stringField = 'carbon'",
        "(intField > 10 or stringField = 'carbon')");
    checkExpression(
        "intField > -10 or (stringField = 'carbon' and floatField > 5.0)",
        "(intField > -10 or (stringField = 'carbon' and floatField > 5.0))");

    // data type: short, int, bigint, double, decimal, bigDecimal string, timestamp, date
    checkExpression("shortField = 1+S", "shortField = 1");
    checkExpression("intField = 1");
    checkExpression("bigintField = 1+L", "bigintField = 1");
    checkExpression("doubleField = 1.01+D", "doubleField = 1.01");
    checkExpression("decimalField = 1000.01001", "decimalField = 1000.01001");
    checkExpression("decimalField = 1000.01001+BD", "decimalField = 1000.01001");
    checkExpression("stringField = 'carbon'");
    checkExpression("timestampField = '2018-01-01 10:01:01'");
    checkExpression("dateField = '2018-01-01'");
  }
}
