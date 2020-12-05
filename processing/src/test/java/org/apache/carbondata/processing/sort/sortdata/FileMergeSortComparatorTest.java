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
package org.apache.carbondata.processing.sort.sortdata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.junit.Test;

public class FileMergeSortComparatorTest {

  static ColumnSchema getDimensionColumn(String colName, Boolean isSortColumn, Boolean isDictColumn,
      DataType dataType) {
    ColumnSchema dimColumn = new ColumnSchema();
    dimColumn.setColumnName(colName);
    dimColumn.setDataType(dataType);
    dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
    dimColumn.setDimensionColumn(true);
    List<Encoding> encodeList =
        new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    if (isDictColumn) {
      encodeList.add(Encoding.DICTIONARY);
    }
    dimColumn.setEncodingList(encodeList);
    if (isSortColumn) {
      dimColumn.setSortColumn(true);
    }
    return dimColumn;
  }

  static TableSchema getTableSchema() {
    TableSchema tableSchema = new TableSchema();
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      columnSchemaList.add(getDimensionColumn("col1", false, false, DataTypes.INT));
    columnSchemaList.add(getDimensionColumn("col2", true, false, DataTypes.STRING));
    columnSchemaList.add(getDimensionColumn("col3", true, false, DataTypes.LONG));
    columnSchemaList.add(getDimensionColumn("col4", true, true, DataTypes.STRING));
    tableSchema.setListOfColumns(columnSchemaList);
    tableSchema.setTableId(UUID.randomUUID().toString());
    tableSchema.setTableName("carbonTestTable");
    return tableSchema;
  }

  static private TableInfo getTableInfo() {
    TableInfo info = new TableInfo();
    info.setDatabaseName("carbonTestDatabase");
    info.setTableUniqueName("carbonTestDatabase_carbonTestTable");
    info.setFactTable(getTableSchema());
    info.setTablePath("testore");
    return info;
  }

  @Test
  public void testFileMergeSortComparator() {
    CarbonTable carbonTable = CarbonTable.buildFromTableInfo(getTableInfo());
    // test get noDictDataTypes
    DataType[] noDictDataTypes = CarbonDataProcessorUtil.getNoDictDataTypes(carbonTable);
    assert (noDictDataTypes.length == 3 && noDictDataTypes[0].equals(DataTypes.INT)
        && noDictDataTypes[1].equals(DataTypes.STRING) && noDictDataTypes[2]
        .equals(DataTypes.LONG));

    // test comparator
    Map<String, int[]> columnIdxMap =
        CarbonDataProcessorUtil.getColumnIdxBasedOnSchemaInRow(carbonTable);
    int[] columnIdxBasedOnSchemaInRows = columnIdxMap.get("columnIdxBasedOnSchemaInRow");
    int[] noDictSortIdxBasedOnSchemaInRows = columnIdxMap.get("noDictSortIdxBasedOnSchemaInRow");
    int[] dictSortIdxBasedOnSchemaInRows = columnIdxMap.get("dictSortIdxBasedOnSchemaInRow");
 
    assert (noDictSortIdxBasedOnSchemaInRows.length == 2 && noDictSortIdxBasedOnSchemaInRows[0] == 1
        && noDictSortIdxBasedOnSchemaInRows[1] == 2);
    assert (dictSortIdxBasedOnSchemaInRows.length == 1 && dictSortIdxBasedOnSchemaInRows[0] == 0);

    FileMergeSortComparator comparator =
        new FileMergeSortComparator(noDictDataTypes, columnIdxBasedOnSchemaInRows,
            noDictSortIdxBasedOnSchemaInRows, dictSortIdxBasedOnSchemaInRows);

    // prepare data for final sort
    int[] dictSortDims1 = { 1 };
    Object[] noDictSortDims1 = { 1, new byte[] { 98, 99, 104 }, 2 };
    byte[] noSortDimsAndMeasures1 = {};
    IntermediateSortTempRow row1 =
        new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = { 1 };
    Object[] noDictSortDims = { 2, new byte[] { 98, 99, 100 }, 1 };
    byte[] noSortDimsAndMeasures = {};
    IntermediateSortTempRow row2 =
        new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    assert (comparator.compare(row1, row2) > 0);
  }

}
