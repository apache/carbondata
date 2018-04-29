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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.SchemaEvolution;
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Builder for {@link TableSchema}
 */
public class TableSchemaBuilder {

  private int ordinal = 0;

  private List<ColumnSchema> sortColumns = new LinkedList<>();

  private List<ColumnSchema> otherColumns = new LinkedList<>();

  private int blockSize;

  private int blockletSize;

  private String tableName;

  public TableSchemaBuilder blockSize(int blockSize) {
    if (blockSize <= 0) {
      throw new IllegalArgumentException("blockSize should be greater than 0");
    }
    this.blockSize = blockSize;
    return this;
  }

  public TableSchemaBuilder blockletSize(int blockletSize) {
    if (blockletSize <= 0) {
      throw new IllegalArgumentException("blockletSize should be greater than 0");
    }
    this.blockletSize = blockletSize;
    return this;
  }

  public TableSchemaBuilder tableName(String tableName) {
    Objects.requireNonNull(tableName);
    this.tableName = tableName;
    return this;
  }

  public TableSchema build() {
    TableSchema schema = new TableSchema();
    schema.setTableName(tableName);
    schema.setTableId(UUID.randomUUID().toString());
    schema.setPartitionInfo(null);
    schema.setBucketingInfo(null);
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    schema.setSchemaEvalution(schemaEvol);
    List<ColumnSchema> allColumns = new LinkedList<>(sortColumns);
    allColumns.addAll(otherColumns);
    schema.setListOfColumns(allColumns);

    Map<String, String> property = new HashMap<>();
    if (blockSize > 0) {
      property.put(CarbonCommonConstants.TABLE_BLOCKSIZE, String.valueOf(blockSize));
    }
    if (blockletSize > 0) {
      property.put(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB, String.valueOf(blockletSize));
    }
    if (property.size() != 0) {
      schema.setTableProperties(property);
    }

    return schema;
  }

  public void setSortColumns(List<ColumnSchema> sortColumns) {
    this.sortColumns = sortColumns;
  }

  public ColumnSchema addColumn(StructField field, boolean isSortColumn) {
    Objects.requireNonNull(field);
    checkRepeatColumnName(field);
    ColumnSchema newColumn = new ColumnSchema();
    newColumn.setColumnName(field.getFieldName());
    newColumn.setDataType(field.getDataType());
    if (isSortColumn ||
        field.getDataType() == DataTypes.STRING ||
        field.getDataType() == DataTypes.DATE ||
        field.getDataType() == DataTypes.TIMESTAMP ||
        DataTypes.isStructType(field.getDataType())) {
      newColumn.setDimensionColumn(true);
    } else {
      newColumn.setDimensionColumn(false);
    }
    newColumn.setSchemaOrdinal(ordinal++);
    newColumn.setColumnar(true);

    // For NonTransactionalTable, multiple sdk writer output with same column name can be placed in
    // single folder for query.
    // That time many places in code, columnId check will fail. To avoid that
    // keep column ID as same as column name.
    // Anyhow Alter table is not supported for NonTransactionalTable.
    // SO, this will not have any impact.
    newColumn.setColumnUniqueId(field.getFieldName());
    newColumn.setColumnReferenceId(newColumn.getColumnUniqueId());
    newColumn.setEncodingList(createEncoding(field.getDataType(), isSortColumn));
    if (field.getDataType().isComplexType()) {
      newColumn.setNumberOfChild(((StructType) field.getDataType()).getFields().size());
    }
    if (DataTypes.isDecimal(field.getDataType())) {
      DecimalType decimalType = (DecimalType) field.getDataType();
      newColumn.setPrecision(decimalType.getPrecision());
      newColumn.setScale(decimalType.getScale());
    }
    if (!isSortColumn) {
      otherColumns.add(newColumn);
    }
    if (newColumn.isDimensionColumn()) {
      newColumn.setUseInvertedIndex(true);
    }
    if (field.getDataType().isComplexType()) {
      if (((StructType) field.getDataType()).getFields().size() > 0) {
        // This field has children.
        List<StructField> fields = ((StructType) field.getDataType()).getFields();
        for (int i = 0; i < fields.size(); i ++) {
          addColumn(fields.get(i), false);
        }
      }
    }
    return newColumn;
  }

  /**
   * Throw exception if {@param field} name is repeated
   */
  private void checkRepeatColumnName(StructField field) {
    for (ColumnSchema column : sortColumns) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }
    for (ColumnSchema column : otherColumns) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }
  }

  private List<Encoding> createEncoding(DataType dataType, boolean isSortColumn) {
    List<Encoding> encodings = new LinkedList<>();
    if (dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
      encodings.add(Encoding.DICTIONARY);
    }
    if (isSortColumn) {
      encodings.add(Encoding.INVERTED_INDEX);
    }
    return encodings;
  }

}
