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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.MapType;
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

  private List<ColumnSchema> dimension = new LinkedList<>();

  private List<ColumnSchema> varCharColumns = new LinkedList<>();

  private List<ColumnSchema> complex = new LinkedList<>();

  private List<ColumnSchema> measures = new LinkedList<>();

  private int blockSize;

  private int blockletSize;

  private int pageSizeInMb;

  private String tableName;
  private boolean isLocalDictionaryEnabled;
  private String localDictionaryThreshold;

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

  public TableSchemaBuilder pageSizeInMb(int pageSizeInMb) {
    this.pageSizeInMb = pageSizeInMb;
    return this;
  }

  public TableSchemaBuilder localDictionaryThreshold(int localDictionaryThreshold) {
    this.localDictionaryThreshold = String.valueOf(localDictionaryThreshold);
    return this;
  }

  public TableSchemaBuilder enableLocalDictionary(boolean enableLocalDictionary) {
    this.isLocalDictionaryEnabled = enableLocalDictionary;
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
    schema.setSchemaEvolution(schemaEvol);
    List<ColumnSchema> allColumns = new LinkedList<>(sortColumns);
    allColumns.addAll(dimension);
    allColumns.addAll(varCharColumns);
    allColumns.addAll(complex);
    allColumns.addAll(measures);
    schema.setListOfColumns(allColumns);

    Map<String, String> property = new HashMap<>();
    if (blockSize > 0) {
      property.put(CarbonCommonConstants.TABLE_BLOCKSIZE, String.valueOf(blockSize));
    }
    if (blockletSize > 0) {
      property.put(CarbonCommonConstants.TABLE_BLOCKLET_SIZE, String.valueOf(blockletSize));
    }
    if (pageSizeInMb > 0) {
      property.put(CarbonCommonConstants.TABLE_PAGE_SIZE_INMB, String.valueOf(pageSizeInMb));
    }

    // Adding local dictionary, applicable only for String(dictionary exclude)
    if (isLocalDictionaryEnabled) {
      property.put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
          String.valueOf(isLocalDictionaryEnabled));
      String localdictionaryThreshold = localDictionaryThreshold.equalsIgnoreCase("0") ?
          CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT :
          localDictionaryThreshold;
      property.put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD, localdictionaryThreshold);
      for (int index = 0; index < allColumns.size(); index++) {
        ColumnSchema colSchema = allColumns.get(index);
        if (colSchema.getDataType() == DataTypes.STRING
            || colSchema.getDataType() == DataTypes.VARCHAR) {
          colSchema.setLocalDictColumn(true);
          allColumns.set(index, colSchema);
        }
      }
    }
    if (property.size() != 0) {
      schema.setTableProperties(property);
    }
    return schema;
  }

  public void setSortColumns(List<ColumnSchema> sortColumns) {
    this.sortColumns = sortColumns;
  }

  public ColumnSchema addColumn(StructField field, AtomicInteger valIndex, boolean isSortColumn,
      boolean isInvertedIdxColumn) {
    return addColumn(field, null, valIndex, isSortColumn, false, isInvertedIdxColumn);
  }

  private ColumnSchema addColumn(StructField field, String parentName, AtomicInteger valIndex,
      boolean isSortColumn, boolean isComplexChild, boolean isInvertedIdxColumn) {
    Objects.requireNonNull(field);
    if (isComplexChild) {
      // if field is complex then append parent name to the child field to check
      // if any other field with same name exists
      checkRepeatColumnName(field, parentName);
    } else {
      checkRepeatColumnName(field);
    }
    ColumnSchema newColumn = new ColumnSchema();
    if (parentName != null) {
      newColumn.setColumnName(parentName + "." + field.getFieldName());
    } else {
      newColumn.setColumnName(field.getFieldName());
    }
    newColumn.setDataType(field.getDataType());
    if (isSortColumn ||
        field.getDataType() == DataTypes.STRING ||
        field.getDataType() == DataTypes.VARCHAR ||
        field.getDataType() == DataTypes.DATE ||
        field.getDataType() == DataTypes.TIMESTAMP ||
        field.getDataType() == DataTypes.BINARY ||
        field.getDataType().isComplexType() ||
        (isComplexChild))  {
      newColumn.setDimensionColumn(true);
    } else {
      newColumn.setDimensionColumn(false);
    }
    if (!isComplexChild) {
      newColumn.setSchemaOrdinal(ordinal++);
    } else {
      // child column should not be counted for schema ordinal
      newColumn.setSchemaOrdinal(-1);
    }

    // For NonTransactionalTable, multiple sdk writer output with same column name can be placed in
    // single folder for query.
    // That time many places in code, columnId check will fail. To avoid that
    // keep column ID as same as column name.
    // Anyhow Alter table is not supported for NonTransactionalTable.
    // SO, this will not have any impact.
    newColumn.setColumnUniqueId(field.getFieldName());
    newColumn.setColumnReferenceId(newColumn.getColumnUniqueId());
    newColumn
        .setEncodingList(createEncoding(field.getDataType(), isInvertedIdxColumn, isComplexChild));
    if (field.getDataType().isComplexType()) {
      if (DataTypes.isArrayType(field.getDataType()) || DataTypes.isMapType(field.getDataType())) {
        newColumn.setNumberOfChild(1);
      } else {
        newColumn.setNumberOfChild(((StructType) field.getDataType()).getFields().size());
      }
    }
    if (DataTypes.isDecimal(field.getDataType())) {
      DecimalType decimalType = (DecimalType) field.getDataType();
      newColumn.setPrecision(decimalType.getPrecision());
      newColumn.setScale(decimalType.getScale());
    }
    if (!isSortColumn) {
      if (!newColumn.isDimensionColumn()) {
        measures.add(newColumn);
      } else if (DataTypes.isStructType(field.getDataType()) || DataTypes
          .isArrayType(field.getDataType()) || DataTypes.isMapType(field.getDataType())
          || isComplexChild) {
        complex.add(newColumn);
      } else {
        if (field.getDataType() == DataTypes.VARCHAR) {
          varCharColumns.add(newColumn);
        } else {
          dimension.add(newColumn);
        }
      }
    } else {
      newColumn.setSortColumn(true);
      sortColumns.add(newColumn);
    }
    if (field.getDataType().isComplexType()) {
      String parentFieldName = newColumn.getColumnName();
      if (DataTypes.isArrayType(field.getDataType())) {
        String colName = getColNameForArray(valIndex);
        addColumn(new StructField(colName, ((ArrayType) field.getDataType()).getElementType()),
            field.getFieldName(), valIndex, false, true, isInvertedIdxColumn);
      } else if (DataTypes.isStructType(field.getDataType())
          && ((StructType) field.getDataType()).getFields().size() > 0) {
        // This field has children.
        List<StructField> fields = ((StructType) field.getDataType()).getFields();
        for (int i = 0; i < fields.size(); i++) {
          addColumn(fields.get(i), parentFieldName, valIndex, false, true, isInvertedIdxColumn);
        }
      } else if (DataTypes.isMapType(field.getDataType())) {
        String colName = getColNameForArray(valIndex);
        addColumn(new StructField(colName, ((MapType) field.getDataType()).getValueType()),
            parentFieldName, valIndex, false, true, isInvertedIdxColumn);
      }
    }
    // todo: need more information such as long_string_columns
    return newColumn;
  }

  private String getColNameForArray(AtomicInteger valIndex) {
    String colName = "val" + valIndex.get();
    valIndex.incrementAndGet();
    return colName;
  }

  /**
   * Throw exception if {@param field} name is repeated
   */
  private void checkRepeatColumnName(StructField field, String parentName) {
    checkRepeatColumnName(
        new StructField(parentName + "." + field.getFieldName(), field.getDataType(),
            field.getChildren()));
  }

  private void checkRepeatColumnName(StructField field) {
    for (ColumnSchema column : sortColumns) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }
    for (ColumnSchema column : dimension) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }

    for (ColumnSchema column : complex) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }

    for (ColumnSchema column : measures) {
      if (column.getColumnName().equalsIgnoreCase(field.getFieldName())) {
        throw new IllegalArgumentException("column name already exists");
      }
    }
  }

  private List<Encoding> createEncoding(DataType dataType, boolean isInvertedIdxColumn,
      boolean isComplexChild) {
    List<Encoding> encodings = new LinkedList<>();
    if (dataType == DataTypes.DATE && !isComplexChild) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
      encodings.add(Encoding.DICTIONARY);
    }
    if (isInvertedIdxColumn) {
      encodings.add(Encoding.INVERTED_INDEX);
    }
    return encodings;
  }

}
