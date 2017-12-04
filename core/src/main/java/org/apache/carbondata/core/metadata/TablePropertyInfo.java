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

package org.apache.carbondata.core.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.table.MalformedCarbonCommandException;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * This class encapsulate the definition of sortColumns, dictionaryColumns, noInvertedIndexColumns
 * in the table.
 * These columns are extracted from schema and table property map when creating instance
 * of this class.
 * It is used when creating table and alter table add column.
 */
public class TablePropertyInfo {

  private Map<String, String> tableProperties;
  private List<String> sortColumns;
  private List<String> noInvertedIndexColumns;
  private List<String> dictionaryColumns;

  /**
   * This is called in case of create table
   */
  public TablePropertyInfo(StructType schema, Map<String, String> tableProperties)
      throws MalformedCarbonCommandException {
    if (tableProperties == null) {
      throw new IllegalArgumentException("table property should not be null");
    }
    this.tableProperties = tableProperties;
    this.noInvertedIndexColumns = extractNoInvertedIndexColumns();
    this.dictionaryColumns = extractDictionaryColumns();
    this.sortColumns = extractSortColumns(schema);
  }

  /**
   * This is called in case of alter table add column
   */
  public TablePropertyInfo(
      List<StructField> newFields,
      List<String> existingSortColumns,
      Map<String, String> existingTableProperties,
      Map<String, String> alterTableProperties) {
    if (existingSortColumns == null) {
      throw new IllegalArgumentException("sort columns should not be null");
    }
    if (existingTableProperties == null) {
      throw new IllegalArgumentException("table property should not be null");
    }

    // User may add new table property in alter table command
    // these new properties need to be added in existing table property
    for (Map.Entry<String, String> entry : alterTableProperties.entrySet()) {
      String newValue = entry.getValue();
      String existingValue = existingTableProperties.get(entry.getKey());
      if (existingValue != null) {
        existingTableProperties.put(entry.getKey(), existingValue + "," + newValue);
      } else {
        existingTableProperties.put(entry.getKey(), newValue);
      }
    }
    this.tableProperties = existingTableProperties;
    this.noInvertedIndexColumns = extractNoInvertedIndexColumns();
    this.dictionaryColumns = extractDictionaryColumns();
    this.sortColumns = updateSortColumnList(newFields, existingSortColumns, alterTableProperties);
    updateSortColumnProperty(tableProperties);
  }

  // Add column to sortColumns if it is dimension column, return the updated sort columns
  // TODO: this is the old behavior, it is strange, keep it now for backward compatibility
  private List<String> updateSortColumnList(
      List<StructField> newFields,
      List<String> sortColumns,
      Map<String, String> alterTableProperties) {
    List<String> newSortColumns = new ArrayList<>();
    newSortColumns.addAll(sortColumns);

    // add it to sort columns if it is either in dictionary include or it is dimension type
    String dictColumnString = alterTableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE);
    if (dictColumnString != null) {
      String[] dictColumns = dictColumnString.split(",");
      for (String dictColumn : dictColumns) {
        newSortColumns.add(dictColumn.trim());
      }
    }
    // add field to sort column if it is dimension type
    for (StructField newField : newFields) {
      if (newField.getDataType() == DataTypes.STRING || newField.getDataType() == DataTypes.DATE ||
          newField.getDataType() == DataTypes.TIMESTAMP || newField.getDataType().isComplexType()) {
        newSortColumns.add(newField.getFieldName());
      }
    }
    return newSortColumns;
  }

  // update the sort_columns value in `tableProperties` according to `sortColumns`
  // this is required in case of alter table add column
  private void updateSortColumnProperty(Map<String, String> tableProperties) {
    if (!tableProperties.containsKey(CarbonCommonConstants.SORT_COLUMNS)) {
      StringBuilder sortColumnsString = new StringBuilder();
      for (String column : sortColumns) {
        sortColumnsString.append(column).append(",");
      }
      if (sortColumnsString.length() > 0) {
        tableProperties.put(
            CarbonCommonConstants.SORT_COLUMNS,
            sortColumnsString.substring(0, sortColumnsString.length() - 1));
      }
    }
    this.tableProperties = tableProperties;
  }

  public List<String> getSortColumns() {
    return sortColumns;
  }

  public List<String> getNoInvertedIndexColumns() {
    return noInvertedIndexColumns;
  }

  public List<String> getDictionaryColumns() {
    return dictionaryColumns;
  }

  private List<String> extractSortColumns(StructType schema) {
    String sortColumnsString = tableProperties.get(CarbonCommonConstants.SORT_COLUMNS);
    List<String> sortColumns = new ArrayList<>();
    if (sortColumnsString != null) {
      String[] columns =
          CarbonUtil.unquoteChar(sortColumnsString.toLowerCase()).trim().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      sortColumns = Arrays.asList(columns);
    }
    if (sortColumns.isEmpty()) {
      // default sort columns is all dimension except complex type
      List<StructField> fields = schema.getFields();
      for (StructField field : fields) {
        DataType dataType = field.getDataType();
        if (dictionaryColumns.contains(field.getFieldName().toLowerCase()) &&
            !field.getDataType().isComplexType()) {
          sortColumns.add(field.getFieldName());
        }
        if (dataType == DataTypes.STRING || dataType == DataTypes.TIMESTAMP ||
            dataType == DataTypes.DATE) {
          sortColumns.add(field.getFieldName());
        }
      }
    }
    return sortColumns;
  }

  private List<String> extractNoInvertedIndexColumns() {
    // Column names that does not do inverted index.
    // Note that inverted index is allowed only in sort columns
    String noInvertedIndexString = tableProperties.get(CarbonCommonConstants.NO_INVERTED_INDEX);
    if (noInvertedIndexString == null) {
      return new ArrayList<>(0);
    } else {
      String[] columns = noInvertedIndexString.toLowerCase().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      return Arrays.asList(columns);
    }
  }

  private List<String> extractDictionaryColumns() {
    String dictionaryColumns = tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE);
    if (dictionaryColumns == null) {
      return new ArrayList<>(0);
    } else {
      String[] columns = dictionaryColumns.toLowerCase().split(",");
      for (int i = 0; i < columns.length; i++) {
        columns[i] = columns[i].trim();
      }
      return Arrays.asList(columns);
    }
  }

  public Map<String, String> getPropertyMap() {
    return tableProperties;
  }
}
