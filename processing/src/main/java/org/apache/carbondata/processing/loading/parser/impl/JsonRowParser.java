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

package org.apache.carbondata.processing.loading.parser.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.parser.RowParser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonRowParser implements RowParser {

  private DataField[] dataFields;

  public JsonRowParser(DataField[] dataFields) {
    this.dataFields = dataFields;
  }

  @Override
  public Object[] parseRow(Object[] row) {
    try {
      return convertJsonToNoDictionaryToBytes((String) row[0]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object[] convertJsonToNoDictionaryToBytes(String jsonString)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      Map<String, Object> jsonNodeMap =
          objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {
          });
      if (jsonNodeMap == null) {
        return null;
      }
      Map<String, Object> jsonNodeMapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      jsonNodeMapCaseInsensitive.putAll(jsonNodeMap);
      return jsonToCarbonRecord(jsonNodeMapCaseInsensitive, dataFields);
    } catch (IOException e) {
      throw new IOException("Failed to parse Json String: " + e.getMessage(), e);
    }
  }

  private Object[] jsonToCarbonRecord(Map<String, Object> jsonNodeMap, DataField[] dataFields) {
    List<Object> fields = new ArrayList<>();
    for (DataField dataField : dataFields) {
      Object field = jsonToCarbonObject(jsonNodeMap, dataField.getColumn());
      fields.add(field);
    }
    // use this array object to form carbonRow
    return fields.toArray();
  }

  private Object jsonToCarbonObject(Map<String, Object> jsonNodeMap, CarbonColumn column) {
    DataType type = column.getDataType();
    if (DataTypes.isArrayType(type)) {
      CarbonDimension carbonDimension = (CarbonDimension) column;
      ArrayList array = (ArrayList) jsonNodeMap.get(extractChildColumnName(column));
      if ((array == null) || (array.size() == 0)) {
        return null;
      }
      // stored as array in carbonObject
      Object[] arrayChildObjects = new Object[array.size()];
      for (int i = 0; i < array.size(); i++) {
        // array column will have only one child, hence get(0).
        // But data can have n elements, hence the loop.
        CarbonDimension childCol = carbonDimension.getListOfChildDimensions().get(0);
        arrayChildObjects[i] = jsonChildElementToCarbonChildElement(array.get(i), childCol);
      }
      return new ArrayObject(arrayChildObjects);
    } else if (DataTypes.isStructType(type)) {
      CarbonDimension carbonDimension = (CarbonDimension) column;
      int size = carbonDimension.getNumberOfChild();
      Map<String, Object> jsonMap =
          (Map<String, Object>) jsonNodeMap.get(extractChildColumnName(column));
      if (jsonMap == null) {
        return null;
      }
      Object[] structChildObjects = new Object[size];
      for (int i = 0; i < size; i++) {
        CarbonDimension childCol = carbonDimension.getListOfChildDimensions().get(i);
        Object childObject =
            jsonChildElementToCarbonChildElement(jsonMap.get(extractChildColumnName(childCol)),
                childCol);
        structChildObjects[i] = childObject;
      }
      return new StructObject(structChildObjects);
    } else {
      // primitive type
      if (jsonNodeMap.get(extractChildColumnName(column)) == null) {
        return null;
      }
      return jsonNodeMap.get(extractChildColumnName(column)).toString();
    }
  }

  private Object jsonChildElementToCarbonChildElement(Object childObject,
      CarbonDimension column) {
    if (childObject == null) {
      return null;
    }
    DataType type = column.getDataType();
    if (DataTypes.isArrayType(type)) {
      ArrayList array = (ArrayList) childObject;
      if (array.size() == 0) {
        // handling empty array
        return null;
      }
      // stored as array in carbonObject
      Object[] arrayChildObjects = new Object[array.size()];
      for (int i = 0; i < array.size(); i++) {
        // array column will have only one child, hence get(0).
        // But data can have n elements, hence the loop.
        CarbonDimension childCol = column.getListOfChildDimensions().get(0);
        arrayChildObjects[i] = jsonChildElementToCarbonChildElement(array.get(i), childCol);
      }
      return new ArrayObject(arrayChildObjects);
    } else if (DataTypes.isStructType(type)) {
      Map<String, Object> childFieldsMap = (Map<String, Object>) childObject;
      int size = column.getNumberOfChild();
      Object[] structChildObjects = new Object[size];
      for (int i = 0; i < size; i++) {
        CarbonDimension childCol = column.getListOfChildDimensions().get(i);
        Object child = jsonChildElementToCarbonChildElement(
            childFieldsMap.get(extractChildColumnName(childCol)), childCol);
        structChildObjects[i] = child;
      }
      return new StructObject(structChildObjects);
    } else {
      // primitive type
      return childObject.toString();
    }
  }

  private static String extractChildColumnName(CarbonColumn column) {
    String columnName = column.getColName();
    if (column.getColumnSchema().isComplexColumn()) {
      // complex type child column names can be like following
      // a) struct type --> parent.child
      // b) array type --> parent.val.val...child [If create table flow]
      // c) array type --> parent.val0.val1...child [If SDK flow]
      // But json data's key is only child column name. So, extracting below
      String[] splits = columnName.split("\\.");
      columnName = splits[splits.length - 1];
    }
    return columnName;
  }
}
