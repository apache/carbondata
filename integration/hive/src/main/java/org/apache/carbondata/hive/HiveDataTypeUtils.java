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

package org.apache.carbondata.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;

import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class HiveDataTypeUtils {

  public static TypeInfo convertCarbonDataTypeToHive(CarbonColumn column) {
    int id = column.getDataType().getId();
    if (id == DataTypes.STRING.getId()) {
      return TypeInfoFactory.stringTypeInfo;
    } else if (id == DataTypes.DATE.getId()) {
      return TypeInfoFactory.dateTypeInfo;
    } else if (id == DataTypes.TIMESTAMP.getId()) {
      return TypeInfoFactory.timestampTypeInfo;
    } else if (id == DataTypes.BOOLEAN.getId()) {
      return TypeInfoFactory.booleanTypeInfo;
    } else if (id == DataTypes.BYTE.getId()) {
      return TypeInfoFactory.byteTypeInfo;
    } else if (id == DataTypes.SHORT.getId()) {
      return TypeInfoFactory.shortTypeInfo;
    } else if (id == DataTypes.INT.getId()) {
      return TypeInfoFactory.intTypeInfo;
    } else if (id == DataTypes.LONG.getId()) {
      return TypeInfoFactory.longTypeInfo;
    } else if (id == DataTypes.FLOAT.getId()) {
      return TypeInfoFactory.floatTypeInfo;
    } else if (id == DataTypes.DOUBLE.getId()) {
      return TypeInfoFactory.doubleTypeInfo;
    } else if (id == DataTypes.DECIMAL_TYPE_ID) {
      DecimalType type = (DecimalType) column.getDataType();
      return new DecimalTypeInfo(type.getPrecision(), type.getScale());
    } else if (id == DataTypes.BINARY.getId()) {
      return TypeInfoFactory.binaryTypeInfo;
    } else if (id == DataTypes.ARRAY_TYPE_ID) {
      ListTypeInfo typeInfo = new ListTypeInfo();
      if (!(column instanceof CarbonDimension)) {
        throw new RuntimeException(
            "Failed to get child columns of column: " + column.getColName());
      }
      typeInfo.setListElementTypeInfo(
          convertCarbonDataTypeToHive(
              ((CarbonDimension) column).getListOfChildDimensions().get(0)
          )
      );
      return typeInfo;
    } else if (id == DataTypes.STRUCT_TYPE_ID) {
      StructTypeInfo typeInfo = new StructTypeInfo();
      if (!(column instanceof CarbonDimension)) {
        throw new RuntimeException(
            "Failed to get child columns of column: " + column.getColName());
      }
      List<CarbonDimension> listOfChildDimensions =
          ((CarbonDimension) column).getListOfChildDimensions();
      ArrayList<String> allStructFieldNames = new ArrayList<>(listOfChildDimensions.size());
      ArrayList<TypeInfo> allStructFieldTypeInfos = new ArrayList<>(listOfChildDimensions.size());
      typeInfo.setAllStructFieldNames(allStructFieldNames);
      typeInfo.setAllStructFieldTypeInfos(allStructFieldTypeInfos);
      for (CarbonDimension dimension : listOfChildDimensions) {
        String[] columnsNames = dimension.getColName().split("\\.");
        allStructFieldNames.add(columnsNames[columnsNames.length - 1]);
        allStructFieldTypeInfos.add(convertCarbonDataTypeToHive(dimension));
      }
      return typeInfo;
    } else if (id == DataTypes.MAP_TYPE_ID) {
      MapTypeInfo typeInfo = new MapTypeInfo();
      List<CarbonDimension> listOfChildDimensions = ((CarbonDimension) column)
          .getListOfChildDimensions()
          .get(0)
          .getListOfChildDimensions();
      typeInfo.setMapKeyTypeInfo(convertCarbonDataTypeToHive(listOfChildDimensions.get(0)));
      typeInfo.setMapValueTypeInfo(convertCarbonDataTypeToHive(listOfChildDimensions.get(1)));
      return typeInfo;
    } else if (id == DataTypes.VARCHAR.getId()) {
      return TypeInfoFactory.varcharTypeInfo;
    } else {
      throw new RuntimeException("convert DataType with invalid id: " + id);
    }
  }
}
