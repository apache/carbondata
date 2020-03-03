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

package org.apache.carbondata.hive.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;

public class DataTypeUtil {

  public static DataType convertHiveTypeToCarbon(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type) || type.startsWith("char")) {
      return DataTypes.STRING;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return DataTypes.VARCHAR;
    } else if ("float".equalsIgnoreCase(type)) {
      return DataTypes.FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return DataTypes.DOUBLE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return DataTypes.BOOLEAN;
    } else if ("tinyint".equalsIgnoreCase(type) || "smallint".equalsIgnoreCase(type)) {
      return DataTypes.SHORT;
    } else if ("int".equalsIgnoreCase(type)) {
      return DataTypes.INT;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return DataTypes.LONG;
    } else if ("date".equalsIgnoreCase(type)) {
      return DataTypes.DATE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return DataTypes.TIMESTAMP;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return DataTypes.createDefaultDecimalType();
    } else if ("binary".equalsIgnoreCase(type)) {
      return DataTypes.BINARY;
    } else if ("map".equalsIgnoreCase(type)) {
      return DataTypes.createDefaultMapType();
    } else if (type.startsWith("decimal")) {
      String[] precisionScale =
          type.substring(type.indexOf("(") + 1, type.lastIndexOf(")")).split(",");
      return DataTypes.createDecimalType(Integer.parseInt(precisionScale[0]),
          Integer.parseInt(precisionScale[1]));
    } else if (type.startsWith("array<")) {
      String subType = type.substring(type.indexOf("<") + 1, type.indexOf(">"));
      return DataTypes.createArrayType(convertHiveTypeToCarbon(subType));
    } else if (type.startsWith("map<")) {
      String[] subType = (type.substring(type.indexOf("<") + 1, type.indexOf(">"))).split(",");
      return DataTypes
          .createMapType(convertHiveTypeToCarbon(subType[0]), convertHiveTypeToCarbon(subType[1]));
    } else if (type.startsWith("struct<")) {
      String[] subTypes =
          (type.substring(type.indexOf("<") + 1, type.indexOf(">"))).split(",");
      List<StructField> structFieldList = new ArrayList<>();
      for (String subType : subTypes) {
        String[] nameAndType = subType.split(":");
        structFieldList
            .add(new StructField(nameAndType[0], convertHiveTypeToCarbon(nameAndType[1])));
      }
      return DataTypes.createStructType(structFieldList);
    } else {
      throw new SQLException("Unrecognized column type: " + type);
    }

  }
}
