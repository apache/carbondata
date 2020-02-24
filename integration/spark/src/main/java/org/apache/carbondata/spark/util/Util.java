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

package org.apache.carbondata.spark.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CarbonMetastoreTypes;
import org.apache.spark.sql.util.SparkTypeConverter;
import org.apache.spark.util.Utils;

public class Util {
  /**
   * return the Array of available local-dirs
   */
  public static String[] getConfiguredLocalDirs(SparkConf conf) {
    return Utils.getConfiguredLocalDirs(conf);
  }

  /**
   * Method to check whether there exists any block which does not contain the blocklet info
   *
   * @param splitList
   * @return
   */
  public static boolean isBlockWithoutBlockletInfoExists(List<CarbonInputSplit> splitList) {
    for (CarbonInputSplit inputSplit : splitList) {
      if (inputSplit.isBlockCache()) {
        return true;
      }
    }
    return false;
  }

  public static org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType, boolean forGlobalSort) {
    if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.STRING) {
      return DataTypes.StringType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT) {
      return DataTypes.ShortType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
      return DataTypes.IntegerType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
      return DataTypes.LongType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
      return DataTypes.DoubleType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
      return DataTypes.BooleanType;
    } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(carbonDataType)) {
      return DataTypes.createDecimalType();
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
      if (forGlobalSort) {
        // data is already converted, so need to change data type also
        return DataTypes.LongType;
      }
      return DataTypes.TimestampType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      if (forGlobalSort) {
        // data is already converted, so need to change data type also
        return DataTypes.IntegerType;
      }
      return DataTypes.DateType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BINARY) {
      return DataTypes.BinaryType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.VARCHAR) {
      return DataTypes.StringType;
    } else {
      return null;
    }
  }

  public static StructType convertToSparkSchema(CarbonTable table) {
    List<CarbonColumn> columns = table.getCreateOrderColumn();
    ColumnSchema[] schema = new ColumnSchema[columns.size()];
    int i = 0;
    for (CarbonColumn column : columns) {
      schema[i++] = column.getColumnSchema();
    }
    return convertToSparkSchema(table, schema, false);
  }

  public static StructType convertToSparkSchemaFromColumnSchema(CarbonTable table,
      boolean forGlobalSort) {
    List<ColumnSchema> columns = table.getTableInfo().getFactTable().getListOfColumns();
    List<ColumnSchema> validColumnSchema = new ArrayList<>();
    for (ColumnSchema column : columns) {
      if (!column.isInvisible() && !column.getColumnName().contains(".")) {
        validColumnSchema.add(column);
      }
    }
    return convertToSparkSchema(table, validColumnSchema.toArray(new ColumnSchema[0]),
        forGlobalSort);
  }

  public static StructType convertToSparkSchema(CarbonTable table, ColumnSchema[] carbonColumns,
      boolean forGlobalSort) {
    List<StructField> fields = new ArrayList<>(carbonColumns.length);
    for (int i = 0; i < carbonColumns.length; i++) {
      ColumnSchema carbonColumn = carbonColumns[i];
      DataType dataType = carbonColumn.getDataType();
      if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(dataType)) {
        fields.add(new StructField(carbonColumn.getColumnName(),
            new DecimalType(carbonColumn.getPrecision(), carbonColumn.getScale()),
            true, Metadata.empty()));
      } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isStructType(dataType)) {
        fields.add(
            new StructField(
                carbonColumn.getColumnName(),
                CarbonMetastoreTypes.toDataType(
                    String.format("struct<%s>",
                        SparkTypeConverter.getStructChildren(table, carbonColumn.getColumnName()))),
                true,
                Metadata.empty()));
      } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isArrayType(dataType)) {
        fields.add(
            new StructField(
                carbonColumn.getColumnName(),
                CarbonMetastoreTypes.toDataType(
                    String.format("array<%s>",
                        SparkTypeConverter.getArrayChildren(
                            table,
                            carbonColumn.getColumnName()))),
                true,
                Metadata.empty()));
      } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isMapType(dataType)) {
        fields.add(
            new StructField(
                carbonColumn.getColumnName(),
                CarbonMetastoreTypes.toDataType(
                    String.format("map<%s>",
                        SparkTypeConverter.getMapChildren(
                            table,
                            carbonColumn.getColumnName()))),
                true,
                Metadata.empty()));
      } else {
        fields.add(new StructField(carbonColumn.getColumnName(),
            convertCarbonToSparkDataType(carbonColumn.getDataType(), forGlobalSort), true,
            Metadata.empty()));
      }
    }
    return new StructType(fields.toArray(new StructField[0]));
  }
}
