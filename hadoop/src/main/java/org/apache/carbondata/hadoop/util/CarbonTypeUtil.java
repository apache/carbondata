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

package org.apache.carbondata.hadoop.util;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;

public class CarbonTypeUtil {

  public static org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType) {
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
      return DataTypes.TimestampType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      return DataTypes.DateType;
    } else {
      return null;
    }
  }

  public static StructField[] convertCarbonSchemaToSparkSchema(CarbonColumn[] carbonColumns) {
    StructField[] fields = new StructField[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      CarbonColumn carbonColumn = carbonColumns[i];
      if (carbonColumn.isDimension()) {
        if (carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(carbonColumn.getDataType());
          fields[i] = new StructField(carbonColumn.getColName(),
              CarbonTypeUtil.convertCarbonToSparkDataType(generator.getReturnType()), true, null);
        } else if (!carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
          fields[i] = new StructField(carbonColumn.getColName(),
              CarbonTypeUtil.convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else if (carbonColumn.isComplex()) {
          fields[i] = new StructField(carbonColumn.getColName(),
              CarbonTypeUtil.convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(), CarbonTypeUtil
              .convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.INT), true, null);
        }
      } else if (carbonColumn.isMeasure()) {
        DataType dataType = carbonColumn.getDataType();
        if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
          fields[i] = new StructField(carbonColumn.getColName(),
              CarbonTypeUtil.convertCarbonToSparkDataType(dataType), true, null);
        } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(dataType)) {
          CarbonMeasure measure = (CarbonMeasure) carbonColumn;
          fields[i] = new StructField(carbonColumn.getColName(),
              new DecimalType(measure.getPrecision(), measure.getScale()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(), CarbonTypeUtil
              .convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE), true, null);
        }
      }
    }
    return fields;
  }

}
