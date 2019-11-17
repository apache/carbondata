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

package org.apache.carbondata.sdk.file.arrow;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.metadata.datatype.ArrayType;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowUtils {

  public static final RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

  public static ArrowType toArrowType(DataType carbonDataType, String timeZoneId) {
    if (carbonDataType == DataTypes.STRING || carbonDataType == DataTypes.VARCHAR) {
      return ArrowType.Utf8.INSTANCE;
    } else if (carbonDataType == DataTypes.BYTE) {
      return new ArrowType.Int(DataTypes.BYTE.getSizeInBytes() * 8, true);
    } else if (carbonDataType == DataTypes.SHORT) {
      return new ArrowType.Int(DataTypes.SHORT.getSizeInBytes() * 8, true);
    } else if (carbonDataType == DataTypes.INT) {
      return new ArrowType.Int(DataTypes.INT.getSizeInBytes() * 8, true);
    } else if (carbonDataType == DataTypes.LONG) {
      return new ArrowType.Int(DataTypes.LONG.getSizeInBytes() * 8, true);
    } else if (carbonDataType == DataTypes.FLOAT) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (carbonDataType == DataTypes.DOUBLE) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (carbonDataType == DataTypes.BOOLEAN) {
      return ArrowType.Bool.INSTANCE;
    } else if (carbonDataType instanceof DecimalType) {
      // instance of check is for findbugs, instead of datatypes check
      DecimalType decimal = (DecimalType) carbonDataType;
      return new ArrowType.Decimal(decimal.getPrecision(), decimal.getScale());
    } else if (carbonDataType == DataTypes.TIMESTAMP) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId);
    } else if (carbonDataType == DataTypes.DATE) {
      return new ArrowType.Date(DateUnit.DAY);
    } else if (carbonDataType == DataTypes.BINARY) {
      return ArrowType.Binary.INSTANCE;
    } else {
      throw new UnsupportedOperationException("Operation not supported");
    }
  }

  public static org.apache.arrow.vector.types.pojo.Field toArrowField(String name,
      DataType dataType, String timeZoneId) {
    if (dataType instanceof ArrayType) {
      // instance of check is for findbugs, instead of datatypes check
      FieldType fieldType = new FieldType(true, ArrowType.List.INSTANCE, null);
      List<org.apache.arrow.vector.types.pojo.Field> structFields = new ArrayList<>();
      DataType elementType = ((ArrayType) dataType).getElementType();
      structFields.add(toArrowField("element", elementType, timeZoneId));
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, structFields);
    } else if (dataType instanceof StructType) {
      // instance of check is for findbugs, instead of datatypes check
      StructType dataType1 = (StructType) dataType;
      FieldType fieldType = new FieldType(true, ArrowType.Struct.INSTANCE, null);
      List<StructField> fields = dataType1.getFields();
      List<org.apache.arrow.vector.types.pojo.Field> structFields = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        structFields.add(
            toArrowField(fields.get(i).getFieldName(), fields.get(i).getDataType(), timeZoneId));
      }
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, structFields);
    } else {
      FieldType fieldType = new FieldType(true, toArrowType(dataType, timeZoneId), null);
      return new org.apache.arrow.vector.types.pojo.Field(name, fieldType,
          new ArrayList<org.apache.arrow.vector.types.pojo.Field>());
    }
  }

  public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(Schema carbonSchema,
      String timeZoneId) {
    final Field[] fields = carbonSchema.getFields();
    Set<org.apache.arrow.vector.types.pojo.Field> arrowField = new LinkedHashSet<>();
    for (int i = 0; i < fields.length; i++) {
      arrowField.add(toArrowField(fields[i].getFieldName(), fields[i].getDataType(), timeZoneId));
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowField);
  }
}
