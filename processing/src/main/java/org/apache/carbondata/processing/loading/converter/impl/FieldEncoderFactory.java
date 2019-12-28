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

package org.apache.carbondata.processing.loading.converter.impl;

import java.util.List;

import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.impl.binary.Base64BinaryDecoder;
import org.apache.carbondata.processing.loading.converter.impl.binary.BinaryDecoder;
import org.apache.carbondata.processing.loading.converter.impl.binary.DefaultBinaryDecoder;
import org.apache.carbondata.processing.loading.converter.impl.binary.HexBinaryDecoder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.commons.lang3.StringUtils;

public class FieldEncoderFactory {

  private static FieldEncoderFactory instance;

  private FieldEncoderFactory() {

  }

  public static FieldEncoderFactory getInstance() {
    if (instance == null) {
      instance = new FieldEncoderFactory();
    }
    return instance;
  }

  /**
   * Creates the FieldConverter for all dimensions, for measures return null.
   *
   * @param dataField               column schema
   * @param index                   index of column in the row
   * @param nullFormat              null format of the field
   * @param isEmptyBadRecord        whether is Empty BadRecord
   * @param isConvertToBinary       whether the no dictionary field to be converted to binary or not
   * @param binaryDecoder           carbon binary decoder for loading data
   * @return
   */
  public FieldConverter createFieldEncoder(
      DataField dataField, int index, String nullFormat, boolean isEmptyBadRecord,
      boolean isConvertToBinary, String binaryDecoder) {
    // Converters are only needed for dimensions and measures it return null.
    if (dataField.getColumn().isDimension()) {
      if (dataField.getColumn().hasEncoding(Encoding.DIRECT_DICTIONARY) &&
          !dataField.getColumn().isComplex()) {
        return new DirectDictionaryFieldConverterImpl(dataField, nullFormat, index,
            isEmptyBadRecord);
      } else if (dataField.getColumn().isComplex()) {
        return new ComplexFieldConverterImpl(createComplexDataType(dataField, nullFormat), index);
      } else if (dataField.getColumn().getDataType() == DataTypes.BINARY) {
        BinaryDecoder binaryDecoderObject = null;
        if (binaryDecoder.equalsIgnoreCase(
            CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_BASE64)) {
          binaryDecoderObject = new Base64BinaryDecoder();
        } else if (binaryDecoder.equalsIgnoreCase(
            CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_HEX)) {
          binaryDecoderObject = new HexBinaryDecoder();
        } else if (!StringUtils.isBlank(binaryDecoder)) {
          throw new CarbonDataLoadingException("Binary decoder only support Base64, " +
              "Hex or no decode for string, don't support " + binaryDecoder);
        } else {
          binaryDecoderObject = new DefaultBinaryDecoder();
        }

        return new BinaryFieldConverterImpl(dataField, nullFormat,
            index, isEmptyBadRecord, binaryDecoderObject);
      } else {
        // if the no dictionary column is a numeric column and no need to convert to binary
        // then treat it is as measure col
        // so that the adaptive encoding can be applied on it easily
        if (DataTypeUtil.isPrimitiveColumn(dataField.getColumn().getDataType())
            && !isConvertToBinary) {
          return new MeasureFieldConverterImpl(dataField, nullFormat, index, isEmptyBadRecord);
        }
        return new NonDictionaryFieldConverterImpl(dataField, nullFormat, index, isEmptyBadRecord);
      }
    } else {
      return new MeasureFieldConverterImpl(dataField, nullFormat, index, isEmptyBadRecord);
    }
  }

  /**
   * Create parser for the carbon column.
   */
  public static GenericDataType createComplexDataType(DataField dataField, String nullFormat) {
    return createComplexType(dataField.getColumn(), dataField.getColumn().getColName(), nullFormat);
  }

  /**
   * This method may be called recursively if the carbon column is complex type.
   *
   * @return GenericDataType
   */

  private static GenericDataType createComplexType(CarbonColumn carbonColumn, String parentName,
      String nullFormat) {
    DataType dataType = carbonColumn.getDataType();
    if (DataTypes.isArrayType(dataType) || DataTypes.isMapType(dataType)) {
      List<CarbonDimension> listOfChildDimensions =
          ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      // Create array parser with complex delimiter
      ArrayDataType arrayDataType =
          new ArrayDataType(carbonColumn.getColName(), parentName, carbonColumn.getColumnId(),
              carbonColumn.hasEncoding(Encoding.DICTIONARY));
      for (CarbonDimension dimension : listOfChildDimensions) {
        arrayDataType.addChildren(
            createComplexType(dimension, carbonColumn.getColName(), nullFormat));
      }
      return arrayDataType;
    } else if (DataTypes.isStructType(dataType)) {
      List<CarbonDimension> dimensions =
          ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      // Create struct parser with complex delimiter
      StructDataType structDataType =
          new StructDataType(carbonColumn.getColName(), parentName, carbonColumn.getColumnId(),
              carbonColumn.hasEncoding(Encoding.DICTIONARY));
      for (CarbonDimension dimension : dimensions) {
        structDataType.addChildren(
            createComplexType(dimension, carbonColumn.getColName(), nullFormat));
      }
      return structDataType;
    } else {
      return new PrimitiveDataType(carbonColumn, parentName, carbonColumn.getColumnId(),
          (CarbonDimension) carbonColumn, nullFormat);
    }
  }

}
