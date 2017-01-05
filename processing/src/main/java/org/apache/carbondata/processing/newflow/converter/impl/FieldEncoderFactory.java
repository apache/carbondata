/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow.converter.impl;

import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.FieldConverter;

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
   * @param dataField             column schema
   * @param cache                 dicionary cache.
   * @param carbonTableIdentifier table identifier
   * @param index                 index of column in the row.
   * @return
   */
  public FieldConverter createFieldEncoder(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index, String nullFormat,
      DictionaryClient client, Boolean useOnePass, String storePath) {
    // Converters are only needed for dimensions and measures it return null.
    if (dataField.getColumn().isDimesion()) {
      if (dataField.getColumn().hasEncoding(Encoding.DIRECT_DICTIONARY) &&
          !dataField.getColumn().isComplex()) {
        return new DirectDictionaryFieldConverterImpl(dataField, nullFormat, index);
      } else if (dataField.getColumn().hasEncoding(Encoding.DICTIONARY) &&
          !dataField.getColumn().isComplex()) {
        return new DictionaryFieldConverterImpl(dataField, cache, carbonTableIdentifier, nullFormat,
            index, client, useOnePass, storePath);
      } else if (dataField.getColumn().isComplex()) {
        return new ComplexFieldConverterImpl(
            createComplexType(dataField, cache, carbonTableIdentifier,
                    client, useOnePass, storePath), index);
      } else {
        return new NonDictionaryFieldConverterImpl(dataField, nullFormat, index);
      }
    } else {
      return new MeasureFieldConverterImpl(dataField, nullFormat, index);
    }
  }

  /**
   * Create parser for the carbon column.
   */
  private static GenericDataType createComplexType(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier,
      DictionaryClient client, Boolean useOnePass, String storePath) {
    return createComplexType(dataField.getColumn(), dataField.getColumn().getColName(), cache,
        carbonTableIdentifier, client, useOnePass, storePath);
  }

  /**
   * This method may be called recursively if the carbon column is complex type.
   *
   * @return GenericDataType
   */
  private static GenericDataType createComplexType(CarbonColumn carbonColumn, String parentName,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier,
      DictionaryClient client, Boolean useOnePass, String storePath) {
    switch (carbonColumn.getDataType()) {
      case ARRAY:
        List<CarbonDimension> listOfChildDimensions =
            ((CarbonDimension) carbonColumn).getListOfChildDimensions();
        // Create array parser with complex delimiter
        ArrayDataType arrayDataType =
            new ArrayDataType(carbonColumn.getColName(), parentName, carbonColumn.getColumnId());
        for (CarbonDimension dimension : listOfChildDimensions) {
          arrayDataType.addChildren(createComplexType(dimension, carbonColumn.getColName(), cache,
              carbonTableIdentifier, client, useOnePass, storePath));
        }
        return arrayDataType;
      case STRUCT:
        List<CarbonDimension> dimensions =
            ((CarbonDimension) carbonColumn).getListOfChildDimensions();
        // Create struct parser with complex delimiter
        StructDataType structDataType =
            new StructDataType(carbonColumn.getColName(), parentName, carbonColumn.getColumnId());
        for (CarbonDimension dimension : dimensions) {
          structDataType.addChildren(createComplexType(dimension, carbonColumn.getColName(), cache,
              carbonTableIdentifier, client, useOnePass, storePath));
        }
        return structDataType;
      case MAP:
        throw new UnsupportedOperationException("Complex type Map is not supported yet");
      default:
        return new PrimitiveDataType(carbonColumn.getColName(), parentName,
            carbonColumn.getColumnId(), (CarbonDimension) carbonColumn, cache,
            carbonTableIdentifier, client, useOnePass, storePath);
    }
  }
}
