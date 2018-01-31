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
package org.apache.carbondata.core.scan.executor.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Utility class for restructuring
 */
public class RestructureUtil {

  /**
   * Below method will be used to get the updated query dimension updation
   * means, after restructuring some dimension will be not present in older
   * table blocks in that case we need to select only those dimension out of
   * query dimension which is present in the current table block
   *
   * @param blockExecutionInfo
   * @param queryDimensions
   * @param tableBlockDimensions
   * @param tableComplexDimension
   * @return list of query dimension which is present in the table block
   */
  public static List<ProjectionDimension> createDimensionInfoAndGetCurrentBlockQueryDimension(
      BlockExecutionInfo blockExecutionInfo, List<ProjectionDimension> queryDimensions,
      List<CarbonDimension> tableBlockDimensions, List<CarbonDimension> tableComplexDimension,
      int measureCount) {
    List<ProjectionDimension> presentDimension =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    boolean[] isDimensionExists = new boolean[queryDimensions.size()];
    Object[] defaultValues = new Object[queryDimensions.size()];
    // create dimension information instance
    DimensionInfo dimensionInfo = new DimensionInfo(isDimensionExists, defaultValues);
    dimensionInfo.dataType = new DataType[queryDimensions.size() + measureCount];
    int newDictionaryColumnCount = 0;
    int newNoDictionaryColumnCount = 0;
    // selecting only those dimension which is present in the query
    int dimIndex = 0;
    for (ProjectionDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        presentDimension.add(queryDimension);
        isDimensionExists[dimIndex] = true;
        dimensionInfo.dataType[queryDimension.getOrdinal()] =
            queryDimension.getDimension().getDataType();
      } else {
        for (CarbonDimension tableDimension : tableBlockDimensions) {
          if (tableDimension.getColumnId().equals(queryDimension.getDimension().getColumnId())) {
            ProjectionDimension currentBlockDimension = new ProjectionDimension(tableDimension);
            tableDimension.getColumnSchema()
                .setPrecision(queryDimension.getDimension().getColumnSchema().getPrecision());
            tableDimension.getColumnSchema()
                .setScale(queryDimension.getDimension().getColumnSchema().getScale());
            tableDimension.getColumnSchema()
                .setDefaultValue(queryDimension.getDimension().getDefaultValue());
            currentBlockDimension.setOrdinal(queryDimension.getOrdinal());
            presentDimension.add(currentBlockDimension);
            isDimensionExists[dimIndex] = true;
            dimensionInfo.dataType[currentBlockDimension.getOrdinal()] =
                currentBlockDimension.getDimension().getDataType();
            break;
          }
        }
        // if dimension is found then no need to search in the complex dimensions list
        if (isDimensionExists[dimIndex]) {
          dimIndex++;
          continue;
        }
        for (CarbonDimension tableDimension : tableComplexDimension) {
          if (tableDimension.getColumnId().equals(queryDimension.getDimension().getColumnId())) {
            ProjectionDimension currentBlockDimension = new ProjectionDimension(tableDimension);
            // TODO: for complex dimension set scale and precision by traversing
            // the child dimensions
            currentBlockDimension.setOrdinal(queryDimension.getOrdinal());
            presentDimension.add(currentBlockDimension);
            isDimensionExists[dimIndex] = true;
            dimensionInfo.dataType[currentBlockDimension.getOrdinal()] =
                currentBlockDimension.getDimension().getDataType();
            break;
          }
        }
        // add default value only in case query dimension is not found in the current block
        if (!isDimensionExists[dimIndex]) {
          defaultValues[dimIndex] = validateAndGetDefaultValue(queryDimension.getDimension());
          blockExecutionInfo.setRestructuredBlock(true);
          // set the flag to say whether a new dictionary column or no dictionary column
          // has been added. This will be useful after restructure for compaction scenarios where
          // newly added columns data need to be filled
          if (queryDimension.getDimension().hasEncoding(Encoding.DICTIONARY)) {
            dimensionInfo.setDictionaryColumnAdded(true);
            newDictionaryColumnCount++;
          } else {
            dimensionInfo.setNoDictionaryColumnAdded(true);
            newNoDictionaryColumnCount++;
          }
        }
      }
      dimIndex++;
    }
    dimensionInfo.setNewDictionaryColumnCount(newDictionaryColumnCount);
    dimensionInfo.setNewNoDictionaryColumnCount(newNoDictionaryColumnCount);
    blockExecutionInfo.setDimensionInfo(dimensionInfo);
    return presentDimension;
  }

  /**
   * This method will validate and return the default value to be
   * filled at the time of result preparation
   *
   * @param queryDimension
   * @return
   */
  public static Object validateAndGetDefaultValue(CarbonDimension queryDimension) {
    byte[] defaultValue = queryDimension.getDefaultValue();
    Object defaultValueToBeConsidered = null;
    if (CarbonUtil.hasEncoding(queryDimension.getEncoder(), Encoding.DICTIONARY)) {
      // direct dictionary case
      if (CarbonUtil.hasEncoding(queryDimension.getEncoder(), Encoding.DIRECT_DICTIONARY)) {
        defaultValueToBeConsidered = getDirectDictionaryDefaultValue(queryDimension.getDataType(),
            queryDimension.getDefaultValue());
      } else {
        // dictionary case
        defaultValueToBeConsidered = getDictionaryDefaultValue(defaultValue);
      }
    } else {
      // no dictionary
      defaultValueToBeConsidered =
          getNoDictionaryDefaultValue(queryDimension.getDataType(), defaultValue);
    }
    return defaultValueToBeConsidered;
  }

  /**
   * Method for computing default value for dictionary column
   *
   * @param defaultValue
   * @return
   */
  private static Object getDictionaryDefaultValue(byte[] defaultValue) {
    Object dictionaryDefaultValue = null;
    // dictionary has 2 cases:
    // 1. If default value is specified then its surrogate key will be 2
    // 2.  If default value is not specified then its surrogate key will be
    // 1 which is for member default value null
    if (isDefaultValueNull(defaultValue)) {
      dictionaryDefaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
    } else {
      dictionaryDefaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY + 1;
    }
    return dictionaryDefaultValue;
  }

  /**
   * Method for computing default value for direct dictionary
   *
   * @param dataType
   * @param defaultValue
   * @return
   */
  private static Object getDirectDictionaryDefaultValue(DataType dataType, byte[] defaultValue) {
    Object directDictionaryDefaultValue = null;
    if (!isDefaultValueNull(defaultValue)) {
      DirectDictionaryGenerator directDictionaryGenerator =
          DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(dataType);
      if (directDictionaryGenerator != null) {
        String value =
            new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        directDictionaryDefaultValue =
            directDictionaryGenerator.getValueFromSurrogate(Integer.parseInt(value));
      }
    }
    return directDictionaryDefaultValue;
  }

  /**
   * Method for computing default value for no dictionary
   *
   * @param defaultValue
   * @return
   */
  private static Object getNoDictionaryDefaultValue(DataType datatype, byte[] defaultValue) {
    Object noDictionaryDefaultValue = null;
    String value = null;
    if (!isDefaultValueNull(defaultValue)) {
      if (datatype == DataTypes.INT) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        noDictionaryDefaultValue = Integer.parseInt(value);
      } else if (datatype == DataTypes.LONG) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        noDictionaryDefaultValue = Long.parseLong(value);
      } else if (datatype == DataTypes.TIMESTAMP) {
        long timestampValue = ByteUtil.toLong(defaultValue, 0, defaultValue.length);
        noDictionaryDefaultValue = timestampValue * 1000L;
      } else {
        noDictionaryDefaultValue = UTF8String.fromBytes(defaultValue);
      }
    }
    return noDictionaryDefaultValue;
  }

  /**
   * This method will validate whether a given value is empty or null
   *
   * @param defaultValue
   * @return
   */
  private static boolean isDefaultValueNull(byte[] defaultValue) {
    return null == defaultValue;
  }

  /**
   * Method for computing measure default value based on the data type
   *
   * @param columnSchema
   * @param defaultValue
   * @return
   */
  public static Object getMeasureDefaultValue(ColumnSchema columnSchema, byte[] defaultValue) {
    Object measureDefaultValue = null;
    if (!isDefaultValueNull(defaultValue)) {
      String value;
      DataType dataType = columnSchema.getDataType();
      if (dataType == DataTypes.SHORT) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Short.valueOf(value);
      } else if (dataType == DataTypes.LONG) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Long.parseLong(value);
      } else if (dataType == DataTypes.INT) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Integer.parseInt(value);
      } else if (dataType == DataTypes.BOOLEAN) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Boolean.valueOf(value);
      } else if (DataTypes.isDecimal(dataType)) {
        BigDecimal decimal = DataTypeUtil.byteToBigDecimal(defaultValue);
        if (columnSchema.getScale() > decimal.scale()) {
          decimal = decimal.setScale(columnSchema.getScale(), RoundingMode.HALF_UP);
        }
        measureDefaultValue = decimal;
      } else {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        Double parsedValue = Double.valueOf(value);
        if (!Double.isInfinite(parsedValue) && !Double.isNaN(parsedValue)) {
          measureDefaultValue = parsedValue;
        }
      }
    }
    return measureDefaultValue;
  }

  /**
   * Gets the default value based on the column data type.
   *
   * @param columnSchema
   * @param defaultValue
   * @return
   */
  public static Object getMeasureDefaultValueByType(ColumnSchema columnSchema,
      byte[] defaultValue) {
    Object measureDefaultValue = null;
    if (!isDefaultValueNull(defaultValue)) {
      String value;
      DataType dataType = columnSchema.getDataType();
      if (dataType == DataTypes.SHORT) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Short.parseShort(value);
      } else if (dataType == DataTypes.INT) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Integer.parseInt(value);
      } else if (dataType == DataTypes.LONG) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Long.parseLong(value);
      } else if (dataType == DataTypes.BOOLEAN) {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        measureDefaultValue = Boolean.valueOf(value);
      } else if (DataTypes.isDecimal(dataType)) {
        BigDecimal decimal = DataTypeUtil.byteToBigDecimal(defaultValue);
        if (columnSchema.getScale() > decimal.scale()) {
          decimal = decimal.setScale(columnSchema.getScale(), RoundingMode.HALF_UP);
        }
        measureDefaultValue = Decimal.apply(decimal);
      } else {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        Double parsedValue = Double.valueOf(value);
        if (!Double.isInfinite(parsedValue) && !Double.isNaN(parsedValue)) {
          measureDefaultValue = parsedValue;
        }
      }
    }
    return measureDefaultValue;
  }

  /**
   * Below method will be used to prepare the measure info object
   * in this method some of the properties which will be extracted
   * from query measure and current block measures will be set
   *
   * @param blockExecutionInfo
   * @param queryMeasures        measures present in query
   * @param currentBlockMeasures current block measures
   * @return measures present in the block
   */
  public static List<ProjectionMeasure> createMeasureInfoAndGetCurrentBlockQueryMeasures(
      BlockExecutionInfo blockExecutionInfo, List<ProjectionMeasure> queryMeasures,
      List<CarbonMeasure> currentBlockMeasures) {
    MeasureInfo measureInfo = new MeasureInfo();
    List<ProjectionMeasure> presentMeasure = new ArrayList<>(queryMeasures.size());
    int numberOfMeasureInQuery = queryMeasures.size();
    List<Integer> measureOrdinalList = new ArrayList<>(numberOfMeasureInQuery);
    Object[] defaultValues = new Object[numberOfMeasureInQuery];
    boolean[] measureExistsInCurrentBlock = new boolean[numberOfMeasureInQuery];
    int index = 0;
    for (ProjectionMeasure queryMeasure : queryMeasures) {
      // if query measure exists in current dimension measures
      // then setting measure exists is true
      // otherwise adding a default value of a measure
      for (CarbonMeasure carbonMeasure : currentBlockMeasures) {
        if (carbonMeasure.getColumnId().equals(queryMeasure.getMeasure().getColumnId())) {
          ProjectionMeasure currentBlockMeasure = new ProjectionMeasure(carbonMeasure);
          carbonMeasure.getColumnSchema().setDataType(queryMeasure.getMeasure().getDataType());
          carbonMeasure.getColumnSchema().setPrecision(queryMeasure.getMeasure().getPrecision());
          carbonMeasure.getColumnSchema().setScale(queryMeasure.getMeasure().getScale());
          carbonMeasure.getColumnSchema()
              .setDefaultValue(queryMeasure.getMeasure().getDefaultValue());
          currentBlockMeasure.setOrdinal(queryMeasure.getOrdinal());
          presentMeasure.add(currentBlockMeasure);
          measureOrdinalList.add(carbonMeasure.getOrdinal());
          measureExistsInCurrentBlock[index] = true;
          break;
        }
      }
      if (!measureExistsInCurrentBlock[index]) {
        defaultValues[index] = getMeasureDefaultValue(queryMeasure.getMeasure().getColumnSchema(),
            queryMeasure.getMeasure().getDefaultValue());
        blockExecutionInfo.setRestructuredBlock(true);
      }
      index++;
    }
    int[] measureOrdinals =
        ArrayUtils.toPrimitive(measureOrdinalList.toArray(new Integer[measureOrdinalList.size()]));
    measureInfo.setDefaultValues(defaultValues);
    measureInfo.setMeasureOrdinals(measureOrdinals);
    measureInfo.setMeasureExists(measureExistsInCurrentBlock);
    blockExecutionInfo.setMeasureInfo(measureInfo);
    return presentMeasure;
  }
}
