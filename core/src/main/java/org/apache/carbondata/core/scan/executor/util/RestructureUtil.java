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
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
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
  public static List<QueryDimension> createDimensionInfoAndGetCurrentBlockQueryDimension(
      BlockExecutionInfo blockExecutionInfo, List<QueryDimension> queryDimensions,
      List<CarbonDimension> tableBlockDimensions, List<CarbonDimension> tableComplexDimension) {
    List<QueryDimension> presentDimension =
        new ArrayList<QueryDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    boolean[] isDimensionExists = new boolean[queryDimensions.size()];
    Object[] defaultValues = new Object[queryDimensions.size()];
    // create dimension information instance
    DimensionInfo dimensionInfo = new DimensionInfo(isDimensionExists, defaultValues);
    int newDictionaryColumnCount = 0;
    int newNoDictionaryColumnCount = 0;
    // selecting only those dimension which is present in the query
    int dimIndex = 0;
    for (QueryDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        presentDimension.add(queryDimension);
        isDimensionExists[dimIndex] = true;
      } else {
        for (CarbonDimension tableDimension : tableBlockDimensions) {
          if (tableDimension.getColumnId().equals(queryDimension.getDimension().getColumnId())) {
            QueryDimension currentBlockDimension = new QueryDimension(tableDimension.getColName());
            tableDimension.getColumnSchema()
                .setDataType(queryDimension.getDimension().getDataType());
            tableDimension.getColumnSchema()
                .setPrecision(queryDimension.getDimension().getColumnSchema().getPrecision());
            tableDimension.getColumnSchema()
                .setScale(queryDimension.getDimension().getColumnSchema().getScale());
            tableDimension.getColumnSchema()
                .setDefaultValue(queryDimension.getDimension().getDefaultValue());
            currentBlockDimension.setDimension(tableDimension);
            currentBlockDimension.setQueryOrder(queryDimension.getQueryOrder());
            presentDimension.add(currentBlockDimension);
            isDimensionExists[dimIndex] = true;
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
            QueryDimension currentBlockDimension = new QueryDimension(tableDimension.getColName());
            // TODO: for complex dimension set scale and precision by traversing
            // the child dimensions
            currentBlockDimension.setDimension(tableDimension);
            currentBlockDimension.setQueryOrder(queryDimension.getQueryOrder());
            presentDimension.add(currentBlockDimension);
            isDimensionExists[dimIndex] = true;
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
      defaultValueToBeConsidered = getNoDictionaryDefaultValue(defaultValue);
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
      dictionaryDefaultValue = new Integer(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
    } else {
      dictionaryDefaultValue =
          new Integer(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY + 1);
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
  private static Object getNoDictionaryDefaultValue(byte[] defaultValue) {
    Object noDictionaryDefaultValue = null;
    if (!isDefaultValueNull(defaultValue)) {
      noDictionaryDefaultValue = UTF8String.fromBytes(defaultValue);
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
    if (null == defaultValue) {
      return true;
    }
    return false;
  }

  /**
   * Below method is to add dimension children for complex type dimension as
   * internally we are creating dimension column for each each complex
   * dimension so when complex query dimension request will come in the query,
   * we need to add its children as it is hidden from the user For example if
   * complex dimension is of Array of String[2] so we are storing 3 dimension
   * and when user will query for complex type i.e. array type we need to add
   * its children and then we will read respective block and create a tuple
   * based on all three dimension
   *
   * @param queryDimensions      current query dimensions
   * @param tableBlockDimensions dimensions which is present in the table block
   * @return updated dimension(after adding complex type children)
   */
  public static List<CarbonDimension> addChildrenForComplexTypeDimension(
      List<CarbonDimension> queryDimensions, List<CarbonDimension> tableBlockDimensions) {
    List<CarbonDimension> updatedQueryDimension = new ArrayList<CarbonDimension>();
    int numberOfChildren = 0;
    for (CarbonDimension queryDimension : queryDimensions) {
      // if number of child is zero, then it is not a complex dimension
      // so directly add it query dimension
      if (queryDimension.getNumberOfChild() == 0) {
        updatedQueryDimension.add(queryDimension);
      }
      // if number of child is more than 1 then add all its children
      numberOfChildren = queryDimension.getOrdinal() + queryDimension.getNumberOfChild();
      for (int j = queryDimension.getOrdinal(); j < numberOfChildren; j++) {
        updatedQueryDimension.add(tableBlockDimensions.get(j));
      }
    }
    return updatedQueryDimension;
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
      String value = null;
      switch (columnSchema.getDataType()) {
        case SHORT:
        case INT:
        case LONG:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          measureDefaultValue = Long.parseLong(value);
          break;
        case DECIMAL:
          BigDecimal decimal = DataTypeUtil.byteToBigDecimal(defaultValue);
          if (columnSchema.getScale() > decimal.scale()) {
            decimal = decimal.setScale(columnSchema.getScale(), RoundingMode.HALF_UP);
          }
          measureDefaultValue = decimal;
          break;
        default:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
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
      String value = null;
      switch (columnSchema.getDataType()) {
        case SHORT:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          measureDefaultValue = Short.parseShort(value);
          break;
        case INT:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          measureDefaultValue = Integer.parseInt(value);
          break;
        case LONG:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          measureDefaultValue = Long.parseLong(value);
          break;
        case DECIMAL:
          BigDecimal decimal = DataTypeUtil.byteToBigDecimal(defaultValue);
          if (columnSchema.getScale() > decimal.scale()) {
            decimal = decimal.setScale(columnSchema.getScale(), RoundingMode.HALF_UP);
          }
          measureDefaultValue = Decimal.apply(decimal);
          break;
        default:
          value =
              new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
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
  public static List<QueryMeasure> createMeasureInfoAndGetCurrentBlockQueryMeasures(
      BlockExecutionInfo blockExecutionInfo, List<QueryMeasure> queryMeasures,
      List<CarbonMeasure> currentBlockMeasures) {
    MeasureInfo measureInfo = new MeasureInfo();
    List<QueryMeasure> presentMeasure = new ArrayList<>(queryMeasures.size());
    int numberOfMeasureInQuery = queryMeasures.size();
    List<Integer> measureOrdinalList = new ArrayList<>(numberOfMeasureInQuery);
    Object[] defaultValues = new Object[numberOfMeasureInQuery];
    boolean[] measureExistsInCurrentBlock = new boolean[numberOfMeasureInQuery];
    int index = 0;
    for (QueryMeasure queryMeasure : queryMeasures) {
      // if query measure exists in current dimension measures
      // then setting measure exists is true
      // otherwise adding a default value of a measure
      for (CarbonMeasure carbonMeasure : currentBlockMeasures) {
        if (carbonMeasure.getColumnId().equals(queryMeasure.getMeasure().getColumnId())) {
          QueryMeasure currentBlockMeasure = new QueryMeasure(carbonMeasure.getColName());
          carbonMeasure.getColumnSchema().setDataType(queryMeasure.getMeasure().getDataType());
          carbonMeasure.getColumnSchema().setPrecision(queryMeasure.getMeasure().getPrecision());
          carbonMeasure.getColumnSchema().setScale(queryMeasure.getMeasure().getScale());
          carbonMeasure.getColumnSchema()
              .setDefaultValue(queryMeasure.getMeasure().getDefaultValue());
          currentBlockMeasure.setMeasure(carbonMeasure);
          currentBlockMeasure.setQueryOrder(queryMeasure.getQueryOrder());
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
