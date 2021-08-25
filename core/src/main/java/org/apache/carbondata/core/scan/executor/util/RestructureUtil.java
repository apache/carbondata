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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Utility class for restructuring
 */
public class RestructureUtil {
  // if table column is of complex type- this look up stores the column id of the parent
  // (as well as children) [tableColumn_id -> tableColumn_name]. This helps to determine the
  // existence of incoming query column by matching based on id.
  private static Map<String, String> existingTableColumnIDMap;

  /**
   * Below method will be used to get the updated query dimension update
   * means, after restructuring some dimension will be not present in older
   * table blocks in that case we need to select only those dimension out of
   * query dimension which is present in the current table block
   *
   * @param blockExecutionInfo
   * @param queryDimensions
   * @param tableBlockDimensions
   * @param tableComplexDimension
   * @param isTransactionalTable
   * @return list of query dimension which is present in the table block
   */
  public static List<ProjectionDimension> createDimensionInfoAndGetCurrentBlockQueryDimension(
      BlockExecutionInfo blockExecutionInfo, ProjectionDimension[] queryDimensions,
      List<CarbonDimension> tableBlockDimensions, List<CarbonDimension> tableComplexDimension,
      int measureCount, boolean isTransactionalTable, QueryModel queryModel) {
    List<ProjectionDimension> presentDimension =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    boolean[] isDimensionExists = new boolean[queryDimensions.length];
    Object[] defaultValues = new Object[queryDimensions.length];
    // create dimension information instance
    DimensionInfo dimensionInfo = new DimensionInfo(isDimensionExists, defaultValues);
    dimensionInfo.dataType = new DataType[queryDimensions.length + measureCount];
    int newDictionaryColumnCount = 0;
    int newNoDictionaryColumnCount = 0;
    int newNoDictionaryComplexColumnCount = 0;
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
          if (isColumnMatches(isTransactionalTable, queryDimension.getDimension(),
              tableDimension)) {
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
          if (isColumnMatches(isTransactionalTable, queryDimension.getDimension(),
              tableDimension)) {
            ProjectionDimension currentBlockDimension;
            // If projection dimension is child of struct field and contains Parent Ordinal
            if (null != queryDimension.getDimension().getComplexParentDimension()) {
              currentBlockDimension = new ProjectionDimension(queryDimension.getDimension());
            } else {
              currentBlockDimension = new ProjectionDimension(tableDimension);
              // for complex dimension update datatype, set scale and precision by traversing
              // the child dimensions. Spark requires the resultant row with latest datatype,
              // update the dimension here to collect the resultant rows with updated datatype.
              fillNewDatatypesForComplexChildren(currentBlockDimension.getDimension(),
                  queryDimension.getDimension());
            }
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
          queryModel.setDirectVectorFill(false);
          // set the flag to say whether a new dictionary column or no dictionary column
          // has been added. This will be useful after restructure for compaction scenarios where
          // newly added columns data need to be filled
          if (queryDimension.getDimension().getDataType() == DataTypes.DATE) {
            dimensionInfo.setDictionaryColumnAdded(true);
            newDictionaryColumnCount++;
          } else if (queryDimension.getDimension().getDataType().isComplexType()) {
            dimensionInfo.setComplexColumnAdded(true);
            newNoDictionaryComplexColumnCount++;
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
    dimensionInfo.setNewComplexColumnCount(newNoDictionaryComplexColumnCount);
    blockExecutionInfo.setDimensionInfo(dimensionInfo);
    return presentDimension;
  }

  public static CarbonDimension getCarbonDimension(String columnId,
      List<CarbonDimension> dimensions) {
    CarbonDimension carbonDimension = null;
    if (dimensions == null) {
      return carbonDimension;
    }
    for (CarbonDimension dim : dimensions) {
      if (dim.isComplex()) {
        carbonDimension = getCarbonDimension(columnId, dim.getListOfChildDimensions());
      } else if (dim.getColumnId().equalsIgnoreCase(columnId)) {
        carbonDimension = dim;
        break;
      }
    }
    return carbonDimension;
  }

  public static void fillNewDatatypesForComplexChildren(CarbonDimension currentDimension,
      CarbonDimension tableDimension) {
    if (tableDimension.getListOfChildDimensions() == null) {
      return;
    }
    for (CarbonDimension childDimension : tableDimension.getListOfChildDimensions()) {
      if (childDimension.getDataType().isComplexType()) {
        fillNewDatatypesForComplexChildren(currentDimension, childDimension);
      } else {
        // only in case of primitive types, datatype is allowed to be altered.
        // Find and update the current block dimension datatype.
        CarbonDimension currentChildDimension = getCarbonDimension(childDimension.getColumnId(),
            currentDimension.getListOfChildDimensions());
        if (currentChildDimension != null) {
          ColumnSchema currentSchema = currentChildDimension.getColumnSchema();
          currentSchema.setDataType(childDimension.getDataType());
          currentSchema.setPrecision(childDimension.getColumnSchema().getPrecision());
          currentSchema.setScale(childDimension.getColumnSchema().getScale());
        }
      }
    }
  }

  public static void fillExistingTableColumnIDMap(CarbonDimension tableColumn) {
    existingTableColumnIDMap.put(tableColumn.getColumnId(), tableColumn.getColName());
    List<CarbonDimension> children = tableColumn.getListOfChildDimensions();
    if (children == null) return;
    for (CarbonDimension dimension : children) {
      fillExistingTableColumnIDMap(dimension);
    }
  }

  /**
   * Match the columns for transactional and non transactional tables
   * @param isTransactionalTable
   * @param queryColumn - column entity that is present in the fired query or in the query model.
   * @param tableColumn - column entity that is present in the table block or in the segment
   *                      properties.
   * @return
   */
  public static boolean isColumnMatches(boolean isTransactionalTable,
      CarbonColumn queryColumn, CarbonColumn tableColumn) {
    // If it is non transactional table just check the column names, no need to validate
    // column id as multiple sdk's output placed in a single folder doesn't have same
    // column ID but can have same column name
    if (tableColumn.getDataType().isComplexType() && !(tableColumn.getDataType().getId()
        == DataTypes.ARRAY_TYPE_ID)) {
      if (tableColumn.getColumnId().equalsIgnoreCase(queryColumn.getColumnId()) || tableColumn
          .isColumnMatchBasedOnId(queryColumn)) {
        return true;
      } else {
        if (tableColumn instanceof CarbonDimension) {
          // insert list of table column id into a lookUp set, which will later be used to match
          // against query column id
          existingTableColumnIDMap = new HashMap<>();
          fillExistingTableColumnIDMap((CarbonDimension) tableColumn);
        }
        return isColumnMatchesStruct(tableColumn, queryColumn);
      }
    } else {
      return (tableColumn.getColumnId().equalsIgnoreCase(queryColumn.getColumnId()) || (
          !isTransactionalTable && tableColumn.getColName()
              .equalsIgnoreCase(queryColumn.getColName()))
          // In case of SDK, columnId is same as columnName therefore the following check will
          // ensure that if the table columnName is same as the query columnName and the table
          // columnId is the same as table columnName then it's a valid columnName to be scanned.
          || tableColumn.isColumnMatchBasedOnId(queryColumn));
    }
  }

  /**
   * In case of Multilevel Complex column - Struct/StructOfStruct, traverse all the child dimensions
   * of tableColumn to check if any of its column Id has matched with that of queryColumn .
   *
   * @param tableColumn - column entity that is present in the table block or in the segment
   *                      properties.
   * @param queryColumn - column entity that is present in the fired query or in the query model.
   * tableColumn name and queryColumn name may or may not be the same in case schema has evolved.
   * Hence matching happens based on the column ID
   * @return
   */
  private static boolean isColumnMatchesStruct(CarbonColumn tableColumn, CarbonColumn queryColumn) {
    if (tableColumn instanceof CarbonDimension) {
      List<CarbonDimension> childrenDimensions =
          ((CarbonDimension) tableColumn).getListOfChildDimensions();
      CarbonDimension carbonDimension;
      String[] colSplits = queryColumn.getColName().split("\\.");
      StringBuffer tempColName = new StringBuffer(colSplits[0]);
      for (String colSplit : colSplits) {
        if (!tempColName.toString().equalsIgnoreCase(colSplit)) {
          tempColName = tempColName.append(CarbonCommonConstants.POINT).append(colSplit);
        }
        carbonDimension =
            CarbonTable.getCarbonDimension(tempColName.toString(), childrenDimensions);
        if (carbonDimension == null) {
          // Avoid returning true in case of SDK as the column name contains the id.
          if (existingTableColumnIDMap != null && existingTableColumnIDMap
              .containsKey(queryColumn.getColumnId())) {
            String columnName = existingTableColumnIDMap.get(queryColumn.getColumnId());
            if (columnName != null && !columnName.contains(queryColumn.getColumnId())) {
              return true;
            }
          }
        } else {
          // In case of SDK the columnId and columnName is same and this check will ensure for
          // all the child columns that the table column name is equal to query column name and
          // table columnId is equal to table columnName
          if (carbonDimension.getColumnSchema().getColumnUniqueId()
              .equalsIgnoreCase(queryColumn.getColumnId()) || (
              carbonDimension.getColumnSchema().getColumnUniqueId()
                  .equalsIgnoreCase(carbonDimension.getColName()) && carbonDimension.getColName()
                  .equalsIgnoreCase(queryColumn.getColName()))) {
            return true;
          }
          if (carbonDimension.getListOfChildDimensions() != null) {
            childrenDimensions = carbonDimension.getListOfChildDimensions();
          }
        }
      }
    }
    return false;
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
    if (queryDimension.getDataType() == DataTypes.DATE) {
      // direct dictionary case
      defaultValueToBeConsidered = getDirectDictionaryDefaultValue(queryDimension.getDataType(),
          queryDimension.getDefaultValue());
    } else {
      // no dictionary
      defaultValueToBeConsidered =
          getNoDictionaryDefaultValue(queryDimension.getDataType(), defaultValue);
    }
    return defaultValueToBeConsidered;
  }

  /**
   * Method for computing default value for direct dictionary
   *
   * @param dataType
   * @param defaultValue
   * @return
   */
  public static Object getDirectDictionaryDefaultValue(DataType dataType, byte[] defaultValue) {
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
        long timestampValue = ByteUtil.toXorLong(defaultValue, 0, defaultValue.length);
        noDictionaryDefaultValue = timestampValue * 1000L;
      } else {
        noDictionaryDefaultValue =
            DataTypeUtil.getDataTypeConverter().convertFromByteToUTF8Bytes(defaultValue);
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
        double parsedValue = Double.parseDouble(value);
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
        measureDefaultValue = decimal;
      } else {
        value = new String(defaultValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        double parsedValue = Double.parseDouble(value);
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
   * @param isTransactionalTable
   * @return measures present in the block
   */
  public static List<ProjectionMeasure> createMeasureInfoAndGetCurrentBlockQueryMeasures(
      BlockExecutionInfo blockExecutionInfo, ProjectionMeasure[] queryMeasures,
      List<CarbonMeasure> currentBlockMeasures, boolean isTransactionalTable,
      QueryModel queryModel) {
    MeasureInfo measureInfo = new MeasureInfo();
    List<ProjectionMeasure> presentMeasure = new ArrayList<>(queryMeasures.length);
    int numberOfMeasureInQuery = queryMeasures.length;
    List<Integer> measureOrdinalList = new ArrayList<>(numberOfMeasureInQuery);
    Object[] defaultValues = new Object[numberOfMeasureInQuery];
    boolean[] measureExistsInCurrentBlock = new boolean[numberOfMeasureInQuery];
    DataType[] measureDataTypes = new DataType[numberOfMeasureInQuery];
    int index = 0;
    for (ProjectionMeasure queryMeasure : queryMeasures) {
      // if query measure exists in current dimension measures
      // then setting measure exists is true
      // otherwise adding a default value of a measure
      for (CarbonMeasure carbonMeasure : currentBlockMeasures) {
        if (isColumnMatches(isTransactionalTable, queryMeasure.getMeasure(), carbonMeasure)) {
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
          measureDataTypes[index] = carbonMeasure.getDataType();
          break;
        }
      }
      if (!measureExistsInCurrentBlock[index]) {
        defaultValues[index] = getMeasureDefaultValue(queryMeasure.getMeasure().getColumnSchema(),
            queryMeasure.getMeasure().getDefaultValue());
        measureDataTypes[index] = queryMeasure.getMeasure().getDataType();
        blockExecutionInfo.setRestructuredBlock(true);
        queryModel.setDirectVectorFill(false);
      }
      index++;
    }
    int[] measureOrdinals =
        ArrayUtils.toPrimitive(measureOrdinalList.toArray(new Integer[measureOrdinalList.size()]));
    measureInfo.setDefaultValues(defaultValues);
    measureInfo.setMeasureOrdinals(measureOrdinals);
    measureInfo.setMeasureExists(measureExistsInCurrentBlock);
    measureInfo.setMeasureDataTypes(measureDataTypes);
    blockExecutionInfo.setMeasureInfo(measureInfo);
    return presentMeasure;
  }

  /**
   * set actual projection of blockExecutionInfo
   */
  public static void actualProjectionOfSegment(BlockExecutionInfo blockExecutionInfo,
      QueryModel queryModel, SegmentProperties segmentProperties) {
    List<ProjectionDimension> projectionDimensions = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> projectionMeasures = queryModel.getProjectionMeasures();
    if (queryModel.getTable().hasColumnDrift()) {
      List<CarbonMeasure> tableBlockMeasures = segmentProperties.getMeasures();
      List<ProjectionMeasure> updatedProjectionMeasures =
          new ArrayList<>(projectionMeasures.size() + tableBlockMeasures.size());
      updatedProjectionMeasures.addAll(projectionMeasures);
      List<ProjectionDimension> updatedProjectionDimensions =
          new ArrayList<>(projectionDimensions.size());
      for (ProjectionDimension projectionDimension : projectionDimensions) {
        CarbonMeasure carbonMeasure = null;
        for (CarbonMeasure tableBlockMeasure : tableBlockMeasures) {
          if (isColumnMatches(queryModel.getTable().isTransactionalTable(),
              projectionDimension.getDimension(), tableBlockMeasure)) {
            carbonMeasure = tableBlockMeasure;
            break;
          }
        }
        if (carbonMeasure != null) {
          ProjectionMeasure projectionMeasure = new ProjectionMeasure(carbonMeasure);
          projectionMeasure.setOrdinal(projectionDimension.getOrdinal());
          updatedProjectionMeasures.add(projectionMeasure);
        } else {
          updatedProjectionDimensions.add(projectionDimension);
        }
      }
      blockExecutionInfo.setActualQueryDimensions(updatedProjectionDimensions
          .toArray(new ProjectionDimension[updatedProjectionDimensions.size()]));
      blockExecutionInfo.setActualQueryMeasures(updatedProjectionMeasures
          .toArray(new ProjectionMeasure[updatedProjectionMeasures.size()]));
    } else {
      blockExecutionInfo.setActualQueryDimensions(
          projectionDimensions.toArray(new ProjectionDimension[projectionDimensions.size()]));
      blockExecutionInfo.setActualQueryMeasures(
          projectionMeasures.toArray(new ProjectionMeasure[projectionMeasures.size()]));
    }
  }

  public static boolean hasColumnDriftOnSegment(CarbonTable table,
      SegmentProperties segmentProperties) {
    for (CarbonDimension queryColumn : table.getColumnDrift()) {
      for (CarbonMeasure tableColumn : segmentProperties.getMeasures()) {
        if (isColumnMatches(table.isTransactionalTable(), queryColumn, tableColumn)) {
          return true;
        }
      }
    }
    return false;
  }
}
