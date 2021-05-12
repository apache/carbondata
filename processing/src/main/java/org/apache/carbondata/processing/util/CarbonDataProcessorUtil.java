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

package org.apache.carbondata.processing.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.SortScopeOptions;
import org.apache.carbondata.core.metadata.DatabaseLocationProvider;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.loading.model.CarbonDataLoadSchema;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public final class CarbonDataProcessorUtil {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonDataProcessorUtil.class.getName());

  private CarbonDataProcessorUtil() {

  }

  /**
   * This method will be used to delete sort temp location is it is exites
   */
  public static void deleteSortLocationIfExists(String[] locations) {
    for (String loc : locations) {
      File file = new File(loc);
      if (file.exists()) {
        try {
          CarbonUtil.deleteFoldersAndFiles(file);
        } catch (IOException | InterruptedException e) {
          LOGGER.error("Failed to delete " + loc, e);
        }
      }
    }
  }

  /**
   * This method will be used to create dirs
   * @param locations locations to create
   */
  public static void createLocations(String[] locations) {
    for (String loc : locations) {
      File dir = new File(loc);
      if (dir.exists()) {
        LOGGER.warn("dir already exists, skip dir creation: " + loc);
      } else {
        if (!dir.mkdirs() && !dir.exists()) {
          // concurrent scenario mkdir may fail, so checking dir
          LOGGER.error("Error occurs while creating dir: " + loc);
        } else {
          LOGGER.info("Successfully created dir: " + loc);
        }
      }
    }
  }

  /**
   *
   * This method will form the local data folder store location
   *
   * @param carbonTable
   * @param taskId
   * @param segmentId
   * @param isCompactionFlow
   * @param isAltPartitionFlow
   * @return
   */
  public static String[] getLocalDataFolderLocation(CarbonTable carbonTable,
      String taskId, String segmentId, boolean isCompactionFlow, boolean isAltPartitionFlow) {
    String tempLocationKey =
        getTempStoreLocationKey(carbonTable.getDatabaseName(), carbonTable.getTableName(),
            segmentId, taskId, isCompactionFlow, isAltPartitionFlow);
    String baseTempStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey);
    if (baseTempStorePath == null) {
      LOGGER.warn("Location not set for the key " + tempLocationKey
          + ". This will occur during a global-sort loading,"
          + " in this case local dirs will be chosen by spark");
      baseTempStorePath = "./store.location";
    }

    String[] baseTmpStorePathArray = StringUtils.split(baseTempStorePath, File.pathSeparator);
    String[] localDataFolderLocArray = new String[baseTmpStorePathArray.length];

    for (int i = 0; i < baseTmpStorePathArray.length; i++) {
      String tmpStore = baseTmpStorePathArray[i];
      String carbonDataDirectoryPath = CarbonTablePath.getSegmentPath(tmpStore, segmentId);

      localDataFolderLocArray[i] = carbonDataDirectoryPath + File.separator + taskId;
    }
    return localDataFolderLocArray;
  }

  /**
   * This method will form the key for getting the temporary location set in carbon properties
   *
   * @param databaseName
   * @param tableName
   * @param segmentId
   * @param taskId
   * @param isCompactionFlow
   * @return
   */
  public static String getTempStoreLocationKey(String databaseName, String tableName,
      String segmentId, String taskId, boolean isCompactionFlow, boolean isAltPartitionFlow) {
    String tempLocationKey = DatabaseLocationProvider.get().provide(databaseName)
        + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + segmentId + CarbonCommonConstants.UNDERSCORE + taskId;
    if (isCompactionFlow) {
      tempLocationKey = CarbonCommonConstants.COMPACTION_KEY_WORD + CarbonCommonConstants.UNDERSCORE
          + tempLocationKey;
    }
    if (isAltPartitionFlow) {
      tempLocationKey = CarbonCommonConstants.ALTER_PARTITION_KEY_WORD +
          CarbonCommonConstants.UNDERSCORE + tempLocationKey;
    }
    return tempLocationKey;
  }

  /**
   * Preparing the boolean [] to map whether the dimension is no Dictionary or not.
   */
  public static boolean[] getNoDictionaryMapping(DataField[] fields) {
    List<Boolean> noDictionaryMapping = new ArrayList<Boolean>();
    for (DataField field : fields) {
      // for  complex type need to break the loop
      if (field.getColumn().isComplex()) {
        break;
      }

      if (!field.isDateDataType() && field.getColumn().isDimension()) {
        noDictionaryMapping.add(true);
      } else if (field.getColumn().isDimension()) {
        noDictionaryMapping.add(false);
      }
    }
    return ArrayUtils
        .toPrimitive(noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }

  /**
   * Preparing the boolean [] to map whether the dimension is varchar data type or not.
   */
  public static boolean[] getIsVarcharColumnMapping(DataField[] fields) {
    List<Boolean> isVarcharColumnMapping = new ArrayList<Boolean>();
    for (DataField field : fields) {
      // for complex type need to break the loop
      if (field.getColumn().isComplex()) {
        break;
      }

      if (field.getColumn().isDimension()) {
        isVarcharColumnMapping.add(
            field.getColumn().getColumnSchema().getDataType() == DataTypes.VARCHAR);
      }
    }
    return ArrayUtils.toPrimitive(
        isVarcharColumnMapping.toArray(new Boolean[isVarcharColumnMapping.size()]));
  }

  public static boolean[] getNoDictionaryMapping(CarbonColumn[] carbonColumns) {
    List<Boolean> noDictionaryMapping = new ArrayList<Boolean>();
    for (CarbonColumn column : carbonColumns) {
      // for  complex type need to break the loop
      if (column.isComplex()) {
        break;
      }
      if (column.getDataType() != DataTypes.DATE && column.isDimension()) {
        noDictionaryMapping.add(true);
      } else if (column.isDimension()) {
        noDictionaryMapping.add(false);
      }
    }
    return ArrayUtils
        .toPrimitive(noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }

  private static String getComplexTypeString(DataField[] dataFields) {
    StringBuilder dimString = new StringBuilder();
    for (DataField dataField : dataFields) {
      if (dataField.getColumn().getDataType().isComplexType()) {
        addAllComplexTypeChildren((CarbonDimension) dataField.getColumn(), dimString, "");
        dimString.append(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
      }
    }
    return dimString.toString();
  }

  private static String isDictionaryType(CarbonDimension dimension) {
    boolean isDictionary = true;
    if (dimension.getDataType() != DataTypes.DATE) {
      isDictionary = false;
    }
    return String.valueOf(isDictionary);
  }

  /**
   * This method will return all the child dimensions under complex dimension
   */
  private static void addAllComplexTypeChildren(CarbonDimension dimension, StringBuilder dimString,
      String parent) {

    dimString.append(dimension.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(dimension.getDataType()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(parent).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(isDictionaryType(dimension)).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(dimension.getColumnId()).append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      CarbonDimension childDim = dimension.getListOfChildDimensions().get(i);
      if (childDim.getNumberOfChild() > 0) {
        addAllComplexTypeChildren(childDim, dimString, dimension.getColName());
      } else {
        dimString.append(childDim.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getDataType()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(dimension.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(isDictionaryType(dimension)).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getColumnId()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getOrdinal()).append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
    }
  }

  // TODO: need to simplify it. Not required create string first.
  public static Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields,
      String nullFormat) {
    String complexTypeString = getComplexTypeString(dataFields);

    if (null == complexTypeString || complexTypeString.equals("")) {
      return new LinkedHashMap<>();
    }
    Map<String, GenericDataType> complexTypesMap = new LinkedHashMap<String, GenericDataType>();
    String[] hierarchies = complexTypeString.split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    for (int i = 0; i < hierarchies.length; i++) {
      String[] levels = hierarchies[i].split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      String[] levelInfo = levels[0].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      String level1Info = levelInfo[1].toLowerCase();
      GenericDataType g = (level1Info.contains(CarbonCommonConstants.ARRAY) || level1Info
          .contains(CarbonCommonConstants.MAP)) ?
          new ArrayDataType(levelInfo[0], "", levelInfo[3]) :
          new StructDataType(levelInfo[0], "", levelInfo[3]);
      complexTypesMap.put(levelInfo[0], g);
      for (int j = 1; j < levels.length; j++) {
        levelInfo = levels[j].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
        String levelInfo1 = levelInfo[1].toLowerCase();
        if (levelInfo1.contains(CarbonCommonConstants.ARRAY) || levelInfo1
            .contains(CarbonCommonConstants.MAP)) {
          g.addChildren(new ArrayDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
        } else if (levelInfo[1].toLowerCase().contains(CarbonCommonConstants.STRUCT)) {
          g.addChildren(new StructDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
        } else {
          g.addChildren(
              new PrimitiveDataType(levelInfo[0], DataTypeUtil.valueOf(levelInfo[1]),
                  levelInfo[2], levelInfo[4], levelInfo[3].contains("true"), nullFormat
              ));
        }
      }
    }
    return complexTypesMap;
  }

  public static boolean isHeaderValid(String tableName, String[] csvHeader,
      CarbonDataLoadSchema schema, List<String> ignoreColumns) {
    Iterator<String> columnIterator =
        CarbonDataProcessorUtil.getSchemaColumnNames(schema).iterator();
    Set<String> csvColumns = new HashSet<String>(csvHeader.length);
    Collections.addAll(csvColumns, csvHeader);

    // file header should contain all columns of carbon table.
    // So csvColumns should contain all elements of columnIterator.
    while (columnIterator.hasNext()) {
      String column = columnIterator.next().toLowerCase();
      if (!csvColumns.contains(column) && !ignoreColumns.contains(column)) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method update the column Name
   *
   * @param schema
   */
  public static Set<String> getSchemaColumnNames(CarbonDataLoadSchema schema) {
    Set<String> columnNames = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<CarbonDimension> dimensions =
        schema.getCarbonTable().getVisibleDimensions();
    Map<String, String> properties =
        schema.getCarbonTable().getTableInfo().getFactTable().getTableProperties();
    String spatialProperty = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
    spatialProperty = spatialProperty != null ? spatialProperty.trim() : null;
    for (CarbonDimension dimension : dimensions) {
      // skip the non-schema column
      if (!dimension.getColName().equalsIgnoreCase(spatialProperty)) {
        columnNames.add(dimension.getColName());
      }
    }
    List<CarbonMeasure> measures = schema.getCarbonTable().getVisibleMeasures();
    for (CarbonMeasure msr : measures) {
      columnNames.add(msr.getColName());
    }
    return columnNames;
  }

  public static DataType[] getMeasureDataType(int measureCount, CarbonTable carbonTable) {
    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = DataTypes.DOUBLE;
    }
    List<CarbonMeasure> measures = carbonTable.getVisibleMeasures();
    for (int i = 0; i < type.length; i++) {
      type[i] = measures.get(i).getDataType();
    }
    return type;
  }

  /**
   * Get the no dictionary sort data types on the table
   *
   * @param carbonTable
   * @return
   */
  public static DataType[] getNoDictSortDataTypes(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<DataType> type = new ArrayList<>();
    for (int i = 0; i < dimensions.size(); i++) {
      if (dimensions.get(i).isSortColumn() && dimensions.get(i).getDataType() != DataTypes.DATE) {
        type.add(dimensions.get(i).getDataType());
      }
    }
    return type.toArray(new DataType[type.size()]);
  }

  /**
   * Get all the no dictionary data types on the table
   */
  public static DataType[] getNoDictDataTypes(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<DataType> type = new ArrayList<>();
    for (CarbonDimension dimension : dimensions) {
      if (!dimension.hasEncoding(Encoding.DICTIONARY)
          && dimension.getDataType() != DataTypes.DATE) {
        type.add(dimension.getDataType());
      }
    }
    return type.toArray(new DataType[type.size()]);
  }

  /**
   * Get visible no dictionary sort dimensions as per data field order
   */
  public static DataType[] getNoDictDataTypesAsDataFieldOrder(DataField[] dataFields) {
    List<DataType> type = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (!dataField.getColumn().isInvisible() && dataField.getColumn().isDimension()) {
        if (!dataField.getColumn().hasEncoding(Encoding.DICTIONARY)
            && dataField.getColumn().getColumnSchema().getDataType() != DataTypes.DATE) {
          type.add(dataField.getColumn().getColumnSchema().getDataType());
        }
      }
    }
    return type.toArray(new DataType[type.size()]);
  }

  /**
   * get visible no dictionary sort dimensions as per data field order
   *
   * @param dataFields
   * @return
   */
  public static DataType[] getNoDictSortDataTypesAsDataFieldOrder(DataField[] dataFields) {
    List<DataType> type = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (!dataField.getColumn().isInvisible() && dataField.getColumn().isDimension()) {
        if (dataField.getColumn().getColumnSchema().isSortColumn()
            && dataField.getColumn().getColumnSchema().getDataType() != DataTypes.DATE) {
          type.add(dataField.getColumn().getColumnSchema().getDataType());
        }
      }
    }
    return type.toArray(new DataType[type.size()]);
  }

  /**
   * Get the no dictionary sort column mapping of the table
   *
   * @param carbonTable
   * @return
   */
  public static boolean[] getNoDictSortColMapping(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<Boolean> noDicSortColMap = new ArrayList<>();
    for (int i = 0; i < dimensions.size(); i++) {
      if (dimensions.get(i).isSortColumn()) {
        if (dimensions.get(i).getDataType() != DataTypes.DATE) {
          noDicSortColMap.add(true);
        } else {
          noDicSortColMap.add(false);
        }
      }
    }
    Boolean[] mapping = noDicSortColMap.toArray(new Boolean[0]);
    boolean[] noDicSortColMapping = new boolean[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      noDicSortColMapping[i] = mapping[i];
    }
    return noDicSortColMapping;
  }

  /**
   * get mapping based on data fields order
   *
   * @param dataFields
   * @return
   */
  public static boolean[] getNoDictSortColMappingAsDataFieldOrder(DataField[] dataFields) {
    List<Boolean> noDicSortColMap = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (!dataField.getColumn().isInvisible() && dataField.getColumn().isDimension()) {
        if (dataField.getColumn().getColumnSchema().isSortColumn()) {
          if (dataField.getColumn().getColumnSchema().getDataType() != DataTypes.DATE) {
            noDicSortColMap.add(true);
          } else {
            noDicSortColMap.add(false);
          }
        }
      }
    }
    Boolean[] mapping = noDicSortColMap.toArray(new Boolean[0]);
    boolean[] noDicSortColMapping = new boolean[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      noDicSortColMapping[i] = mapping[i];
    }
    return noDicSortColMapping;
  }

  /**
   * If the dimension is added in older version 1.1, by default it will be sort column, So during
   * initial sorting, carbonrow will be in order where added sort column is at the beginning, But
   * before final merger of sort, the data should be in schema order
   * (org.apache.carbondata.processing.sort.SchemaBasedRowUpdater updates the carbonRow in schema
   * order), so This method helps to find the index of no dictionary/ dictionary sort column in
   * the carbonrow data.
   */
  public static Map<String, int[]> getColumnIdxBasedOnSchemaInRow(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<Integer> noDicSortColMap = new ArrayList<>();
    // get no-dict / dict sort dimensions
    List<CarbonDimension> noDictSortDimensions = new ArrayList<>();
    List<CarbonDimension> dictSortDimensions = new ArrayList<>();

    List<Integer> noDictSortColIdx = new ArrayList<>();
    List<Integer> dictSortColIdx = new ArrayList<>();

    int counter = 0;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.hasEncoding(Encoding.DICTIONARY) || dimension.getDataType() == DataTypes.DATE) {
        dictSortDimensions.add(dimension);
      } else {
        noDictSortDimensions.add(dimension);
      }
      if (dimension.getDataType() == DataTypes.DATE) {
        continue;
      }
      if (dimension.isSortColumn() && DataTypeUtil.isPrimitiveColumn(dimension.getDataType())) {
        noDicSortColMap.add(counter);
      }
      counter++;
    }
    // add no-Dict sort column index
    for (int i = 0; i < noDictSortDimensions.size(); i++) {
      if (noDictSortDimensions.get(i).isSortColumn()) {
        noDictSortColIdx.add(i);
      }
    }
    // add dict sort column index
    for (int i = 0; i < dictSortDimensions.size(); i++) {
      if (dictSortDimensions.get(i).isSortColumn()) {
        dictSortColIdx.add(i);
      }
    }
    Integer[] mapping = noDicSortColMap.toArray(new Integer[0]);
    int[] columnIdxBasedOnSchemaInRow = new int[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      columnIdxBasedOnSchemaInRow[i] = mapping[i];
    }
    Integer[] noDictSortIdx = noDictSortColIdx.toArray(new Integer[0]);
    int[] noDictSortIdxBasedOnSchemaInRow = new int[noDictSortIdx.length];
    for (int i = 0; i < noDictSortIdx.length; i++) {
      noDictSortIdxBasedOnSchemaInRow[i] = noDictSortIdx[i];
    }
    Integer[] dictSortIdx = dictSortColIdx.toArray(new Integer[0]);
    int[] dictSortIdxBasedOnSchemaInRow = new int[dictSortIdx.length];
    for (int i = 0; i < dictSortIdx.length; i++) {
      dictSortIdxBasedOnSchemaInRow[i] = dictSortIdx[i];
    }

    Map<String, int[]> dictOrNoDictSortInfoMap = new HashMap<>();
    dictOrNoDictSortInfoMap.put("columnIdxBasedOnSchemaInRow", columnIdxBasedOnSchemaInRow);
    dictOrNoDictSortInfoMap.put("noDictSortIdxBasedOnSchemaInRow", noDictSortIdxBasedOnSchemaInRow);
    dictOrNoDictSortInfoMap.put("dictSortIdxBasedOnSchemaInRow", dictSortIdxBasedOnSchemaInRow);
    return dictOrNoDictSortInfoMap;
  }

  /**
   * If the dimension is added in older version 1.1, by default it will be sort column, So during
   * initial sorting, carbonrow will be in order where added sort column is at the beginning, But
   * before final merger of sort, the data should be in schema order
   * (org.apache.carbondata.processing.sort.SchemaBasedRowUpdater updates the carbonRow in schema
   * order), so This method helps to find the index of no dictionary/ dictionary sort column in
   * the carbonrow data.
   */
  public static Map<String, int[]> getColumnIdxBasedOnSchemaInRowAsDataFieldOrder(
      DataField[] dataFields) {
    List<Integer> noDicSortColMap = new ArrayList<>();
    int counter = 0;
    // get no-dict / dict sort column schema
    List<CarbonColumn> noDictSortColumns = new ArrayList<>();
    List<CarbonColumn> dictSortColumns = new ArrayList<>();

    List<Integer> noDictSortColIdx = new ArrayList<>();
    List<Integer> dictSortColIdx = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (!dataField.getColumn().isInvisible() && dataField.getColumn().isDimension()) {
        if (dataField.getColumn().getColumnSchema().hasEncoding(Encoding.DICTIONARY)
            || dataField.getColumn().getColumnSchema().getDataType() == DataTypes.DATE) {
          dictSortColumns.add(dataField.getColumn());
        } else {
          noDictSortColumns.add(dataField.getColumn());
        }
        if (dataField.getColumn().getColumnSchema().getDataType() == DataTypes.DATE) {
          continue;
        }
        if (dataField.getColumn().getColumnSchema().isSortColumn() && DataTypeUtil
            .isPrimitiveColumn(dataField.getColumn().getColumnSchema().getDataType())) {
          noDicSortColMap.add(counter);
        }
        counter++;
      }
    }
    // add no-Dict sort column index
    for (int i = 0; i < noDictSortColumns.size(); i++) {
      if (noDictSortColumns.get(i).getColumnSchema().isSortColumn()) {
        noDictSortColIdx.add(i);
      }
    }
    // add dict sort column index
    for (int i = 0; i < dictSortColumns.size(); i++) {
      if (dictSortColumns.get(i).getColumnSchema().isSortColumn()) {
        dictSortColIdx.add(i);
      }
    }
    Integer[] mapping = noDicSortColMap.toArray(new Integer[0]);
    int[] columnIdxBasedOnSchemaInRow = new int[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      columnIdxBasedOnSchemaInRow[i] = mapping[i];
    }
    Integer[] noDictSortIdx = noDictSortColIdx.toArray(new Integer[0]);
    int[] noDictSortIdxBasedOnSchemaInRow = new int[noDictSortIdx.length];
    for (int i = 0; i < noDictSortIdx.length; i++) {
      noDictSortIdxBasedOnSchemaInRow[i] = noDictSortIdx[i];
    }
    Integer[] dictSortIdx = dictSortColIdx.toArray(new Integer[0]);
    int[] dictSortIdxBasedOnSchemaInRow = new int[dictSortIdx.length];
    for (int i = 0; i < dictSortIdx.length; i++) {
      dictSortIdxBasedOnSchemaInRow[i] = dictSortIdx[i];
    }

    Map<String, int[]> dictOrNoSortInfoMap = new HashMap<>();
    dictOrNoSortInfoMap.put("columnIdxBasedOnSchemaInRow", columnIdxBasedOnSchemaInRow);
    dictOrNoSortInfoMap.put("noDictSortIdxBasedOnSchemaInRow", noDictSortIdxBasedOnSchemaInRow);
    dictOrNoSortInfoMap.put("dictSortIdxBasedOnSchemaInRow", dictSortIdxBasedOnSchemaInRow);
    return dictOrNoSortInfoMap;
  }

  /**
   * Get the data types of the no dictionary sort columns
   *
   * @param carbonTable
   * @return
   */
  public static Map<String, DataType[]> getNoDictSortAndNoSortDataTypes(CarbonTable carbonTable) {
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<DataType> noDictSortType = new ArrayList<>();
    List<DataType> noDictNoSortType = new ArrayList<>();
    for (int i = 0; i < dimensions.size(); i++) {
      if (dimensions.get(i).getDataType() != DataTypes.DATE) {
        if (dimensions.get(i).isSortColumn()) {
          noDictSortType.add(dimensions.get(i).getDataType());
        } else {
          noDictNoSortType.add(dimensions.get(i).getDataType());
        }
      }
    }
    DataType[] noDictSortTypes = noDictSortType.toArray(new DataType[noDictSortType.size()]);
    DataType[] noDictNoSortTypes = noDictNoSortType.toArray(new DataType[noDictNoSortType.size()]);
    Map<String, DataType[]> noDictSortAndNoSortTypes = new HashMap<>(2);
    noDictSortAndNoSortTypes.put("noDictSortDataTypes", noDictSortTypes);
    noDictSortAndNoSortTypes.put("noDictNoSortDataTypes", noDictNoSortTypes);
    return noDictSortAndNoSortTypes;
  }

  /**
   * Get the data types of the no dictionary sort columns as per dataFields order
   *
   * @param dataFields
   * @return
   */
  public static Map<String, DataType[]> getNoDictSortAndNoSortDataTypes(
      DataField[] dataFields) {
    List<DataType> noDictSortType = new ArrayList<>();
    List<DataType> noDictNoSortType = new ArrayList<>();
    for (DataField dataField : dataFields) {
      if (dataField.getColumn().isDimension()
          && dataField.getColumn().getColumnSchema().getDataType() != DataTypes.DATE) {
        if (dataField.getColumn().getColumnSchema().isSortColumn()) {
          noDictSortType.add(dataField.getColumn().getColumnSchema().getDataType());
        } else {
          noDictNoSortType.add(dataField.getColumn().getColumnSchema().getDataType());
        }
      }
    }
    DataType[] noDictSortTypes = noDictSortType.toArray(new DataType[noDictSortType.size()]);
    DataType[] noDictNoSortTypes = noDictNoSortType.toArray(new DataType[noDictNoSortType.size()]);
    Map<String, DataType[]> noDictSortAndNoSortTypes = new HashMap<>(2);
    noDictSortAndNoSortTypes.put("noDictSortDataTypes", noDictSortTypes);
    noDictSortAndNoSortTypes.put("noDictNoSortDataTypes", noDictNoSortTypes);
    return noDictSortAndNoSortTypes;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  public static String createCarbonStoreLocation(CarbonTable carbonTable, String segmentId) {
    return CarbonTablePath.getSegmentPath(carbonTable.getTablePath(), segmentId);
  }

  /**
   * This method will get the store location for the given path, partition spec
   *
   * @return data directory path
   */
  public static String createCarbonStoreLocationForPartition(CarbonTable carbonTable,
      String partition) {
    return carbonTable.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + partition;
  }

  /**
   * initialise data type for measures for their storage format
   */
  public static DataType[] initDataType(CarbonTable carbonTable, String tableName,
      int measureCount) {
    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = DataTypes.DOUBLE;
    }
    List<CarbonMeasure> measures = carbonTable.getVisibleMeasures();
    for (int i = 0; i < measureCount; i++) {
      type[i] = measures.get(i).getDataType();
    }
    return type;
  }

  /**
   * Check whether batch sort is enabled or not.
   * @param configuration
   * @return
   */
  public static SortScopeOptions.SortScope getSortScope(CarbonDataLoadConfiguration configuration) {
    SortScopeOptions.SortScope sortScope;
    try {
      // first check whether user input it from ddl, otherwise get from carbon properties
      if (configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_SORT_SCOPE) == null) {
        sortScope = SortScopeOptions.getSortScope(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT));
      } else {
        sortScope = SortScopeOptions.getSortScope(
            configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_SORT_SCOPE)
                .toString());
      }
    } catch (Exception e) {
      sortScope = SortScopeOptions.getSortScope(CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT);
      LOGGER.warn("Exception occured while resolving sort scope. " +
          "sort scope is set to " + sortScope);
    }
    return sortScope;
  }

  public static SortScopeOptions.SortScope getSortScope(String sortScopeString) {
    SortScopeOptions.SortScope sortScope;
    try {
      // first check whether user input it from ddl, otherwise get from carbon properties
      if (sortScopeString == null) {
        sortScope = SortScopeOptions.getSortScope(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT));
      } else {
        sortScope = SortScopeOptions.getSortScope(sortScopeString);
      }
      LOGGER.info("sort scope is set to " + sortScope);
    } catch (Exception e) {
      sortScope = SortScopeOptions.getSortScope(CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT);
      LOGGER.warn("Exception occured while resolving sort scope. " +
          "sort scope is set to " + sortScope);
    }
    return sortScope;
  }

  /**
   * Get the number of partitions in global sort
   * @param globalSortPartitions
   * @return the number of partitions
   */
  public static int getGlobalSortPartitions(Object globalSortPartitions) {
    int numPartitions;
    try {
      // First try to get the number from ddl, otherwise get it from carbon properties.
      if (globalSortPartitions == null) {
        numPartitions = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS,
            CarbonCommonConstants.LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT));
      } else {
        numPartitions = Integer.parseInt(globalSortPartitions.toString());
      }
    } catch (Exception e) {
      numPartitions = 0;
    }
    return numPartitions;
  }

  /**
   * the method prepares and return the message mentioning the reason of badrecord
   *
   * @param columnName
   * @param dataType
   * @return
   */
  public static String prepareFailureReason(String columnName, DataType dataType) {
    return "The value with column name " + columnName + " and column data type " + dataType
        .getName() + " is not a valid " + dataType + " type.";
  }

  /**
   * This method will return an array whose element with be appended with the `append` strings
   * @param inputArr  inputArr
   * @param append strings to append
   * @return result
   */
  public static String[] arrayAppend(String[] inputArr, String... append) {
    String[] outArr = new String[inputArr.length];
    StringBuffer sb = new StringBuffer();
    for (String str : append) {
      sb.append(str);
    }
    String appendStr = sb.toString();
    for (int i = 0; i < inputArr.length; i++) {
      outArr[i] = inputArr[i] + appendStr;
    }
    return outArr;
  }

  /**
   * This method returns String if exception is TextParsingException
   *
   * @param input
   * @return
   */
  public static String trimErrorMessage(String input) {
    String errorMessage = input;
    if (input != null) {
      if (input.split("Hint").length > 1) {
        errorMessage = input.split("Hint")[0];
      } else if (input.split("Parser Configuration:").length > 1) {
        errorMessage = input.split("Parser Configuration:")[0];
      }
    }
    return errorMessage;
  }

  /**
   * The method returns true is either logger is enabled or action is redirect
   * @param configuration
   * @return
   */
  public static boolean isRawDataRequired(CarbonDataLoadConfiguration configuration) {
    boolean isRawDataRequired = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE)
            .toString());
    // if logger is disabled then check if action is redirect then raw data will be required.
    if (!isRawDataRequired) {
      Object bad_records_action =
          configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION);
      if (null != bad_records_action) {
        LoggerAction loggerAction = null;
        try {
          loggerAction = LoggerAction.valueOf(bad_records_action.toString().toUpperCase());
        } catch (IllegalArgumentException e) {
          loggerAction = LoggerAction.FORCE;
        }
        isRawDataRequired = loggerAction == LoggerAction.REDIRECT;
      }
    }
    return isRawDataRequired;
  }

  /**
   * Partition input iterators equally as per the number of threads.
   *
   * @return
   */
  public static List<CarbonIterator<Object[]>>[] partitionInputReaderIterators(
      CarbonIterator<Object[]>[] inputIterators, short sdkWriterCores) {
    // Get the number of cores configured in property.
    int numberOfCores;
    if (sdkWriterCores > 0) {
      numberOfCores = sdkWriterCores;
    } else {
      numberOfCores = CarbonProperties.getInstance().getNumberOfLoadingCores();
    }
    // Get the minimum of number of cores and iterators size to get the number of parallel threads
    // to be launched.
    int parallelThreadNumber = Math.min(inputIterators.length, numberOfCores);

    if (parallelThreadNumber <= 0) {
      parallelThreadNumber = 1;
    }

    List<CarbonIterator<Object[]>>[] iterators = new List[parallelThreadNumber];
    for (int i = 0; i < parallelThreadNumber; i++) {
      iterators[i] = new ArrayList<>();
    }
    // Equally partition the iterators as per number of threads
    for (int i = 0; i < inputIterators.length; i++) {
      iterators[i % parallelThreadNumber].add(inputIterators[i]);
    }
    return iterators;
  }

}
