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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.model.CarbonDataLoadSchema;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.sort.SortScopeOptions;

import org.apache.commons.lang3.ArrayUtils;

public final class CarbonDataProcessorUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataProcessorUtil.class.getName());

  private CarbonDataProcessorUtil() {

  }

  /**
   * Below method will be used to get the buffer size
   *
   * @param numberOfFiles
   * @return buffer size
   */
  public static int getFileBufferSize(int numberOfFiles, CarbonProperties instance,
      int deafultvalue) {
    int configuredBufferSize = 0;
    try {
      configuredBufferSize =
          Integer.parseInt(instance.getProperty(CarbonCommonConstants.SORT_FILE_BUFFER_SIZE));
    } catch (NumberFormatException e) {
      configuredBufferSize = deafultvalue;
    }
    int fileBufferSize = (configuredBufferSize * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) / numberOfFiles;
    if (fileBufferSize < CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) {
      fileBufferSize = CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    }
    return fileBufferSize;
  }

  /**
   * @param storeLocation
   */
  public static void renameBadRecordsFromInProgressToNormal(String storeLocation) {
    // get the base store location
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    FileType fileType = FileFactory.getFileType(badLogStoreLocation);
    try {
      if (!FileFactory.isFileExist(badLogStoreLocation, fileType)) {
        return;
      }
    } catch (IOException e1) {
      LOGGER.info("bad record folder does not exist");
    }
    CarbonFile carbonFile = FileFactory.getCarbonFile(badLogStoreLocation, fileType);

    CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile pathname) {
        if (pathname.getName().indexOf(CarbonCommonConstants.FILE_INPROGRESS_STATUS) > -1) {
          return true;
        }
        return false;
      }
    });

    String badRecordsInProgressFileName = null;
    String changedFileName = null;
    for (CarbonFile badFiles : listFiles) {
      badRecordsInProgressFileName = badFiles.getName();

      changedFileName = badLogStoreLocation + File.separator + badRecordsInProgressFileName
          .substring(0, badRecordsInProgressFileName.lastIndexOf('.'));

      badFiles.renameTo(changedFileName);

      if (badFiles.exists()) {
        if (!badFiles.delete()) {
          LOGGER.error("Unable to delete File : " + badFiles.getName());
        }
      }
    }
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   */
  public static void deleteSortLocationIfExists(String tempFileLocation) {
    // create new temp file location where this class
    //will write all the temp files
    File file = new File(tempFileLocation);

    if (file.exists()) {
      try {
        CarbonUtil.deleteFoldersAndFiles(file);
      } catch (IOException | InterruptedException e) {
        LOGGER.error(e);
      }
    }
  }

  /**
   * This method will form the local data folder store location
   *
   * @param databaseName
   * @param tableName
   * @param taskId
   * @param partitionId
   * @param segmentId
   * @return
   */
  public static String getLocalDataFolderLocation(String databaseName, String tableName,
      String taskId, String partitionId, String segmentId, boolean isCompactionFlow) {
    String tempLocationKey =
        getTempStoreLocationKey(databaseName, tableName, taskId, isCompactionFlow);
    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(baseStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId + "");
    return carbonDataDirectoryPath + File.separator + taskId;
  }

  /**
   * This method will form the key for getting the temporary location set in carbon properties
   *
   * @param databaseName
   * @param tableName
   * @param taskId
   * @param isCompactionFlow
   * @return
   */
  public static String getTempStoreLocationKey(String databaseName, String tableName, String taskId,
      boolean isCompactionFlow) {
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + taskId;
    if (isCompactionFlow) {
      tempLocationKey = CarbonCommonConstants.COMPACTION_KEY_WORD + CarbonCommonConstants.UNDERSCORE
          + tempLocationKey;
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

      if (!field.hasDictionaryEncoding() && field.getColumn().isDimension()) {
        noDictionaryMapping.add(true);
      } else if (field.getColumn().isDimension()) {
        noDictionaryMapping.add(false);
      }
    }
    return ArrayUtils
        .toPrimitive(noDictionaryMapping.toArray(new Boolean[noDictionaryMapping.size()]));
  }

  /**
   * Preparing the boolean [] to map whether the dimension use inverted index or not.
   */
  public static boolean[] getIsUseInvertedIndex(DataField[] fields) {
    List<Boolean> isUseInvertedIndexList = new ArrayList<Boolean>();
    for (DataField field : fields) {
      if (field.getColumn().isUseInvertedIndex() && field.getColumn().isDimension()) {
        isUseInvertedIndexList.add(true);
      } else if (field.getColumn().isDimension()) {
        isUseInvertedIndexList.add(false);
      }
    }
    return ArrayUtils
        .toPrimitive(isUseInvertedIndexList.toArray(new Boolean[isUseInvertedIndexList.size()]));
  }

  private static String getComplexTypeString(DataField[] dataFields) {
    StringBuilder dimString = new StringBuilder();
    for (int i = 0; i < dataFields.length; i++) {
      DataField dataField = dataFields[i];
      if (dataField.getColumn().getDataType().equals(DataType.ARRAY) || dataField.getColumn()
          .getDataType().equals(DataType.STRUCT)) {
        addAllComplexTypeChildren((CarbonDimension) dataField.getColumn(), dimString, "");
        dimString.append(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
      }
    }
    return dimString.toString();
  }

  /**
   * This method will return all the child dimensions under complex dimension
   */
  private static void addAllComplexTypeChildren(CarbonDimension dimension, StringBuilder dimString,
      String parent) {
    dimString.append(dimension.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(dimension.getDataType()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(parent).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
        .append(dimension.getColumnId()).append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      CarbonDimension childDim = dimension.getListOfChildDimensions().get(i);
      if (childDim.getNumberOfChild() > 0) {
        addAllComplexTypeChildren(childDim, dimString, dimension.getColName());
      } else {
        dimString.append(childDim.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getDataType()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(dimension.getColName()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getColumnId()).append(CarbonCommonConstants.COLON_SPC_CHARACTER)
            .append(childDim.getOrdinal()).append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
    }
  }

  // TODO: need to simplify it. Not required create string first.
  public static Map<String, GenericDataType> getComplexTypesMap(DataField[] dataFields) {
    String complexTypeString = getComplexTypeString(dataFields);
    if (null == complexTypeString || complexTypeString.equals("")) {
      return new LinkedHashMap<>();
    }
    Map<String, GenericDataType> complexTypesMap = new LinkedHashMap<String, GenericDataType>();
    String[] hierarchies = complexTypeString.split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    for (int i = 0; i < hierarchies.length; i++) {
      String[] levels = hierarchies[i].split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      String[] levelInfo = levels[0].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      GenericDataType g = levelInfo[1].equals(CarbonCommonConstants.ARRAY) ?
          new ArrayDataType(levelInfo[0], "", levelInfo[3]) :
          new StructDataType(levelInfo[0], "", levelInfo[3]);
      complexTypesMap.put(levelInfo[0], g);
      for (int j = 1; j < levels.length; j++) {
        levelInfo = levels[j].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
        switch (levelInfo[1]) {
          case CarbonCommonConstants.ARRAY:
            g.addChildren(new ArrayDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          case CarbonCommonConstants.STRUCT:
            g.addChildren(new StructDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          default:
            g.addChildren(new PrimitiveDataType(levelInfo[0], levelInfo[2], levelInfo[3],
                Integer.parseInt(levelInfo[4])));
        }
      }
    }
    return complexTypesMap;
  }

  public static boolean isHeaderValid(String tableName, String[] csvHeader,
      CarbonDataLoadSchema schema) {
    Iterator<String> columnIterator =
        CarbonDataProcessorUtil.getSchemaColumnNames(schema, tableName).iterator();
    Set<String> csvColumns = new HashSet<String>(csvHeader.length);
    Collections.addAll(csvColumns, csvHeader);

    // file header should contain all columns of carbon table.
    // So csvColumns should contain all elements of columnIterator.
    while (columnIterator.hasNext()) {
      if (!csvColumns.contains(columnIterator.next().toLowerCase())) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method update the column Name
   *
   * @param schema
   * @param tableName
   */
  public static Set<String> getSchemaColumnNames(CarbonDataLoadSchema schema, String tableName) {
    Set<String> columnNames = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    String factTableName = schema.getCarbonTable().getFactTableName();
    if (tableName.equals(factTableName)) {
      List<CarbonDimension> dimensions =
          schema.getCarbonTable().getDimensionByTableName(factTableName);
      for (CarbonDimension dimension : dimensions) {
        columnNames.add(dimension.getColName());
      }
      List<CarbonMeasure> measures = schema.getCarbonTable().getMeasureByTableName(factTableName);
      for (CarbonMeasure msr : measures) {
        columnNames.add(msr.getColName());
      }
    } else {
      List<CarbonDimension> dimensions = schema.getCarbonTable().getDimensionByTableName(tableName);
      for (CarbonDimension dimension : dimensions) {
        columnNames.add(dimension.getColName());
      }
      List<CarbonMeasure> measures = schema.getCarbonTable().getMeasureByTableName(tableName);
      for (CarbonMeasure msr : measures) {
        columnNames.add(msr.getColName());
      }
    }
    return columnNames;
  }

  public static DataType[] getMeasureDataType(int measureCount, String databaseName,
      String tableName) {
    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = DataType.DOUBLE;
    }
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(tableName);
    for (int i = 0; i < type.length; i++) {
      type[i] = measures.get(i).getDataType();
    }
    return type;
  }

  public static DataType[] getMeasureDataType(int measureCount, DataField[] measureFields) {
    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = measureFields[i].getColumn().getDataType();
    }
    return type;
  }

  /**
   * Creates map for columns which dateformats mentioned while loading the data.
   * @param dataFormatString
   * @return
   */
  public static Map<String, String> getDateFormatMap(String dataFormatString) {
    Map<String, String> dateformatsHashMap = new HashMap<>();
    if (dataFormatString != null && !dataFormatString.isEmpty()) {
      String[] dateformats = dataFormatString.split(CarbonCommonConstants.COMMA);
      for (String dateFormat : dateformats) {
        String[] dateFormatSplits = dateFormat.split(":", 2);
        dateformatsHashMap
            .put(dateFormatSplits[0].toLowerCase().trim(), dateFormatSplits[1].trim());
      }
    }
    return dateformatsHashMap;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  public static String checkAndCreateCarbonStoreLocation(String factStoreLocation,
      String databaseName, String tableName, String partitionId, String segmentId) {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(factStoreLocation, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId);
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
    return carbonDataDirectoryPath;
  }

  /**
   * initialise data type for measures for their storage format
   */
  public static DataType[] initDataType(CarbonTable carbonTable, String tableName,
      int measureCount) {
    DataType[] type = new DataType[measureCount];
    for (int i = 0; i < type.length; i++) {
      type[i] = DataType.DOUBLE;
    }
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(tableName);
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
      LOGGER.warn("sort scope is set to " + sortScope);
    }
    return sortScope;
  }

  /**
   * Get the batch sort size
   * @param configuration
   * @return
   */
  public static int getBatchSortSizeinMb(CarbonDataLoadConfiguration configuration) {
    int batchSortSizeInMb;
    try {
      // First try get from user input from ddl , otherwise get from carbon properties.
      if (configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB)
          == null) {
        batchSortSizeInMb = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB, "0"));
      } else {
        batchSortSizeInMb = Integer.parseInt(
            configuration.getDataLoadProperty(CarbonCommonConstants.LOAD_BATCH_SORT_SIZE_INMB)
                .toString());
      }
    } catch (Exception e) {
      batchSortSizeInMb = 0;
    }
    return batchSortSizeInMb;
  }

}