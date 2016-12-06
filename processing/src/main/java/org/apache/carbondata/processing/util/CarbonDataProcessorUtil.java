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

package org.apache.carbondata.processing.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonDataLoadSchema;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
import org.apache.carbondata.processing.etl.DataLoadingException;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

import org.apache.commons.lang3.ArrayUtils;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepMeta;

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
   * Utility method to get level cardinality string
   *
   * @param dimCardinalities
   * @param aggDims
   * @return level cardinality string
   */
  public static String getLevelCardinalitiesString(Map<String, String> dimCardinalities,
      String[] aggDims) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < aggDims.length; i++) {
      String string = dimCardinalities.get(aggDims[i]);
      if (string != null) {
        sb.append(string);
        sb.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
      }
    }
    String resultStr = sb.toString();
    if (resultStr.endsWith(CarbonCommonConstants.COMA_SPC_CHARACTER)) {
      resultStr = resultStr
          .substring(0, resultStr.length() - CarbonCommonConstants.COMA_SPC_CHARACTER.length());
    }
    return resultStr;
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
    // CHECKSTYLE:OFF
    for (CarbonFile badFiles : listFiles) {
      // CHECKSTYLE:ON
      badRecordsInProgressFileName = badFiles.getName();

      changedFileName = badLogStoreLocation + File.separator + badRecordsInProgressFileName
          .substring(0, badRecordsInProgressFileName.lastIndexOf('.'));

      badFiles.renameTo(changedFileName);

      if (badFiles.exists()) {
        if (!badFiles.delete()) {
          LOGGER.error("Unable to delete File : " + badFiles.getName());
        }
      }
    }// CHECKSTYLE:ON
  }

  public static void checkResult(List<CheckResultInterface> remarks, StepMeta stepMeta,
      String[] input) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.",
          stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!",
          stepMeta);
      remarks.add(cr);
    }
  }

  public static void check(Class<?> pkg, List<CheckResultInterface> remarks, StepMeta stepMeta,
      RowMetaInterface prev, String[] input) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
          BaseMessages.getString(pkg, "CarbonStep.Check.StepIsReceivingInfoFromOtherSteps"),
          stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(pkg, "CarbonStep.Check.NoInputReceivedFromOtherSteps"), stepMeta);
      remarks.add(cr);
    }

    // also check that each expected key fields are acually coming
    if (prev != null && prev.size() > 0) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
          BaseMessages.getString(pkg, "CarbonStep.Check.AllFieldsFoundInInput"), stepMeta);
      remarks.add(cr);
    } else {
      String errorMessage =
          BaseMessages.getString(pkg, "CarbonStep.Check.CouldNotReadFromPreviousSteps") + Const.CR;
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, errorMessage, stepMeta);
      remarks.add(cr);
    }
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public static void deleteSortLocationIfExists(String tempFileLocation)
      throws CarbonSortKeyAndGroupByException {
    // create new temp file location where this class
    //will write all the temp files
    File file = new File(tempFileLocation);

    if (file.exists()) {
      try {
        CarbonUtil.deleteFoldersAndFiles(file);
      } catch (CarbonUtilException e) {
        LOGGER.error(e);
      }
    }
  }

  /**
   * return the modification TimeStamp Separated by HASH_SPC_CHARACTER
   */
  public static String getLoadNameFromLoadMetaDataDetails(
      List<LoadMetadataDetails> loadMetadataDetails) {
    StringBuilder builder = new StringBuilder();
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      builder.append(CarbonCommonConstants.LOAD_FOLDER).append(loadMetadataDetail.getLoadName())
          .append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    String loadNames =
        builder.substring(0, builder.lastIndexOf(CarbonCommonConstants.HASH_SPC_CHARACTER));
    return loadNames;
  }

  /**
   * return the modOrDelTimesStamp TimeStamp Separated by HASH_SPC_CHARACTER
   */
  public static String getModificationOrDeletionTimesFromLoadMetadataDetails(
      List<LoadMetadataDetails> loadMetadataDetails) {
    StringBuilder builder = new StringBuilder();
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      builder.append(loadMetadataDetail.getModificationOrdeletionTimesStamp())
          .append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    String modOrDelTimesStamp =
        builder.substring(0, builder.indexOf(CarbonCommonConstants.HASH_SPC_CHARACTER));
    return modOrDelTimesStamp;
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
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + taskId;
    if (isCompactionFlow) {
      tempLocationKey = CarbonCommonConstants.COMPACTION_KEY_WORD + '_' + tempLocationKey;
    }

    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(baseStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId + "");
    String localDataLoadFolderLocation = carbonDataDirectoryPath + File.separator + taskId;
    return localDataLoadFolderLocation;
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

      if (!field.hasDictionaryEncoding() && field.getColumn().isDimesion()) {
        noDictionaryMapping.add(true);
      } else if (field.getColumn().isDimesion()) {
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
      if (field.getColumn().isUseInvertedIndex() && field.getColumn().isDimesion()) {
        isUseInvertedIndexList.add(true);
      } else if(field.getColumn().isDimesion()){
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

  /**
   * Get the csv file to read if it the path is file otherwise get the first file of directory.
   *
   * @param csvFilePath
   * @return File
   */
  public static CarbonFile getCsvFileToRead(String csvFilePath) {
    CarbonFile csvFile =
        FileFactory.getCarbonFile(csvFilePath, FileFactory.getFileType(csvFilePath));

    CarbonFile[] listFiles = null;
    if (csvFile.isDirectory()) {
      listFiles = csvFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile pathname) {
          if (!pathname.isDirectory()) {
            if (pathname.getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION) || pathname
                .getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION
                    + CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
              return true;
            }
          }
          return false;
        }
      });
    } else {
      listFiles = new CarbonFile[1];
      listFiles[0] = csvFile;
    }
    return listFiles[0];
  }

  /**
   * Get the file header from csv file.
   */
  public static String getFileHeader(CarbonFile csvFile)
      throws DataLoadingException {
    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    FileType fileType = FileFactory.getFileType(csvFile.getAbsolutePath());

    if (!csvFile.exists()) {
      csvFile = FileFactory
          .getCarbonFile(csvFile.getAbsolutePath() + CarbonCommonConstants.FILE_INPROGRESS_STATUS,
              fileType);
    }

    try {
      fileReader = FileFactory.getDataInputStream(csvFile.getAbsolutePath(), fileType);
      bufferedReader =
          new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
      readLine = bufferedReader.readLine();
    } catch (FileNotFoundException e) {
      LOGGER.error(e, "CSV Input File not found  " + e.getMessage());
      throw new DataLoadingException("CSV Input File not found ", e);
    } catch (IOException e) {
      LOGGER.error(e, "Not able to read CSV input File  " + e.getMessage());
      throw new DataLoadingException("Not able to read CSV input File ", e);
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }

    return readLine;
  }

  public static boolean isHeaderValid(String tableName, String header,
      CarbonDataLoadSchema schema, String delimiter) throws DataLoadingException {
    delimiter = CarbonUtil.delimiterConverter(delimiter);
    String[] columnNames =
        CarbonDataProcessorUtil.getSchemaColumnNames(schema, tableName).toArray(new String[0]);
    String[] csvHeader = header.toLowerCase().split(delimiter);

    List<String> csvColumnsList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (String column : csvHeader) {
      csvColumnsList.add(column.replaceAll("\"", "").trim());
    }

    int count = 0;

    for (String columns : columnNames) {
      if (csvColumnsList.contains(columns.toLowerCase())) {
        count++;
      }
    }
    return count == columnNames.length;
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

        String foreignKey = null;
        for (CarbonDataLoadSchema.DimensionRelation dimRel : schema.getDimensionRelationList()) {
          for (String field : dimRel.getColumns()) {
            if (dimension.getColName().equals(field)) {
              foreignKey = dimRel.getRelation().getFactForeignKeyColumn();
              break;
            }
          }
          if (null != foreignKey) {
            break;
          }
        }
        if (null == foreignKey) {
          columnNames.add(dimension.getColName());
        } else {
          columnNames.add(foreignKey);
        }
      }

      List<CarbonMeasure> measures = schema.getCarbonTable().getMeasureByTableName(factTableName);
      for (CarbonMeasure msr : measures) {
        if (!msr.getColumnSchema().isInvisible()) {
          columnNames.add(msr.getColName());
        }
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

  /**
   * Splits header to fields using delimiter.
   * @param header
   * @param delimiter
   * @return
   */
  public static String[] getColumnFields(String header, String delimiter) {
    delimiter = CarbonUtil.delimiterConverter(delimiter);
    String[] columnNames = header.split(delimiter);
    String tmpCol;
    for (int i = 0; i < columnNames.length; i++) {
      tmpCol = columnNames[i].replaceAll("\"", "");
      columnNames[i] = tmpCol.trim();
    }

    return columnNames;
  }

  /**
   * get agg type
   */
  public static char[] getAggType(int measureCount, String databaseName, String tableName) {
    char[] aggType = new char[measureCount];
    Arrays.fill(aggType, 'n');
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(tableName);
    for (int i = 0; i < aggType.length; i++) {
      aggType[i] = DataTypeUtil.getAggType(measures.get(i).getDataType());
    }
    return aggType;
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
   * Maybe we can extract interfaces later to support task context in hive ,spark
   */
  public static Object fetchTaskContext() {
    try {
      return Class.forName("org.apache.spark.TaskContext").getMethod("get").invoke(null);
    } catch (Exception e) {
      //just ignore
      LOGGER.info("org.apache.spark.TaskContext not found");
      return null;
    }
  }

  public static void configureTaskContext(Object context) {
    try {
      Class clazz = Class.forName("org.apache.spark.TaskContext$");
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.getName().equals("setTaskContext")) {
          Field field = clazz.getField("MODULE$");
          Object instance = field.get(null);
          method.invoke(instance, new Object[]{context});
        }
      }
    } catch (Exception e) {
      //just ignore
      LOGGER.info("org.apache.spark.TaskContext not found");
    }
  }
}