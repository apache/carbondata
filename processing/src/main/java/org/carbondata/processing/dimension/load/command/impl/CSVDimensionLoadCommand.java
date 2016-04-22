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

package org.carbondata.processing.dimension.load.command.impl;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.dimension.load.command.DimensionLoadCommand;
import org.carbondata.processing.dimension.load.info.DimensionLoadInfo;
import org.carbondata.processing.schema.metadata.HierarchiesInfo;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedDimSurrogateKeyGen;
import org.carbondata.processing.surrogatekeysgenerator.csvbased.CarbonCSVBasedSeqGenMeta;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

import org.pentaho.di.core.exception.KettleException;

public class CSVDimensionLoadCommand implements DimensionLoadCommand {
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CSVDimensionLoadCommand.class.getName());

  /**
   * Dimension Load Info
   */
  private DimensionLoadInfo dimensionLoadInfo;

  private int currentRestructNumber;

  public CSVDimensionLoadCommand(DimensionLoadInfo loadInfo, int currentRestructNum) {
    this.dimensionLoadInfo = loadInfo;
    this.currentRestructNumber = currentRestructNum;
  }

  /**
   * @throws KettleException
   * @see DimensionLoadCommand#execute()
   */
  @Override public void execute() throws KettleException {
    loadData(dimensionLoadInfo);
  }

  /**
   * @param dimensionLoadInfo
   * @throws KettleException
   */
  private void loadData(DimensionLoadInfo dimensionLoadInfo) throws KettleException {
    List<HierarchiesInfo> metahierVoList = dimensionLoadInfo.getHierVOlist();

    try {
      String dimFileMapping = dimensionLoadInfo.getDimFileLocDir();
      Map<String, String> fileMaps = new HashMap<String, String>();

      if (null != dimFileMapping && dimFileMapping.length() > 0) {
        String[] fileMapsArray = dimFileMapping.split(",");

        for (String entry : fileMapsArray) {
          String tableName = entry.split(":")[0];
          String dimCSVFileLoc = entry.substring(tableName.length() + 1);
          fileMaps.put(tableName, dimCSVFileLoc);
        }
      }

      for (int i = 0; i < metahierVoList.size(); i++) {
        HierarchiesInfo hierarchyInfo = metahierVoList.get(i);
        String hierarichiesName = hierarchyInfo.getHierarichieName();
        int[] columnIndex = hierarchyInfo.getColumnIndex();
        String[] columnNames = hierarchyInfo.getColumnNames();
        String query = hierarchyInfo.getQuery();
        boolean isTimeDim = hierarchyInfo.isTimeDimension();
        Map<String, String> levelTypeColumnMap = hierarchyInfo.getLevelTypeColumnMap();
        if (null == query) // table will be denormalized so no foreign
        // key , primary key for this hierarchy
        { // Direct column names will be present in the csv file. in
          // that case continue.
          continue;
        }
        boolean loadToHierarichiTable = hierarchyInfo.isLoadToHierarichiTable();
        Map<String, String[]> columnPropMap = hierarchyInfo.getColumnPropMap();

        updateHierarichiesFromCSVFiles(columnNames, columnPropMap, columnIndex, hierarichiesName,
            loadToHierarichiTable, query, isTimeDim, levelTypeColumnMap, currentRestructNumber,
            fileMaps);

      }
    } catch (Exception e) {
      throw new KettleException(e.getMessage(), e);
    }
  }

  private void updateHierarichiesFromCSVFiles(String[] columnNames,
      Map<String, String[]> columnPropMap, int[] columnIndex, String hierarichiesName,
      boolean loadToHier, String query, boolean isTimeDim, Map<String, String> levelTypeColumnMap,
      int currentRestructNumber, Map<String, String> fileMaps) throws KettleException, IOException

  {

    DimenionLoadCommandHelper dimenionLoadCommandHelper = DimenionLoadCommandHelper.getInstance();

    String modifiedDimesions = dimensionLoadInfo.getModifiedDimesions();

    String substring = query.substring(query.indexOf("SELECT") + 6, query.indexOf("FROM"));
    String[] actualColumnsIncludingPrimaryKey = substring.split(",");

    for (int colIndex = 0; colIndex < actualColumnsIncludingPrimaryKey.length; colIndex++) {
      if (actualColumnsIncludingPrimaryKey[colIndex].contains("\"")) {
        actualColumnsIncludingPrimaryKey[colIndex] =
            actualColumnsIncludingPrimaryKey[colIndex].replaceAll("\"", "");
      }
    }

    String tblName = query.substring(query.indexOf("FROM") + 4).trim();
    if (tblName.contains(".")) {
      tblName = tblName.split("\\.")[1];
    }
    if (tblName.contains("\"")) {
      tblName = tblName.replaceAll("\"", "");
    }
    //trim to remove any spaces
    tblName = tblName.trim();

    //First we need to check whether modified dimensions is null and this is first call for data
    // loading,
    // In that case we need to load the data for all the dimension table.
    // If modifeied dimensions is not null then we will update only the dimension table data
    // which is mensioned in the modified dimension table list.

    // In case of restructuring we are adding one member by default in the level mapping file, so
    // for
    // Incremental load we we need to check if restructure happened then for that table added newly
    // we have to load data. So added method checkModifiedTableInSliceMetaData().
    if (null == modifiedDimesions && dimenionLoadCommandHelper
        .isDimCacheExist(actualColumnsIncludingPrimaryKey, tblName, columnPropMap,
            dimensionLoadInfo) && dimenionLoadCommandHelper
        .isHierCacheExist(hierarichiesName, dimensionLoadInfo) && dimenionLoadCommandHelper
        .checkModifiedTableInSliceMetaData(tblName, dimensionLoadInfo, currentRestructNumber)) {
      return;
    } else if (null != modifiedDimesions && dimenionLoadCommandHelper
        .isDimCacheExist(actualColumnsIncludingPrimaryKey, tblName, columnPropMap,
            dimensionLoadInfo) && dimenionLoadCommandHelper
        .isHierCacheExist(hierarichiesName, dimensionLoadInfo) && dimenionLoadCommandHelper
        .checkModifiedTableInSliceMetaData(tblName, dimensionLoadInfo, currentRestructNumber)) {
      String[] dimTables = modifiedDimesions.split(",");
      int count = 0;
      for (String dimTable : dimTables) {
        if (dimTable.equalsIgnoreCase(tblName)) {
          break;
        }
        count++;
      }

      // table doesnot exist in the modified dimention list then no need
      // to load
      // this dimension table.
      if (count == dimTables.length) {
        return;
      }
    }

    CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();

    // If Dimension table has to load then first check whether it is time Dimension,
    //if yes then check the mappings specified in the realtimedata.properties.
    // If nothing is specified their also then go through the normal flow.
    String primaryKeyColumnName = query.substring(query.indexOf("SELECT") + 6, query.indexOf(","));

    primaryKeyColumnName = tblName + '_' + primaryKeyColumnName.replace("\"", "").trim();
    isTimeDim = false;
    boolean fileAlreadyCreated = false;
    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    try {
      String dimCsvFile = fileMaps.get(tblName);

      if (null == dimCsvFile) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "For Dimension table : \"" + tblName + " \" CSV file path is NULL.");
        throw new RuntimeException(
            "For Dimension table : \"" + dimCsvFile + " \" , CSV file path is NULL.");
      }

      FileType fileType = FileFactory.getFileType(dimCsvFile);

      if (!FileFactory.isFileExist(dimCsvFile, fileType)) {
        LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
            "For Dimension table : \"" + tblName + " \" CSV file not presnt.");
        throw new RuntimeException(
            "For Dimension table : \"" + dimCsvFile + " \" ,CSV file not presnt.");
      }

      fileReader = FileFactory.getDataInputStream(dimCsvFile, fileType);
      bufferedReader =
          new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));

      String header = bufferedReader.readLine();
      if (null == header) {
        return;
      }

      int[] dimColmapping =
          getDimColumnNameMapping(tblName, columnNames, header, dimenionLoadCommandHelper);
      int primaryColumnIndex =
          getPrimaryColumnMap(tblName, primaryKeyColumnName, header, dimenionLoadCommandHelper);

      if (primaryColumnIndex == -1) {
        return;
      }

      String[] columnNameArray = dimenionLoadCommandHelper
          .checkQuotesAndAddTableNameForCSV(dimenionLoadCommandHelper.getRowData(header), tblName);
      int primaryIndexInLevel = dimenionLoadCommandHelper
          .getRepeatedPrimaryFromLevels(tblName, columnNames, actualColumnsIncludingPrimaryKey[0]);
      if (primaryIndexInLevel == -1) {
        if (primaryKeyColumnName.contains("\"")) {
          primaryKeyColumnName = primaryKeyColumnName.replaceAll("\"", "");
        }
        String dimFileName = primaryKeyColumnName + CarbonCommonConstants.LEVEL_FILE_EXTENSION;
      }

      int[] outputVal = new int[columnNames.length];
      int[][] propertyIndex = null;
      int primaryKeySurrogate = -1;
      propertyIndex = new int[columnNames.length][];
      for (int i = 0; i < columnNames.length; i++) {
        String[] property = columnPropMap.get(columnNames[i]);
        propertyIndex[i] = dimenionLoadCommandHelper.getIndex(columnNameArray, property);
      }
      outputVal = new int[columnNames.length];

      boolean isKeyExceeded = false;
      int recordCnt = 0;
      String dataline = null;
      while ((dataline = bufferedReader.readLine()) != null) {
        if (dataline.isEmpty()) {
          continue;
        }
        String[] data = dimenionLoadCommandHelper.getRowData(dataline);

        recordCnt++;
        outputVal = new int[columnIndex.length];
        String primaryKey = data[primaryColumnIndex];
        if (null == primaryKey) {
          continue;
        }

        isKeyExceeded =
            processCSVRows(columnNames, columnIndex, isTimeDim, surrogateKeyGen, dimColmapping,
                outputVal, propertyIndex, data);

        if (!isKeyExceeded) {
          if (primaryIndexInLevel >= 0) {
            primaryKeySurrogate = outputVal[primaryIndexInLevel];
          }
          if (loadToHier) {
            surrogateKeyGen.checkHierExists(outputVal, hierarichiesName, primaryKeySurrogate);
          }
        }
      }

    } catch (KettleException e) {
      throw new KettleException(e.getMessage(), e);
    } finally {
      if (null != bufferedReader) {
        CarbonUtil.closeStreams(bufferedReader);
      }
    }
  }

  /**
   * processCSVRows
   *
   * @param columnNames
   * @param columnIndex
   * @param isTimeDim
   * @param surrogateKeyGen
   * @param dimColmapping
   * @param output
   * @param propertyIndex
   * @param data
   * @return
   * @throws KettleException
   */
  private boolean processCSVRows(String[] columnNames, int[] columnIndex, boolean isTimeDim,
      CarbonCSVBasedDimSurrogateKeyGen surrogateKeyGen, int[] dimColmapping, int[] output,
      int[][] propertyIndex, String[] data) throws KettleException {
    boolean isKeyExceeded = false;
    for (int i = 0; i < columnNames.length; i++) {
      String columnName = null;
      columnName = columnNames[i];
      String tuple = data[dimColmapping[i]];

      Object[] propertyvalue = new Object[propertyIndex[i].length];

      for (int k = 0; k < propertyIndex[i].length; k++) {
        String value = data[propertyIndex[i][k]];

        if (null == value) {
          value = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
        }
        propertyvalue[k] = value;
      }
      if (null == tuple) {
        tuple = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
      }
      if (isTimeDim) {
        output[i] = surrogateKeyGen
            .generateSurrogateKeysForTimeDims(tuple, columnName, columnIndex[i], propertyvalue);
      } else {
        CarbonCSVBasedSeqGenMeta meta = dimensionLoadInfo.getMeta();
        if (meta.isDirectDictionary(i)) {
          DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(meta.getColumnDataType()[i]);
          output[i] = directDictionaryGenerator.generateDirectSurrogateKey(tuple);
        } else {
          output[i] = surrogateKeyGen.generateSurrogateKeys(tuple, columnName);
        }

      }
      if (output[i] == -1) {
        isKeyExceeded = true;
      }
    }
    return isKeyExceeded;
  }

  private int getPrimaryColumnMap(String tableName, String primaryKeyColumnName, String header,
      DimenionLoadCommandHelper dimenionLoadCommandHelper) {
    int index = -1;

    String[] headerColumn = dimenionLoadCommandHelper.getRowData(header);

    for (int j = 0; j < headerColumn.length; j++) {
      if (primaryKeyColumnName.equalsIgnoreCase(tableName + '_' + headerColumn[j])) {
        return j;
      }

    }
    return index;
  }

  /**
   * Return the dimension column mapping.
   *
   * @param tableName
   * @param columnNames
   * @param header
   * @return
   */
  private int[] getDimColumnNameMapping(String tableName, String[] columnNames, String header,
      DimenionLoadCommandHelper commandHelper) {
    int[] index = new int[columnNames.length];

    String[] headerColumn = commandHelper.getRowData(header);

    for (int i = 0; i < columnNames.length; i++) {
      for (int j = 0; j < headerColumn.length; j++) {
        if (columnNames[i].equalsIgnoreCase(tableName + '_' + headerColumn[j])) {
          index[i] = j;
          break;
        }

      }
    }
    return index;
  }

}

