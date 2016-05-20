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

package org.carbondata.processing.dataprocessor.dataretention;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.exception.CarbonDataProcessorException;
import org.carbondata.processing.factreader.CarbonSurrogateTupleHolder;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.factreader.columnar.CarbonColumnarLeafTupleIterator;
import org.carbondata.processing.schema.metadata.CarbonColumnarFactMergerInfo;
import org.carbondata.processing.store.CarbonFactDataHandlerColumnarMerger;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;

//import java.util.Iterator;

public class DataRetentionHandler {

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataRetentionHandler.class.getName());
  private String schemaName;
  private String cubeName;
  private String hdsfStoreLocation;
  private String columnName;
  private String columnValue;
  private String tableName;
  private int surrogateKeyIndex;
  private KeyGenerator keyGenerator;
  private int mdKeySize;
  private int measureLength;
  private String loadSliceLocation;
  private SliceMetaData sliceMetadata;
  private long[] surrogateKeyArray;
  private CarbonFile[] factFiles;
  private int[] compareIndex;
  private int retentionSurrogateKey = -1;
  private int[] dimensionCardinality;
  private Map<Integer, Integer> retentionSurrogateKeyMap;
  /**
   * rsFiles.
   */
  private CarbonFile[] loadFiles;
  private String dateFormat;
  private int currentRestructNumber;
  private String columnActualName;
  private List<LoadMetadataDetails> listOfLoadMetadataDetails;
  private String dimensionTableName;
  /**
   * dictionary key of columnValue (DateStringFormat)
   */
  private int retentionTimestamp;

  public DataRetentionHandler(String schemaName, String cubeName, String tableName,
      String dimensionTableName, String hdsfStoreLocation, String columnName,
      String columnActualName, String columnValue, String dateFormat, int currentRestructNum,
      List<LoadMetadataDetails> listOfLoadMetadataDetails) {
    this.schemaName = schemaName;
    this.cubeName = cubeName;
    this.hdsfStoreLocation = hdsfStoreLocation + '/' + "store";
    this.columnName = columnName;
    this.columnActualName = columnActualName;
    this.columnValue = columnValue;
    //        this.loadFiles = null;
    this.tableName = tableName;
    this.dateFormat = dateFormat;
    this.currentRestructNumber = currentRestructNum;
    this.listOfLoadMetadataDetails = listOfLoadMetadataDetails;
    this.dimensionTableName = dimensionTableName;
    this.loadFiles = CarbonDataRetentionUtil
        .getAllLoadFolderSlices(schemaName, cubeName, tableName, this.hdsfStoreLocation,
            currentRestructNum);
    //get the dictionary value of the columnValue
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(SqlStatement.Type.TIMESTAMP);
    retentionTimestamp = directDictionaryGenerator.generateDirectSurrogateKey(columnValue);
  }

  /**
   * updateFactFileBasedOnDataRetentionPolicy.This API will apply the data
   * retention policy in store and find whether the complete load has been
   * undergone for deletion or updation.
   *
   * @return Map<String, String> , Data load name and status
   * @throws CarbonDataProcessorException
   */
  public Map<String, String> updateFactFileBasedOnDataRetentionPolicy()

  {
    Map<String, String> loadDetails = new HashMap<String, String>();
    for (int restrucureNum = currentRestructNumber; restrucureNum >= 0; restrucureNum--) {

      loadFiles = CarbonDataRetentionUtil
          .getAllLoadFolderSlices(schemaName, cubeName, tableName, this.hdsfStoreLocation,
              restrucureNum);

      if (null == loadFiles) {
        continue;
      }
      LOGGER.info("System is going Data retention policy based on member"
          + columnValue + " For column:" + columnName);

      String sliceMetadataLocation =
          CarbonUtil.getRSPath(schemaName, cubeName, tableName, hdsfStoreLocation, restrucureNum);
      sliceMetadata = CarbonUtil.readSliceMetaDataFile(sliceMetadataLocation, restrucureNum);

      loadFiles = CarbonUtil.getSortedFileList(loadFiles);
      // Getting the details as per retention member for applying data
      // retention policy.
      for (CarbonFile carbonFile : loadFiles) {
        if (isLoadFolderDeleted(carbonFile)) {
          continue;
        }
        factFiles = CarbonUtil.getAllFactFiles(carbonFile.getAbsolutePath(), tableName,
            FileFactory.getFileType(carbonFile.getAbsolutePath()));
        try {
          dimensionCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(
              carbonFile.getAbsolutePath() + '/' + CarbonCommonConstants.LEVEL_METADATA_FILE
                  + tableName + ".metadata");
          if (null == dimensionCardinality) {
            continue;
          }
        } catch (CarbonUtilException e) {
          LOGGER.error("Failed to apply retention policy.");
        }

        applyRetentionDetailsBasedOnRetentionMember();

        // skip deleted and merged load folders.
        if (!isLoadValid(listOfLoadMetadataDetails, carbonFile.getName())) {
          continue;
        }
        factFiles = CarbonUtil.getSortedFileList(factFiles);
        loadSliceLocation = carbonFile.getAbsolutePath();
        try {
          for (CarbonFile factFile : factFiles) {

            applyDataRetentionPolicy(factFile.getAbsolutePath(), loadDetails, carbonFile.getName(),
                carbonFile.getAbsolutePath(), restrucureNum);

          }
        } catch (CarbonDataProcessorException e) {
          LOGGER.error("Failed to apply retention policy.");
        }
      }

    }
    return loadDetails;

  }

  private boolean isLoadFolderDeleted(CarbonFile carbonFile) {
    boolean status = false;
    for (LoadMetadataDetails loadMetadata : listOfLoadMetadataDetails) {

      if (loadMetadata.getLoadName().equals(carbonFile.getName()
          .substring(carbonFile.getName().lastIndexOf('_') + 1, carbonFile.getName().length()))
          && CarbonCommonConstants.MARKED_FOR_DELETE
          .equalsIgnoreCase(loadMetadata.getLoadStatus())) {
        status = true;
        break;
      }
    }
    return status;
  }

  /**
   * @param loadMetadataDetails2
   * @param name
   * @return
   */
  private boolean isLoadValid(List<LoadMetadataDetails> loadMetadataDetails2, String name) {
    String loadName = name.substring(
        name.indexOf(CarbonCommonConstants.LOAD_FOLDER) + CarbonCommonConstants.LOAD_FOLDER
            .length(), name.length());

    for (LoadMetadataDetails loads : loadMetadataDetails2) {
      if (loads.getLoadName().equalsIgnoreCase(loadName)) {
        if (null != loads.getMergedLoadName()) {
          return false;
        } else if (loads.getLoadStatus()
            .equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
          return false;
        }
        return true;
      } else if (null != loads.getMergedLoadName() && loads.getMergedLoadName()
          .equalsIgnoreCase(loadName) && !loads.getLoadStatus()
          .equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
        return true;
      }
    }

    return false;
  }

  private void applyRetentionDetailsBasedOnRetentionMember() {
    measureLength = sliceMetadata.getMeasures().length;
    String[] dimensions = sliceMetadata.getDimensions();

    int dimensionLenghts = dimensions.length;
    surrogateKeyArray = new long[dimensionLenghts];
    Arrays.fill(surrogateKeyArray, 0);
    StringBuilder fileNameSearchPattern = new StringBuilder();

    // Forming the member name for reading the same inorder to get the
    // surrogate
    if (!columnActualName.equals(columnName)) {
      if (null == dimensionTableName || "".equals(dimensionTableName)) {
        fileNameSearchPattern.append(tableName).append('_').append(columnActualName)
            .append(CarbonCommonConstants.LEVEL_FILE_EXTENSION);
        columnName = columnActualName;
      } else {
        fileNameSearchPattern.append(dimensionTableName).append('_').append(columnActualName)
            .append(CarbonCommonConstants.LEVEL_FILE_EXTENSION);
        columnName = columnActualName;
      }
    } else {
      if (null == dimensionTableName || "".equals(dimensionTableName)) {
        fileNameSearchPattern.append(tableName).append('_').append(columnName)
            .append(CarbonCommonConstants.LEVEL_FILE_EXTENSION);
      } else {
        fileNameSearchPattern.append(dimensionTableName).append('_').append(columnName)
            .append(CarbonCommonConstants.LEVEL_FILE_EXTENSION);
      }
    }
    retentionSurrogateKeyMap = new HashMap<Integer, Integer>(1);
    CarbonFile[] rsLoadFiles;
    for (int restrucureNum = currentRestructNumber; restrucureNum >= 0; restrucureNum--) {

      rsLoadFiles = CarbonDataRetentionUtil
          .getAllLoadFolderSlices(schemaName, cubeName, tableName, this.hdsfStoreLocation,
              restrucureNum);
      for (CarbonFile carbonFile : rsLoadFiles) {
        CarbonFile[] carbonLevelFile = CarbonDataRetentionUtil
            .getFilesArray(carbonFile.getAbsolutePath(), fileNameSearchPattern.toString());
        // Retrieving the surrogate key.
        if (carbonLevelFile.length > 0) {

          CarbonDataRetentionUtil
              .getSurrogateKeyForRetentionMember(carbonLevelFile[0], columnName, columnValue,
                  dateFormat, retentionSurrogateKeyMap);
        }
      }

    }

    List<Integer> compareIndexList = new ArrayList<Integer>(16);
    for (int i = 0; i < dimensionLenghts; i++) {
      if (dimensions[i].equals(tableName + '_' + columnName)) {
        surrogateKeyArray[i] = retentionSurrogateKey;
        compareIndexList.add(i);
        surrogateKeyIndex = i;
        break;
      }
    }
    compareIndex = new int[compareIndexList.size()];
    for (int i = 0; i < compareIndex.length; i++) {
      compareIndex[i] = compareIndexList.get(i);
    }

    keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimensionCardinality);
    mdKeySize = keyGenerator.getKeySizeInBytes();
  }

  private void applyDataRetentionPolicy(String absoluteFactFilePath,
      Map<String, String> loadDetails, String loadName, String loadPath, int restrucureNum)
      throws CarbonDataProcessorException {
    if (null != loadName) {
      loadName = loadName.substring(loadName.indexOf('_') + 1, loadName.length());
    }
    List<BlockletInfoColumnar> blockletInfoList = null;
    String fileToBeUpdated = null;
    for (int i = 0; i < factFiles.length; i++) {

      blockletInfoList = CarbonUtil.getBlockletInfoColumnar(factFiles[i]);

      fileToBeUpdated = factFiles[i].getAbsolutePath();
      if (null != fileToBeUpdated) {
        try {
          LOGGER.info("Following load file will be marked for update: " + loadName);
          loadDetails.put(loadName, CarbonCommonConstants.MARKED_FOR_UPDATE);
          processFactFileAsPerFileToBeUpdatedDetails(blockletInfoList, fileToBeUpdated, loadPath,
              loadDetails, loadName, restrucureNum);
        } catch (CarbonDataProcessorException e) {
          throw new CarbonDataProcessorException(e.getMessage());
        }
      }
    }
  }

  private void processFactFileAsPerFileToBeUpdatedDetails(
      List<BlockletInfoColumnar> blockletInfoList, String fileToBeUpdated, String loadPath,
      Map<String, String> loadDetails, String loadName, int restructureNumber)
      throws CarbonDataProcessorException {

    ValueCompressionModel valueCompressionModel = ValueCompressionUtil.getValueCompressionModel(
        loadSliceLocation + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
            + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, measureLength);
    try {
      FactReaderInfo factReaderInfo = getFactReaderInfo();
      CarbonColumnarLeafTupleIterator columnarLeafTupleItr =
          new CarbonColumnarLeafTupleIterator(loadPath, factFiles, factReaderInfo, mdKeySize);
      CarbonColumnarFactMergerInfo carbonColumnarFactMergerInfo =
          new CarbonColumnarFactMergerInfo();
      carbonColumnarFactMergerInfo.setCubeName(cubeName);
      carbonColumnarFactMergerInfo.setSchemaName(schemaName);
      carbonColumnarFactMergerInfo.setDestinationLocation(loadPath);
      carbonColumnarFactMergerInfo.setDimLens(sliceMetadata.getDimLens());
      carbonColumnarFactMergerInfo.setMdkeyLength(keyGenerator.getKeySizeInBytes());
      carbonColumnarFactMergerInfo.setTableName(tableName);
      carbonColumnarFactMergerInfo.setType(valueCompressionModel.getType());
      carbonColumnarFactMergerInfo.setMeasureCount(measureLength);
      carbonColumnarFactMergerInfo.setIsUpdateFact(true);
      CarbonFactDataHandlerColumnarMerger mergerInstance =
          new CarbonFactDataHandlerColumnarMerger(carbonColumnarFactMergerInfo, restructureNumber);
      mergerInstance.initialise();
      int counter = 0;
      try {
        if (retentionSurrogateKeyMap.isEmpty()) {
          return;
        }
        while (columnarLeafTupleItr.hasNext()) {
          CarbonSurrogateTupleHolder carbonSurrogateTuHolder = columnarLeafTupleItr.next();
          byte[] mdKeyFromStore = carbonSurrogateTuHolder.getMdKey();
          Object[] row = new Object[carbonSurrogateTuHolder.getMeasures().length + 1];
          System.arraycopy(carbonSurrogateTuHolder.getMeasures(), 0, row, 0,
              carbonSurrogateTuHolder.getMeasures().length);

          // comparing the MD keys with retention member retention
          // keys
          long[] storeTupleSurrogates = keyGenerator.getKeyArray(mdKeyFromStore);

          int res = -1;
          int surrKey = -1;
          try {
            surrKey = (int) storeTupleSurrogates[surrogateKeyIndex];
            //res = retentionSurrogateKeyMap.get(surrKey);
            if (surrKey < retentionTimestamp) {
              res = 0;
            }
          } catch (Exception e) {
            LOGGER.info("Member needs to be added in updated fact file surrogate key is: "
                + surrKey);
          }
          if (res == -1) {
            row[row.length - 1] = mdKeyFromStore;
            mergerInstance.addDataToStore(row);
            counter++;
          }
        }

        if (counter == 0) {
          try {
            FileFactory.createNewFile(fileToBeUpdated + CarbonCommonConstants.FACT_DELETE_EXTENSION,
                FileFactory.getFileType(fileToBeUpdated));
          } catch (IOException e) {
            throw new CarbonDataProcessorException(e.getMessage());
          }
          loadDetails.put(loadName, CarbonCommonConstants.MARKED_FOR_DELETE);
          return;
        }
      } catch (CarbonDataWriterException e) {
        throw new CarbonDataProcessorException(e.getMessage());
      } finally {
        if (counter != 0) {
          mergerInstance.finish();
          mergerInstance.closeHandler();
          mergerInstance.copyToHDFS(loadPath);
        }

      }

    } catch (CarbonUtilException e) {
      throw new CarbonDataProcessorException(e.getMessage());
    } catch (CarbonDataWriterException e) {
      throw new CarbonDataProcessorException(e.getMessage());
    }
  }

  private FactReaderInfo getFactReaderInfo() throws CarbonUtilException {
    FactReaderInfo factReaderInfo = new FactReaderInfo();
    int[] blockIndex = new int[sliceMetadata.getDimensions().length];
    for (int i = 0; i < blockIndex.length; i++) {
      blockIndex[i] = i;
    }
    factReaderInfo.setBlockIndex(blockIndex);
    factReaderInfo.setCubeName(cubeName);
    factReaderInfo.setDimLens(sliceMetadata.getDimLens());
    factReaderInfo.setMeasureCount(measureLength);
    factReaderInfo.setSchemaName(schemaName);
    factReaderInfo.setTableName(tableName);
    factReaderInfo.setUpdateMeasureRequired(true);
    return factReaderInfo;
  }

}
