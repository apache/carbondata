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

package org.carbondata.processing.merger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonMergerUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonSliceAndFiles;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.merger.columnar.ColumnarFactFileMerger;
import org.carbondata.processing.merger.columnar.impl.NonTimeBasedMergerColumnar;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.schema.metadata.CarbonColumnarFactMergerInfo;
import org.carbondata.processing.util.CarbonSchemaParser;

public class CarbonColumnarSliceMerger implements CarbonSliceMerger {
  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonColumnarSliceMerger.class.getName());
  /**
   * carbon schema object
   */
  private Schema schema;
  /**
   * carbon cube object
   */
  private Cube cube;
  /**
   * table name to be merged
   */
  private String tableName;
  private List<String> loadsToBeMerged;
  private String mergedLoadName;

  public CarbonColumnarSliceMerger(CarbonSliceMergerInfo carbonSliceMergerInfo) {
    // if schema object is null, then get the schema object after parsing
    // the schema object and update the schema based on partition id
    if (null == carbonSliceMergerInfo.getSchema()) {
      schema = CarbonSchemaParser.loadXML(carbonSliceMergerInfo.getSchemaPath());
    } else {
      schema = carbonSliceMergerInfo.getSchema();
    }
    cube = CarbonSchemaParser.getMondrianCube(schema, carbonSliceMergerInfo.getCubeName());
    if (carbonSliceMergerInfo.getPartitionID() != null && null == carbonSliceMergerInfo
        .getSchema()) {
      String originalSchemaName = schema.name;
      schema.name = originalSchemaName + '_' + carbonSliceMergerInfo.getPartitionID();
      cube.name = cube.name + '_' + carbonSliceMergerInfo.getPartitionID();
    }
    this.tableName = carbonSliceMergerInfo.getTableName();

    this.loadsToBeMerged = carbonSliceMergerInfo.getLoadsToBeMerged();

    this.mergedLoadName = carbonSliceMergerInfo.getMergedLoadName();
  }

  @Override public boolean fullMerge(int currentRestructNumber) throws SliceMergerException {

    String hdfsLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS) + '/'
            + schema.name + '/' + cube.name;

    LOGGER.info("HDFS Location: " + hdfsLocation);
    String localStore = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.STORE_LOCATION,
            CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL) + '/' + schema.name + '/' + cube.name;

    int restrctFolderCount = currentRestructNumber;
    if (restrctFolderCount == -1) {
      restrctFolderCount = 0;
    }
    hdfsLocation =
        hdfsLocation + '/' + CarbonCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount + '/'
            + tableName;

    try {
      if (!FileFactory.isFileExist(hdfsLocation, FileFactory.getFileType(hdfsLocation))) {
        return false;
      }
    } catch (IOException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }

    List<CarbonSliceAndFiles> slicesFromHDFS = CarbonMergerUtil
        .getSliceAndFilesList(hdfsLocation, this.tableName, FileFactory.getFileType(hdfsLocation),
            loadsToBeMerged);

    if (slicesFromHDFS.isEmpty()) {
      return false;
    }

    localStore =
        localStore + '/' + CarbonCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount + '/'
            + tableName + '/' + CarbonCommonConstants.LOAD_FOLDER + mergedLoadName;

    String destinationLocation = localStore + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    File file = new File(destinationLocation);
    if (!file.mkdirs()) {
      throw new SliceMergerException(
          "Problem while creating the destination location for slicemerging");

    }
    startMerge(slicesFromHDFS, CarbonUtil.readSliceMetaDataFile(hdfsLocation, restrctFolderCount),
        file.getAbsolutePath(), restrctFolderCount);

    if (!file.renameTo(new File(localStore))) {
      throw new SliceMergerException(
          "Problem while renaming the destination location for slicemerging");
    }
    return true;
  }

  /**
   * startMerge
   *
   * @throws SliceMergerException
   */
  public void startMerge(List<CarbonSliceAndFiles> slicesFromHDFS, SliceMetaData sliceMetaData,
      String destinationLocation, int currentRestructNumber) throws SliceMergerException {
    String factTableName = CarbonSchemaParser.getFactTableName(this.cube);
    if (factTableName.equals(this.tableName)) {
      mergerSlice(slicesFromHDFS, sliceMetaData, null, null, destinationLocation,
          currentRestructNumber);
    }
  }

  private CarbonColumnarFactMergerInfo getCarbonColumnarFactMergerInfo(
      List<CarbonSliceAndFiles> slicesFromHDFS, String[] aggType, String[] aggClass,
      SliceMetaData readSliceMetaDataFile, String destinationLocation, KeyGenerator globalKeyGen) {
    CarbonColumnarFactMergerInfo columnarFactMergerInfo = new CarbonColumnarFactMergerInfo();
    columnarFactMergerInfo.setAggregatorClass(aggClass);
    columnarFactMergerInfo.setAggregators(aggType);
    columnarFactMergerInfo.setCubeName(this.cube.name);
    columnarFactMergerInfo.setGroupByEnabled(null != aggType ? true : false);
    if (null != aggType) {
      for (int i = 0; i < aggType.length; i++) {
        if (aggType[i].equals(CarbonCommonConstants.DISTINCT_COUNT) || aggType[i]
            .equals(CarbonCommonConstants.CUSTOM)) {
          columnarFactMergerInfo.setMergingRequestForCustomAgg(true);
          break;
        }
      }
    }
    columnarFactMergerInfo.setDestinationLocation(destinationLocation);
    columnarFactMergerInfo
        .setMdkeyLength(readSliceMetaDataFile.getKeyGenerator().getKeySizeInBytes());
    columnarFactMergerInfo.setMeasureCount(readSliceMetaDataFile.getMeasures().length);
    columnarFactMergerInfo.setSchemaName(this.schema.name);
    columnarFactMergerInfo.setTableName(tableName);
    columnarFactMergerInfo.setDimLens(readSliceMetaDataFile.getDimLens());
    columnarFactMergerInfo.setSlicesFromHDFS(slicesFromHDFS);

    columnarFactMergerInfo.setGlobalKeyGen(globalKeyGen);

    char[] type = new char[readSliceMetaDataFile.getMeasures().length];
    Arrays.fill(type, 'n');
    if (null != aggType) {
      for (int i = 0; i < type.length; i++) {
        if (aggType[i].equals(CarbonCommonConstants.DISTINCT_COUNT) || aggType[i]
            .equals(CarbonCommonConstants.CUSTOM)) {
          type[i] = 'c';
        } else {
          type[i] = 'n';
        }
      }
    }
    columnarFactMergerInfo.setType(type);
    return columnarFactMergerInfo;
  }

  /**
   * Below method will be used for merging the slice All the concrete classes
   * will override this method and will implements its own type of merging
   * method
   *
   * @throws SliceMergerException will throw slice merger exception if any problem occurs
   *                              during merging the slice
   */
  public void mergerSlice(List<CarbonSliceAndFiles> slicesFromHDFS, SliceMetaData sliceMetaData,
      String[] aggType, String[] aggClass, String destinationLocation, int currentRestructNumber)
      throws SliceMergerException {
    List<List<BlockletInfoColumnar>> blockletInfoListOfList =
        new ArrayList<List<BlockletInfoColumnar>>(slicesFromHDFS.size());
    List<BlockletInfoColumnar> blockletInfoList = null;
    List<ValueCompressionModel> existingSliceCompressionModel =
        new ArrayList<ValueCompressionModel>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String[] sliceLocation = new String[slicesFromHDFS.size()];
    int index = 0;
    for (int i = 0; i < sliceLocation.length; i++) {
      blockletInfoList =
          new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      CarbonSliceAndFiles sliceAndFiles = slicesFromHDFS.get(i);
      sliceLocation[index++] = sliceAndFiles.getPath();
      CarbonFile[] factFiles = sliceAndFiles.getSliceFactFilesList();
      if (null == factFiles || factFiles.length < 1) {
        continue;
      }
      for (int j = 0; j < factFiles.length; j++) {
        blockletInfoList.addAll(CarbonUtil
            .getBlockletInfoColumnar(factFiles[j]));
      }

      int[] cardinality =
          CarbonMergerUtil.getCardinalityFromLevelMetadata(sliceAndFiles.getPath(), tableName);
      KeyGenerator localKeyGen = KeyGeneratorFactory.getKeyGenerator(cardinality);
      sliceAndFiles.setKeyGen(localKeyGen);

      blockletInfoListOfList.add(blockletInfoList);
      existingSliceCompressionModel
          .add(getCompressionModel(sliceAndFiles.getPath(), sliceMetaData.getMeasures().length));

    }
    for (int i = 0; i < sliceLocation.length; i++) {
      LOGGER.info("Slice Merger Start Merging for slice: " + sliceLocation[i]);
    }
    double[] uniqueValue = new double[sliceMetaData.getMeasures().length];
    double[] maxValue = new double[sliceMetaData.getMeasures().length];
    double[] minValue = new double[sliceMetaData.getMeasures().length];
    int[] decimalLength = new int[sliceMetaData.getMeasures().length];
    if (existingSliceCompressionModel.size() > 0) {
      System.arraycopy(existingSliceCompressionModel.get(0).getUniqueValue(), 0, uniqueValue, 0,
          sliceMetaData.getMeasures().length);
      System.arraycopy(existingSliceCompressionModel.get(0).getMaxValue(), 0, maxValue, 0,
          sliceMetaData.getMeasures().length);
      System.arraycopy(existingSliceCompressionModel.get(0).getMinValue(), 0, minValue, 0,
          sliceMetaData.getMeasures().length);
      System.arraycopy(existingSliceCompressionModel.get(0).getDecimal(), 0, decimalLength, 0,
          sliceMetaData.getMeasures().length);
      for (int i = 1; i < existingSliceCompressionModel.size(); i++) {
        calculateDecimalLength(existingSliceCompressionModel.get(i).getDecimal(), decimalLength);
      }

      // write level metadata

      int[] maxCardinality =
          CarbonMergerUtil.mergeLevelMetadata(sliceLocation, tableName, destinationLocation);

      KeyGenerator globalKeyGen = KeyGeneratorFactory.getKeyGenerator(maxCardinality);

      ColumnarFactFileMerger factMerger = null;

      // pass global key generator;
      factMerger = new NonTimeBasedMergerColumnar(
          getCarbonColumnarFactMergerInfo(slicesFromHDFS, aggType, aggClass, sliceMetaData,
              destinationLocation, globalKeyGen), currentRestructNumber);
      LOGGER.info("Starting fact file merging: ");
      factMerger.mergerSlice();
    }
  }

  /**
   * This method will be used to get the compression model for slice
   *
   * @param path         slice path
   * @param measureCount measure count
   * @return compression model
   */
  private ValueCompressionModel getCompressionModel(String path, int measureCount) {
    ValueCompressionModel compressionModel = ValueCompressionUtil.getValueCompressionModel(
        path + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
            + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, measureCount);
    return compressionModel;
  }

  /**
   * This method will be used to update the measures decimal length If current
   * measure length is more then decimalLength then it will update the decimal
   * length for that measure
   *
   * @param currentMeasure measures array
   */
  private int[] calculateDecimalLength(int[] currentMeasure, int[] decimalLength) {
    int arrayIndex = 0;
    for (int value : currentMeasure) {
      decimalLength[arrayIndex] =
          (decimalLength[arrayIndex] > value ? decimalLength[arrayIndex] : value);
      arrayIndex++;
    }
    return decimalLength;
  }

}
