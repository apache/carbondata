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

package org.carbondata.processing.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.filesystem.HDFSCarbonFile;
import org.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.processing.exception.CarbonDataProcessorException;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.AvgBigDecimalAggregator;
import org.carbondata.query.aggregator.impl.AvgDoubleAggregator;
import org.carbondata.query.aggregator.impl.AvgLongAggregator;
import org.carbondata.query.aggregator.impl.CountAggregator;
import org.carbondata.query.aggregator.impl.DistinctCountAggregator;
import org.carbondata.query.aggregator.impl.MaxAggregator;
import org.carbondata.query.aggregator.impl.MinAggregator;
import org.carbondata.query.aggregator.impl.SumBigDecimalAggregator;
import org.carbondata.query.aggregator.impl.SumDistinctBigDecimalAggregator;
import org.carbondata.query.aggregator.impl.SumDistinctDoubleAggregator;
import org.carbondata.query.aggregator.impl.SumDistinctLongAggregator;
import org.carbondata.query.aggregator.impl.SumDoubleAggregator;
import org.carbondata.query.aggregator.impl.SumLongAggregator;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepMeta;

public final class CarbonDataProcessorUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataProcessorUtil.class.getName());

  private CarbonDataProcessorUtil() {

  }

  /**
   * This method will be used to write measure metadata (max, min ,decimal
   * length)file
   * Measure metadat file will be in below format <max value for each
   * measures><min value for each measures><decimal length of each measures>
   *
   * @throws CarbonDataProcessorException
   * @throws IOException
   * @throws IOException
   */
  private static void writeMeasureMetaDataToFileLocal(Object[] maxValue, Object[] minValue,
      int[] decimalLength, Object[] uniqueValue, char[] aggType, byte[] dataTypeSelected,
      double[] minValueFact, String measureMetaDataFileLocation)
      throws CarbonDataProcessorException {
    int length = maxValue.length;
    // calculating the total size of buffer, which is nothing but [(number
    // of measure * (8*2)) +(number of measure *4)]
    // 8 for holding the double value and 4 for holding the int value, 8*2
    // because of max and min value
    int totalSize = length * CarbonCommonConstants.INT_SIZE_IN_BYTE
        + length * CarbonCommonConstants.CHAR_SIZE_IN_BYTE + length;
    int uniqueValueLength = 0;
    for (int j = 0; j < aggType.length; j++) {
      if (aggType[j] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        BigDecimal val = (BigDecimal) uniqueValue[j];
        byte[] buff = DataTypeUtil.bigDecimalToByte(val);
        uniqueValueLength =
            uniqueValueLength + buff.length + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        val = (BigDecimal) minValue[j];
        buff = DataTypeUtil.bigDecimalToByte(val);
        uniqueValueLength =
            uniqueValueLength + buff.length + CarbonCommonConstants.INT_SIZE_IN_BYTE;
        val = (BigDecimal) maxValue[j];
        buff = DataTypeUtil.bigDecimalToByte(val);
        uniqueValueLength =
            uniqueValueLength + buff.length + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      } else {
        uniqueValueLength = uniqueValueLength + 3 * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE;
      }
    }
    totalSize = totalSize + uniqueValueLength;
    if (minValueFact != null) {
      totalSize += length * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE;
    }
    //        +4 bytes for writing total length at the beginning of measure metadata file
    ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize + CarbonCommonConstants.INT_SIZE_IN_BYTE);
    byteBuffer.putInt(totalSize);
    for (int j = 0; j < aggType.length; j++) {
      byteBuffer.putChar(aggType[j]);
    }

    // add all the max
    for (int j = 0; j < maxValue.length; j++) {
      writeValue(byteBuffer, maxValue[j], aggType[j]);
    }

    // add all the min
    for (int j = 0; j < minValue.length; j++) {
      writeValue(byteBuffer, minValue[j], aggType[j]);
    }

    // add all the decimal
    for (int j = 0; j < decimalLength.length; j++) {
      byteBuffer.putInt(decimalLength[j]);
    }

    for (int j = 0; j < uniqueValue.length; j++) {
      writeValue(byteBuffer, uniqueValue[j], aggType[j]);
    }

    for (int j = 0; j < dataTypeSelected.length; j++) {
      byteBuffer.put(dataTypeSelected[j]);
    }

    if (minValueFact != null) {
      for (int j = 0; j < minValueFact.length; j++) {
        byteBuffer.putDouble(minValueFact[j]);
      }
    }

    // flip the buffer
    byteBuffer.flip();
    FileOutputStream stream = null;
    FileChannel channel = null;
    try {
      stream = new FileOutputStream(measureMetaDataFileLocation);
      // get the channel
      channel = stream.getChannel();
      // write the byte buffer to file
      channel.write(byteBuffer);
    } catch (IOException exception) {
      throw new CarbonDataProcessorException("Problem while writing the measure meta data file",
          exception);
    } finally {
      CarbonUtil.closeStreams(channel, stream);
    }
  }

  private static void writeValue(ByteBuffer byteBuffer, Object value, char type) {
    if (type == CarbonCommonConstants.BIG_INT_MEASURE) {
      byteBuffer.putLong((long) value);
    } else if (type == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
      BigDecimal val = (BigDecimal) value;
      byte[] buff = DataTypeUtil.bigDecimalToByte(val);
      byteBuffer.putInt(buff.length);
      byteBuffer.put(buff);
    } else {
      byteBuffer.putDouble((double) value);
    }
  }

  public static void writeMeasureMetaDataToFile(Object[] maxValue, Object[] minValue,
      int[] decimalLength, Object[] uniqueValue, char[] aggType, byte[] dataTypeSelected,
      String measureMetaDataFileLocation) throws CarbonDataProcessorException {
    writeMeasureMetaDataToFileLocal(maxValue, minValue, decimalLength, uniqueValue, aggType,
        dataTypeSelected, null, measureMetaDataFileLocation);
  }

  public static void writeMeasureMetaDataToFileForAgg(Object[] maxValue, Object[] minValue,
      int[] decimalLength, Object[] uniqueValue, char[] aggType, byte[] dataTypeSelected,
      double[] minValueAgg, String measureMetaDataFileLocation)
      throws CarbonDataProcessorException {
    writeMeasureMetaDataToFileLocal(maxValue, minValue, decimalLength, uniqueValue, aggType,
        dataTypeSelected, minValueAgg, measureMetaDataFileLocation);
  }

  /**
   * This method will be used to read all the RS folders
   *
   * @param schemaName
   * @param cubeName
   */
  public static File[] getAllRSFiles(String schemaName, String cubeName, String baseLocation) {
    baseLocation = baseLocation + File.separator + schemaName + File.separator + cubeName;
    File file = new File(baseLocation);
    File[] rsFile = file.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        return pathname.getName().startsWith(CarbonCommonConstants.RESTRUCTRE_FOLDER);
      }
    });
    return rsFile;
  }

  public static File[] getAllRSFiles(String schemaName, String cubeName) {
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseLocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    baseLocation = baseLocation + File.separator + schemaName + File.separator + cubeName;
    File file = new File(baseLocation);
    File[] rsFile = file.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        return pathname.getName().startsWith(CarbonCommonConstants.RESTRUCTRE_FOLDER);
      }
    });
    return rsFile;
  }

  /**
   * This method will be used to read all the load folders
   *
   * @param rsFiles
   * @param tableName
   * @return
   */
  public static File[] getAllLoadFolders(File rsFiles, String tableName) {
    File file = new File(rsFiles + File.separator + tableName);

    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return (pathname.isDirectory() && pathname.getName()
            .startsWith(CarbonCommonConstants.LOAD_FOLDER));
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the load folders
   *
   * @param rsFiles
   * @param tableName
   * @return
   */
  public static File[] getAllLoadFoldersWithOutInProgressExtension(File rsFiles, String tableName) {
    File file = new File(rsFiles + File.separator + tableName);

    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return (pathname.isDirectory() && pathname.getName()
            .startsWith(CarbonCommonConstants.LOAD_FOLDER) && !pathname.getName()
            .contains(CarbonCommonConstants.FILE_INPROGRESS_STATUS));
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the load folders
   *
   * @return
   */
  public static File[] getAllLoadFolders(File tableFolder) {
    File[] listFiles = tableFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return (pathname.isDirectory() && pathname.getName()
            .startsWith(CarbonCommonConstants.LOAD_FOLDER));
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the load folders
   */
  public static File[] getChildrenFolders(File parentFolder) {
    File[] listFiles = parentFolder.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the fact files
   *
   * @param sliceLocation
   * @param tableName
   * @return
   */
  public static File[] getAllFactFiles(File sliceLocation, final String tableName) {
    File file = new File(sliceLocation.getAbsolutePath());

    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName) && pathname.getName()
            .endsWith(CarbonCommonConstants.FACT_FILE_EXT);
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the files excluding fact files.
   *
   * @param sliceLocation
   * @param tableName
   * @return
   */
  public static File[] getAllFilesExcludeFact(String sliceLocation, final String tableName) {
    File file = new File(sliceLocation);

    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return (!(pathname.getName().startsWith(tableName)));
      }
    });
    return listFiles;
  }

  /**
   * This method will be used to read all the files which are retainable as
   * per the policy applied
   *
   * @param sliceLocation
   * @param tableName
   * @return
   */
  public static File[] getAllRetainableFiles(String sliceLocation, final String tableName) {
    File file = new File(sliceLocation + File.separator + tableName);

    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        return !pathname.getName().startsWith(tableName);
      }
    });
    return listFiles;
  }

  /**
   * @param fileToBeDeleted
   */
  public static String recordFilesNeedsToDeleted(Set<String> fileToBeDeleted) {
    Iterator<String> itr = fileToBeDeleted.iterator();
    String filenames = null;
    BufferedWriter writer = null;
    try {
      while (itr.hasNext()) {
        String filenamesToBeDeleted = itr.next();

        if (null == filenames) {
          filenames =
              filenamesToBeDeleted.substring(0, filenamesToBeDeleted.lastIndexOf(File.separator));
          filenames = filenames + File.separator + CarbonCommonConstants.RETENTION_RECORD;
          File file = new File(filenames);

          if (!file.exists()) {
            if (!file.createNewFile()) {
              throw new Exception("Unable to create file " + file.getName());
            }
          }
        }
        if (null == writer) {
          writer =
              new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filenames), "UTF-8"));
        }

        writer.write(filenamesToBeDeleted);
        writer.newLine();

      }
    } catch (Exception e) {
      LOGGER.error(e, "recordFilesNeedsToDeleted");
    } finally {
      CarbonUtil.closeStreams(writer);
    }
    return filenames;
  }

  /**
   * Pass the folder name, The API will tell you whether the
   * retention processing is in progress or not. Restructure and
   * Merging can call this API inorder to continue with their process.
   *
   * @param folderName
   * @return boolean.
   */
  public static boolean isRetentionProcessIsInProgress(String folderName) {
    boolean inProgress = false;
    String deletionRecordFilePath =
        folderName + File.separator + CarbonCommonConstants.RETENTION_RECORD;
    File deletionRecordFileName = new File(deletionRecordFilePath);
    if (deletionRecordFileName.exists()) {
      inProgress = true;
    }

    return inProgress;

  }

  public static void deleteFileAsPerRetentionFileRecord(String folderName)
      throws CarbonDataProcessorException {
    String deletionRecordFilePath =
        folderName + File.separator + CarbonCommonConstants.RETENTION_RECORD;

    File fileTobeDeleted = null;
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(deletionRecordFilePath), "UTF-8"));
      String sCurrentLine;
      while ((sCurrentLine = reader.readLine()) != null) {
        fileTobeDeleted = new File(sCurrentLine);
        if (fileTobeDeleted.exists()) {
          if (!fileTobeDeleted.delete()) {
            LOGGER.debug("Could not delete the file : " + fileTobeDeleted.getAbsolutePath());
          }
        }

      }

    } catch (FileNotFoundException e) {

      throw new CarbonDataProcessorException("Data Deletion is Failed...");
    } catch (IOException e) {
      throw new CarbonDataProcessorException("Data Deletion is Failed...");
    } finally {
      CarbonUtil.closeStreams(reader);
    }

  }

  /**
   * This mehtod will be used to copmare to byte array
   *
   * @param b1 b1
   * @param b2 b2
   * @return compare result
   */
  public static int compare(byte[] b1, byte[] b2) {
    int cmp = 0;
    int length = b1.length;
    for (int i = 0; i < length; i++) {
      int a = (b1[i] & 0xff);
      int b = (b2[i] & 0xff);
      cmp = a - b;
      if (cmp != 0) {
        cmp = cmp < 0 ? -1 : 1;
        break;
      }
    }
    return cmp;
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
    int fileBufferSize = (configuredBufferSize *
        CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) / numberOfFiles;
    if (fileBufferSize < CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) {
      fileBufferSize = CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    }
    return fileBufferSize;
  }

  /**
   * Utility method to get the level cardinality
   *
   * @return cardinality array
   */
  public static int[] getDimLens(String cardinalityString) {
    String[] dims = cardinalityString.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
    int[] dimLens = new int[dims.length];
    for (int i = 0; i < dims.length; i++) {
      dimLens[i] = Integer.parseInt(dims[i]);
    }

    return dimLens;
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
   * Utility method to get level cardinality string
   *
   * @return level cardinality string
   */
  public static String getLevelCardinalitiesString(int[] dimlens) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < dimlens.length - 1; i++) {
      sb.append(dimlens[i]);
      sb.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
    }
    // in case where there is no dims present but high card dims are present.
    if (dimlens.length > 0) {
      sb.append(dimlens[dimlens.length - 1]);
    }
    return sb.toString();
  }

  /**
   * getUpdatedAggregator
   *
   * @param aggregator
   * @return String[]
   */
  public static String[] getUpdatedAggregator(String[] aggregator) {
    for (int i = 0; i < aggregator.length; i++) {
      if (CarbonCommonConstants.COUNT.equals(aggregator[i])) {
        aggregator[i] = CarbonCommonConstants.SUM;
      }
    }
    return aggregator;
  }

  /**
   * getMaskedByte
   *
   * @param generator
   * @return
   */
  public static int[] getMaskedByte(int[] factLevelIndex, KeyGenerator generator) {

    Set<Integer> integers = new TreeSet<Integer>();
    //
    for (int i = 0; i < factLevelIndex.length; i++) {
      // in case of high card this will be -1
      if (factLevelIndex[i] == -1) {
        continue;
      }
      int[] range = generator.getKeyByteOffsets(factLevelIndex[i]);
      for (int j = range[0]; j <= range[1]; j++) {
        integers.add(j);
      }

    }
    //
    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
      Integer integer = (Integer) iterator.next();
      byteIndexs[j++] = integer.intValue();
    }

    return byteIndexs;
  }

  public static MeasureMetaDataModel getMeasureModelForManual(String storeLocation,
      String tableName, int measureCount, FileType fileType) {
    CarbonFile[] sortedPathForFiles = null;
    MeasureMetaDataModel model = null;
    sortedPathForFiles = CarbonUtil.getAllFactFiles(storeLocation, tableName, fileType);
    if (null != sortedPathForFiles && sortedPathForFiles.length > 0) {

      model = ValueCompressionUtil.readMeasureMetaDataFile(
          storeLocation + File.separator + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME
              + tableName + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, measureCount);
    }
    return model;
  }

  public static Object[] updateMergedMinValue(String schemaName, String cubeName, String tableName,
      int measureCount, String extension, int currentRestructNumber) {
    // get the table name
    String inputStoreLocation = schemaName + File.separator + cubeName;
    // get the base store location
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseStorelocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL)
        + File.separator + inputStoreLocation;
    int restructFolderNumber = currentRestructNumber;
    if (restructFolderNumber < 0) {
      return null;
    }
    baseStorelocation = baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
        + restructFolderNumber + File.separator + tableName;

    // get the current folder sequence
    int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorelocation);
    if (counter < 0) {
      return null;
    }
    File file = new File(baseStorelocation);
    // get the store location
    String storeLocation =
        file.getAbsolutePath() + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter
            + extension;

    String metaDataFileName = CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
        + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT;
    String measureMetaDataFileLocation = storeLocation + metaDataFileName;
    Object[] mergedMinValue =
        ValueCompressionUtil.readMeasureMetaDataFile(measureMetaDataFileLocation, measureCount)
            .getMinValue();
    return mergedMinValue;
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
    CarbonFile carbonFile = null;
    if (fileType.equals(FileFactory.FileType.HDFS)) {
      carbonFile = new HDFSCarbonFile(badLogStoreLocation);
    } else {
      carbonFile = new LocalCarbonFile(badLogStoreLocation);
    }

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

  public static void writeFileAsObjectStream(String sliceMetaDataFilePath,
      SliceMetaData sliceMetaData) throws KettleException {
    FileOutputStream fileOutputStream = null;
    ObjectOutputStream objectOutputStream = null;
    try {
      fileOutputStream = new FileOutputStream(sliceMetaDataFilePath);
      objectOutputStream = new ObjectOutputStream(fileOutputStream);
      objectOutputStream.writeObject(sliceMetaData);
    } catch (FileNotFoundException e) {
      throw new KettleException("slice metadata file not found", e);
    } catch (IOException e) {
      throw new KettleException("Not able to write slice metadata File", e);
    } finally {
      CarbonUtil.closeStreams(objectOutputStream, fileOutputStream);
    }
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
   * Below method will be used to check whether row is empty or not
   *
   * @param row
   * @return row empty
   */
  public static boolean checkAllValuesAreNull(Object[] row) {
    for (int i = 0; i < row.length; i++) {
      if (null != row[i]) {
        return false;
      }
    }
    return true;
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
   * Below method will be used to create the store
   *
   * @param schemaName
   * @param cubeName
   * @param tableName
   * @return store location
   * @throws KettleException
   */
  public static String createStoreLocaion(String schemaName, String cubeName, String tableName,
      boolean deleteExistingStore, int currentRestructFolder) throws KettleException {
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    baseStorePath = baseStorePath + File.separator + schemaName + File.separator + cubeName;
    int restrctFolderCount = currentRestructFolder;
    if (restrctFolderCount == -1) {
      restrctFolderCount = 0;
    }
    String baseStorePathWithTableName =
        baseStorePath + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
            + restrctFolderCount + File.separator + tableName;
    if (deleteExistingStore) {
      File file = new File(baseStorePathWithTableName);
      if (file.exists()) {
        try {
          CarbonUtil.deleteFoldersAndFiles(file);
        } catch (CarbonUtilException e) {
          throw new KettleException(
              "Problem while deleting the existing aggregate table data in case of Manual "
                  + "Aggregation");
        }
      }
    }
    int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseStorePathWithTableName);
    counter++;
    String basePath =
        baseStorePathWithTableName + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter;
    if (new File(basePath).exists()) {
      counter++;
    }
    basePath =
        baseStorePathWithTableName + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter
            + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    boolean isDirCreated = new File(basePath).mkdirs();
    if (!isDirCreated) {
      throw new KettleException("Unable to create dataload directory" + basePath);
    }
    return basePath;
  }

  /**
   * @param aggregator
   * @return
   */
  public static String getAggType(MeasureAggregator aggregator) {
    if (aggregator instanceof SumDoubleAggregator || aggregator instanceof SumLongAggregator
        || aggregator instanceof SumBigDecimalAggregator) {
      return CarbonCommonConstants.SUM;
    } else if (aggregator instanceof MaxAggregator) {
      return CarbonCommonConstants.MAX;
    } else if (aggregator instanceof MinAggregator) {
      return CarbonCommonConstants.MIN;
    } else if (aggregator instanceof AvgDoubleAggregator || aggregator instanceof AvgLongAggregator
        || aggregator instanceof AvgBigDecimalAggregator) {
      return CarbonCommonConstants.AVERAGE;
    } else if (aggregator instanceof CountAggregator) {
      return CarbonCommonConstants.COUNT;
    } else if (aggregator instanceof DistinctCountAggregator) {
      return CarbonCommonConstants.DISTINCT_COUNT;
    } else if (aggregator instanceof SumDistinctDoubleAggregator
        || aggregator instanceof SumDistinctLongAggregator
        || aggregator instanceof SumDistinctBigDecimalAggregator) {
      return CarbonCommonConstants.SUM_DISTINCT;
    }
    return null;
  }

  /**
   * This method will provide the updated cardinality based on newly added
   * dimensions
   *
   * @param factLevels
   * @param aggreateLevels
   * @param factDimCardinality
   * @param newDimesnions
   * @param newDimLens
   * @return
   */
  public static int[] getKeyGenerator(String[] factLevels, String[] aggreateLevels,
      int[] factDimCardinality, String[] newDimesnions, int[] newDimLens) {
    int[] factTableDimensioncardinality = new int[factLevels.length];
    System.arraycopy(factDimCardinality, 0, factTableDimensioncardinality, 0,
        factDimCardinality.length);
    if (null != newDimesnions) {
      for (int j = 0; j < factLevels.length; j++) {
        for (int i = 0; i < newDimesnions.length; i++) {
          if (factLevels[j].equals(newDimesnions[i])) {
            factTableDimensioncardinality[j] = newDimLens[i];
            break;
          }
        }
      }
    }
    return factTableDimensioncardinality;
  }

  public static SliceMetaData readSliceMetadata(String factStoreLocation,
      int currentRestructNumber) {
    String fileLocation = factStoreLocation
        .substring(0, factStoreLocation.indexOf(CarbonCommonConstants.LOAD_FOLDER) - 1);
    SliceMetaData sliceMetaData =
        CarbonUtil.readSliceMetaDataFile(fileLocation, currentRestructNumber);

    return sliceMetaData;
  }

  public static void writeMeasureAggregatorsToSortTempFile(char[] type, DataOutputStream stream,
      MeasureAggregator[] aggregator) throws IOException {
    for (int j = 0; j < aggregator.length; j++) {
      if (type[j] == CarbonCommonConstants.BYTE_VALUE_MEASURE) {
        byte[] byteArray = aggregator[j].getByteArray();
        stream.writeInt(byteArray.length);
        stream.write(byteArray);
      } else if (type[j] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        BigDecimal val = aggregator[j].getBigDecimalValue();
        byte[] byteArray = DataTypeUtil.bigDecimalToByte(val);
        stream.writeInt(byteArray.length);
        stream.write(byteArray);
      } else {
        // if measure value is null than aggregator will return
        // first time true as no record has been added, so writing null value
        if (aggregator[j].isFirstTime()) {
          stream.writeByte(CarbonCommonConstants.MEASURE_NULL_VALUE);
        } else {
          // else writing not null value followed by data
          stream.writeByte(CarbonCommonConstants.MEASURE_NOT_NULL_VALUE);
          if (type[j] == CarbonCommonConstants.BIG_INT_MEASURE) {
            stream.writeLong(aggregator[j].getLongValue());
          } else {
            stream.writeDouble(aggregator[j].getDoubleValue());
          }
        }

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
        builder.substring(0, builder.lastIndexOf(CarbonCommonConstants.HASH_SPC_CHARACTER))
            .toString();
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
        builder.substring(0, builder.indexOf(CarbonCommonConstants.HASH_SPC_CHARACTER)).toString();
    return modOrDelTimesStamp;
  }
}