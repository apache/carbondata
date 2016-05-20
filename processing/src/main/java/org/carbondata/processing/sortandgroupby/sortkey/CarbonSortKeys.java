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

package org.carbondata.processing.sortandgroupby.sortkey;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.groupby.exception.CarbonGroupByException;
import org.carbondata.processing.schema.metadata.SortObserver;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

public class CarbonSortKeys {

  /**
   * LOGGER
   */
  private static final LogService SORTKEYLOGGER =
      LogServiceFactory.getLogService(CarbonSortKeys.class.getName());

  /**
   * CONSTANT_SIZE_TEN
   */
  private static final int CONSTANT_SIZE_TEN = 10;
  /**
   * lockObject
   */
  private final Object lockObject = new Object();
  /**
   * tempFileLocation
   */
  private String tempFileLocation;
  /**
   * mdKeyIndex
   */
  private int mdKeyIndex;
  /**
   * entryCount
   */
  private int entryCount;
  /**
   * tableName
   */
  private String tableName;
  /**
   * sortBufferSize
   */
  private int sortBufferSize;
  /**
   * record holder array
   */
  private Object[][] recordHolderList;
  /**
   * measure count
   */
  private int measureCount;
  /**
   * mdKeyLenght
   */
  private int mdKeyLength;
  /**
   * fileBufferSize
   */
  private int fileBufferSize;
  /**
   * numberOfIntermediateFileToBeMerged
   */
  private int numberOfIntermediateFileToBeMerged;
  /**
   * executorService
   */
  private ExecutorService executorService;
  /**
   * executorService
   */
  private ExecutorService writerExecutorService;
  /**
   * fileWriteBufferSize
   */
  private int fileWriteBufferSize;
  /**
   * procFiles
   */
  private List<File> procFiles;
  /**
   * observer
   */
  private SortObserver observer;

  /**
   * threadStatusObserver
   */
  private ThreadStatusObserver threadStatusObserver;

  /**
   * isAutoAggRequest
   */
  private boolean isAutoAggRequest;

  /**
   * isFactMdkeyInInputRow
   */
  private boolean isFactMdkeyInInputRow;

  /**
   * factMdkeyLength
   */
  private int factMdkeyLength;

  /**
   * sortTempFileNoOFRecordsInCompression
   */
  private int sortTempFileNoOFRecordsInCompression;

  /**
   * isSortTempFileCompressionEnabled
   */
  private boolean isSortTempFileCompressionEnabled;

  /**
   * aggregators
   */
  private String[] aggregators;

  /**
   * aggregatorClass
   */
  private String[] aggregatorClass;

  /**
   * factKetGenerator
   */
  private KeyGenerator factKetGenerator;

  /**
   * type
   */
  private char[] type;

  /**
   * prefetch
   */
  private boolean prefetch;

  /**
   * bufferSize
   */
  private int bufferSize;

  /**
   * isGroupByInSort
   */
  private boolean isGroupByInSort;

  /**
   * isUpdateMemberRequest
   */
  private boolean isUpdateMemberRequest;

  private CarbonSortKeyHashbasedAggregator hashedBasedAgg;

  private int noDictionaryCount;

  private Object[] mergedMinValue;

  public CarbonSortKeys(String tabelName, int measureCount, int mdkeyIndex, int mdkeylength,
      SortObserver observer, boolean autoAggRequest,
      boolean isFactMdkeyInInputRow, int factMdkeyLength, String[] aggregators,
      String[] aggregatorClass, int[] factDims, String schemaName, String cubeName,
      boolean isUpdateMemberRequest, int noDictionaryCount, char[] type) {
    // set table name
    this.tableName = tabelName;
    // set measure count
    this.measureCount = measureCount;

    this.noDictionaryCount = noDictionaryCount;
    // set mdkey index
    this.mdKeyIndex = mdkeyIndex;
    // set mdkey length
    this.mdKeyLength = mdkeylength;
    // processed file list
    this.procFiles = new ArrayList<File>(CONSTANT_SIZE_TEN);
    // observer for main sorting
    this.observer = observer;
    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
    this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
    this.isAutoAggRequest = autoAggRequest;
    this.factMdkeyLength = factMdkeyLength;
    this.aggregators = aggregators;
    this.type = type;
    this.aggregatorClass = aggregatorClass;
    this.factKetGenerator = KeyGeneratorFactory.getKeyGenerator(factDims);
    this.isUpdateMemberRequest = isUpdateMemberRequest;
    if (isFactMdkeyInInputRow && isUpdateMemberRequest) {
      this.isFactMdkeyInInputRow = false;
    }
  }

  /**
   * This method will check of sort resume is required or not in case of check point
   *
   * @param tableName
   * @return SortingResumeRequired
   */
  public static boolean isSortingResumeRequired(String schemaName, String cubeName,
      final String tableName, int currentRestructNumber) {
    // get the base location
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseLocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    // get the temp file location
    String schemaCubeName = schemaName + File.separator + cubeName;
    baseLocation = baseLocation + File.separator + schemaCubeName + File.separator
        + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + tableName;
    File file = new File(baseLocation);
    File[] tempFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        String name = pathname.getName();
        return name.startsWith(tableName) && (
            name.endsWith(CarbonCommonConstants.SORT_TEMP_FILE_EXT) || name
                .endsWith(CarbonCommonConstants.MERGERD_EXTENSION));
      }
    });
    if (null == tempFiles || tempFiles.length < 1) {
      return false;
    }

    //Delete the fact files created before server was killed/stopped
    return deleteFactFiles(schemaName, cubeName, tableName, currentRestructNumber);

  }

  private static boolean deleteFactFiles(String schemaName, String cubeName, final String tableName,
      int currentRestructNumber) {
    // get the base location
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseLocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    // get the temp file location
    String schemaCubeName = schemaName + File.separator + cubeName;
    baseLocation = baseLocation + File.separator + schemaCubeName;

    int restructFolderNumber = currentRestructNumber/*CarbonUtil
    .checkAndReturnNextRestructFolderNumber(baseLocation,"RS_")*/;

    baseLocation = baseLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
        + restructFolderNumber + File.separator + tableName;

    int counter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(baseLocation);
    // This check is just to get the absolute path because from the property file Relative path
    // will come and sometimes FileOutPutstream was not able to Create the file.
    File file = new File(baseLocation);
    String storeLocation =
        file.getAbsolutePath() + File.separator + CarbonCommonConstants.LOAD_FOLDER + counter;

    storeLocation = storeLocation + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

    File loadFolder = new File(storeLocation);
    File[] factFiles = loadFolder.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        if (pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT) || pathname.getName()
            .endsWith(CarbonCommonConstants.FACT_FILE_EXT
                + CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
          return true;
        }
        return false;
      }
    });

    //If Directory does not exist then list file will return null
    // in that case no need to return without deleting the files
    // as files will not be present.
    if (factFiles == null) {
      return false;
    }

    try {
      CarbonUtil.deleteFiles(factFiles);
    } catch (CarbonUtilException e) {
      SORTKEYLOGGER.error("Not Able to delete fact files while resuming the data loading");
    }

    return true;
  }

  /**
   * This method will be used to initialize
   */
  public void initialize(String schemaName, String cubeName, int currentRestructNumber)
      throws CarbonSortKeyAndGroupByException {
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    setSortConfiguration(carbonProperties);
    // create holde list which will hold incoming rows
    // size of list will be sort buffer size + 1 to avoid creation of new
    // array in list array
    this.recordHolderList = new Object[this.sortBufferSize][];
    updateSortTempFileLocation(schemaName, cubeName, carbonProperties);
    deleteSortLocationIfExists();
    // create new sort temp directory
    if (!new File(this.tempFileLocation).mkdirs()) {
      SORTKEYLOGGER.info("Sort Temp Location Already Exists");
    }
    this.writerExecutorService = Executors.newFixedThreadPool(3);
    this.executorService = Executors.newFixedThreadPool(10);
    this.isSortTempFileCompressionEnabled = Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));

    this.fileWriteBufferSize = Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));

    try {
      this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (this.sortTempFileNoOFRecordsInCompression < 1) {
        SORTKEYLOGGER.error("Invalid value for: "
                + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
                + ": Only Positive Integer value(greater than zero) is allowed.Default value will"
                + " be used");

        this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      SORTKEYLOGGER.error("Invalid value for: "
          + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
          + ": Only Positive Integer value(greater than zero) is allowed.Default value will "
          + "be used");
      this.sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }
    if (isSortTempFileCompressionEnabled) {
      SORTKEYLOGGER.info("Compression will be used for writing the sort temp File");
    }

    prefetch = CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE;
    bufferSize = CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;
    isGroupByInSort = false;

    boolean useHashBasedAggWhileSorting = Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_USE_HASHBASED_AGG_INSORT,
            CarbonCommonConstants.CARBON_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE));
    checkGroupByInSortIfValid();
    updateAggTypeForDistinctCount(isGroupByInSort);
    updateAggTypeForDistinctCount(useHashBasedAggWhileSorting);
    updateAggTypeForCustom();

    if (isAutoAggRequest && !isUpdateMemberRequest) {
      mergedMinValue = CarbonDataProcessorUtil
          .updateMergedMinValue(schemaName, cubeName, tableName, this.aggregators.length,
              CarbonCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
    }
    if (isAutoAggRequest && useHashBasedAggWhileSorting && !isUpdateMemberRequest) {
      SORTKEYLOGGER.info("************************ Hased based aggaregator is being used");
      hashedBasedAgg =
          new CarbonSortKeyHashbasedAggregator(aggregators, aggregatorClass, factKetGenerator, type,
              this.sortBufferSize, mergedMinValue);
    }

    boolean useXXHASH =
        Boolean.valueOf(CarbonProperties.getInstance().getProperty("carbon.enableXXHash", "false"));
    if (useXXHASH) {
      SORTKEYLOGGER.info("************************ XXHASH is being used");
    }
  }

  private void updateAggTypeForCustom() {
    if (isAutoAggRequest && isUpdateMemberRequest) {
      for (int i = 0; i < type.length; i++) {
        if (aggregators[i].equals(CarbonCommonConstants.CUSTOM)) {
          type[i] = CarbonCommonConstants.BYTE_VALUE_MEASURE;
        }
      }
    }
  }

  private void updateAggTypeForDistinctCount(boolean groupByInSort) {
    if (isAutoAggRequest && (groupByInSort || isUpdateMemberRequest)) {
      for (int i = 0; i < type.length; i++) {
        if (aggregators[i].equals(CarbonCommonConstants.DISTINCT_COUNT)) {
          type[i] = CarbonCommonConstants.BYTE_VALUE_MEASURE;
        }
      }
    }
  }

  private void checkGroupByInSortIfValid() {
    if (isAutoAggRequest && (isGroupByInSort || isUpdateMemberRequest)) {
      for (int i = 0; i < aggregators.length; i++) {
        if (aggregators[i].equals(CarbonCommonConstants.CUSTOM)) {
          SORTKEYLOGGER.error("Group By In Sort cannot be used as custom measures are present");
          isGroupByInSort = false;
          break;
        }
      }
    }
  }

  /**
   * This method will be used to get the sort configuration
   *
   * @param instance
   */
  private void setSortConfiguration(CarbonProperties instance) {
    // get sort buffer size
    this.sortBufferSize = Integer.parseInt(instance
        .getProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));
    SORTKEYLOGGER.info("Sort buffer size for cube: " + this.sortBufferSize);
    // set number of intermedaite file to merge
    this.numberOfIntermediateFileToBeMerged = Integer.parseInt(instance
        .getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
            CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE));
    SORTKEYLOGGER.info("Number of intermediate files to be merged: "
        + this.numberOfIntermediateFileToBeMerged);
    // get file buffer size
    this.fileBufferSize = CarbonDataProcessorUtil
        .getFileBufferSize(this.numberOfIntermediateFileToBeMerged, CarbonProperties.getInstance(),
            CONSTANT_SIZE_TEN);
    SORTKEYLOGGER.info("File Buffer Size: " + this.fileBufferSize);
  }

  /**
   * This will be used to get the sort temo location
   *
   * @param instance
   */
  private void updateSortTempFileLocation(String schemaName, String cubeName,
      CarbonProperties instance) {
    // get the base location
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseLocation =
        instance.getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    // get the temp file location
    this.tempFileLocation =
        baseLocation + File.separator + schemaName + File.separator + cubeName + File.separator
            + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + this.tableName;
    SORTKEYLOGGER.info("temp file location" + this.tempFileLocation);
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void deleteSortLocationIfExists() throws CarbonSortKeyAndGroupByException {
    CarbonDataProcessorUtil.deleteSortLocationIfExists(this.tempFileLocation);
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public void startSorting() throws CarbonSortKeyAndGroupByException {
    SORTKEYLOGGER.info("File based sorting will be used");
    if (this.entryCount > 0) {
      Object[][] toSort = null;
      if (null != hashedBasedAgg) {
        toSort = hashedBasedAgg.getResult();
        entryCount = toSort.length;
      } else {
        toSort = new Object[entryCount][];
        System.arraycopy(recordHolderList, 0, toSort, 0, entryCount);
      }
      Arrays.sort(toSort, new CarbonRowComparator(this.mdKeyIndex));
      recordHolderList = toSort;

      // create new file
      File file = new File(
          getFileName(this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
              CarbonCommonConstants.SORT_TEMP_FILE_EXT));
      writeDataTofile(recordHolderList, this.entryCount, file);
    }
    procFiles = null;
    this.recordHolderList = null;
    startFileBasedMerge();
  }

  /**
   * Below method will be used to start file based merge
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startFileBasedMerge() throws CarbonSortKeyAndGroupByException {
    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      writerExecutorService.shutdown();
      writerExecutorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
    }
  }

  /**
   * This method will be used to add new row
   *
   * @param row new row
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    int currentSize = entryCount;
    if (null != hashedBasedAgg) {
      currentSize = hashedBasedAgg.getSize();
    }
    if (sortBufferSize == currentSize) {
      SORTKEYLOGGER.debug("************ Writing to temp file ********** ");
      File[] fileList;
      if (procFiles.size() >= numberOfIntermediateFileToBeMerged) {
        synchronized (lockObject) {
          fileList = procFiles.toArray(new File[procFiles.size()]);
          this.procFiles = new ArrayList<File>(1);
        }
        SORTKEYLOGGER.debug("Sumitting request for intermediate merging no of files: "
            + fileList.length);
        startIntermediateMerging(fileList);
      }
      if (null != hashedBasedAgg) {
        this.recordHolderList = hashedBasedAgg.getResult();
        this.hashedBasedAgg.reset();
      }
      // create new file
      File destFile = new File(
          getFileName(this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
              CarbonCommonConstants.SORT_TEMP_FILE_EXT));
      Object[][] recordHolderListLocal = recordHolderList;
      // / create the new holder Array
      this.recordHolderList = new Object[this.sortBufferSize][];
      sortAndWriteToFile(destFile, recordHolderListLocal, sortBufferSize, mdKeyIndex);
      this.entryCount = 0;
    }
    addRecord(row);
  }

  private void addRecord(Object[] row) {
    if (null != hashedBasedAgg) {
      entryCount++;
      hashedBasedAgg.addData(row);
    } else {
      recordHolderList[entryCount++] = row;
    }
  }

  /**
   * sortAndWriteToFile to write data to temp file
   *
   * @param destFile
   * @throws CarbonSortKeyAndGroupByException
   */
  private void sortAndWriteToFile(final File destFile, final Object[][] recordHolderListLocal,
      final int entryCountLocal, final int mdKeyIndexLocal)
      throws CarbonSortKeyAndGroupByException {
    writerExecutorService.submit(new Callable<Void>() {
      @Override public Void call() throws Exception {
        String newFileName = "";
        File finalFile = null;
        try {
          // sort the record holder list
          Arrays.sort(recordHolderListLocal, new CarbonRowComparator(mdKeyIndexLocal));
          // write data to file
          writeDataTofile(recordHolderListLocal, entryCountLocal, destFile);
          newFileName = destFile.getAbsolutePath();
          finalFile = new File(newFileName);
        } catch (Throwable e) {
          threadStatusObserver.notifyFailed(e);
        }
        synchronized (lockObject) {
          procFiles.add(finalFile);
        }
        return null;
      }
    });
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param intermediateFiles
   */
  private void startIntermediateMerging(File[] intermediateFiles) {
    File file = new File(
        getFileName(this.tempFileLocation + File.separator + this.tableName + System.nanoTime(),
            CarbonCommonConstants.MERGERD_EXTENSION));
    IntermediateFileMerger merger =
        new IntermediateFileMerger(intermediateFiles, fileBufferSize, measureCount, mdKeyLength,
            file, mdKeyIndex, this.fileBufferSize,
            this.isFactMdkeyInInputRow, this.factMdkeyLength, sortTempFileNoOFRecordsInCompression,
            isSortTempFileCompressionEnabled, type, prefetch, bufferSize, this.aggregators,
            this.noDictionaryCount);
    executorService.submit(merger);
  }

  /**
   * Below method will return file name base of check required or not
   *
   * @param prefix
   * @param ext
   * @return file name
   */
  private String getFileName(String prefix, String ext) {
    StringBuilder fileName = new StringBuilder();
    fileName.append(prefix);
    fileName.append(ext);
    return fileName.toString();
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    if (isAutoAggRequest && isGroupByInSort && !isUpdateMemberRequest) {
      CarbonSortKeyAggregator sortAggrgateKey =
          new CarbonSortKeyAggregator(aggregators, aggregatorClass, factKetGenerator, type,
              mergedMinValue);
      try {
        recordHolderList = sortAggrgateKey.getAggregatedData(recordHolderList);
      } catch (CarbonGroupByException ex) {
        SORTKEYLOGGER.error(ex,
            "Problem while writing the sort temp file");
        throw new CarbonSortKeyAndGroupByException(ex);
      }
      entryCountLocal = recordHolderList.length;
    }
    // stream
    if (isSortTempFileCompressionEnabled || prefetch) {
      writeSortTempFile(recordHolderList, entryCountLocal, file);
      return;
    }
    writeData(recordHolderList, entryCountLocal, file, type);
  }

  private void writeData(Object[][] recordHolderList, int entryCountLocal, File file, char[] type)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {

      // open stream
      stream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), fileWriteBufferSize));
      // increment file counter
      // write number of entries to the file
      stream.writeInt(entryCountLocal);
      Object[] row = null;
      int aggregatorIndexInRowObject = 0;
      for (int i = 0; i < entryCountLocal; i++) {
        // get row from record holder list
        row = recordHolderList[i];
        MeasureAggregator[] aggregator = (MeasureAggregator[]) row[aggregatorIndexInRowObject];
        CarbonDataProcessorUtil.writeMeasureAggregatorsToSortTempFile(type, stream, aggregator);
        stream.writeDouble((Double) row[aggregatorIndexInRowObject + 1]);

        // write the high cardinality if present.

        int NoDictionaryIndex = this.mdKeyIndex - 1;
        if (null != row[NoDictionaryIndex]) {
          byte[] singleNoDictionaryArr = (byte[]) row[NoDictionaryIndex];
          stream.write(singleNoDictionaryArr);
        }

        // write mdkey
        stream.write((byte[]) row[this.mdKeyIndex]);
        if (isAutoAggRequest && isFactMdkeyInInputRow) {
          stream.write((byte[]) row[row.length - 1]);
        }
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }

  private void writeSortTempFile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    CarbonSortTempFileWriter writer = null;

    try {
      writer = getWriter();
      writer.initiaize(file, entryCountLocal);
      writer.writeSortTempFile(recordHolderList);
    } catch (CarbonSortKeyAndGroupByException e) {
      SORTKEYLOGGER.error(e, "Problem while writing the sort temp file");
      throw e;
    } finally {
      writer.finish();
    }
  }

  private CarbonSortTempFileWriter getWriter() {
    CarbonSortTempFileWriter writer = null;
    if (prefetch && !isSortTempFileCompressionEnabled) {
      writer = new CarbonSortTempFileChunkWriter(
          new CarbonUnCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
              isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type), bufferSize);
    } else {
      writer = new CarbonSortTempFileChunkWriter(
          new CarbonCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
              isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type),
          sortTempFileNoOFRecordsInCompression);
    }

    return writer;
  }

  /**
   * Observer class for thread execution
   * In case of any failure we need stop all the running thread
   */
  private class ThreadStatusObserver {
    /**
     * Below method will be called if any thread fails during execution
     *
     * @param exception
     * @throws CarbonSortKeyAndGroupByException
     */
    public void notifyFailed(Throwable exception) throws CarbonSortKeyAndGroupByException {
      writerExecutorService.shutdownNow();
      executorService.shutdownNow();
      observer.setFailed(true);
      SORTKEYLOGGER.error(exception);
      throw new CarbonSortKeyAndGroupByException(exception);
    }
  }
}
