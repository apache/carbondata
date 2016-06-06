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

package org.carbondata.processing.sortandgroupby.sortdata;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.processing.schema.metadata.SortObserver;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.RemoveDictionaryUtil;

public class SortDataRows {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortDataRows.class.getName());
  /**
   * lockObject
   */
  private final Object lockObject = new Object();
  /**
   * tempFileLocation
   */
  private String tempFileLocation;
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
  private int measureColCount;
  /**
   * measure count
   */
  private int dimColCount;
  /**
   * measure count
   */
  private int complexDimColCount;
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
   * sortTempFileNoOFRecordsInCompression
   */
  private int sortTempFileNoOFRecordsInCompression;
  /**
   * isSortTempFileCompressionEnabled
   */
  private boolean isSortFileCompressionEnabled;
  /**
   * prefetch
   */
  private boolean prefetch;
  /**
   * bufferSize
   */
  private int bufferSize;
  private String schemaName;
  private String cubeName;

  private char[] aggType;

  /**
   * To know how many columns are of high cardinality.
   */
  private int noDictionaryCount;
  /**
   * partitionID
   */
  private String partitionID;
  /**
   * Id of the load folder
   */
  private String segmentId;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;

  /**
   * This will tell whether dimension is dictionary or not.
   */
  private boolean[] noDictionaryDimnesionColumn;
  /**
   * executor service for data sort holder
   */
  private ExecutorService dataSorterAndWriterExecutorService;
  /**
   * semaphore which will used for managing sorted data object arrays
   */
  private Semaphore semaphore;

  public SortDataRows(String tableName, int dimColCount, int complexDimColCount,
      int measureColCount, SortObserver observer, int noDictionaryCount, String partitionID,
      String segmentId, String taskNo, boolean[] noDictionaryColMaping) {
    // set table name
    this.tableName = tableName;
    this.partitionID = partitionID;
    this.segmentId = segmentId;
    this.taskNo = taskNo;
    // set measure count
    this.measureColCount = measureColCount;

    this.dimColCount = dimColCount;

    this.noDictionaryCount = noDictionaryCount;
    this.complexDimColCount = complexDimColCount;
    this.noDictionaryDimnesionColumn = noDictionaryColMaping;

    // processed file list
    this.procFiles = new ArrayList<File>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    // observer for main sorting
    this.observer = observer;

    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
    this.aggType = new char[measureColCount];
  }

  /**
   * This method will be used to initialize
   */
  public void initialize(String schemaName, String cubeName)
      throws CarbonSortKeyAndGroupByException {
    this.schemaName = schemaName;
    this.cubeName = cubeName;

    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    setSortConfiguration(carbonProperties);

    // create holder list which will hold incoming rows
    // size of list will be sort buffer size + 1 to avoid creation of new
    // array in list array
    this.recordHolderList = new Object[this.sortBufferSize][];
    updateSortTempFileLocation(carbonProperties);

    // Delete if any older file exists in sort temp folder
    deleteSortLocationIfExists();

    // create new sort temp directory
    if (!new File(this.tempFileLocation).mkdirs()) {
      LOGGER.info("Sort Temp Location Already Exists");
    }
    int numberOfCores = 0;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      numberOfCores = numberOfCores / 2;
    } catch (NumberFormatException exc) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    this.executorService = Executors.newFixedThreadPool(numberOfCores);
    this.dataSorterAndWriterExecutorService = Executors.newFixedThreadPool(numberOfCores);
    semaphore = new Semaphore(numberOfCores);
    this.fileWriteBufferSize = Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE));

    this.isSortFileCompressionEnabled = Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));

    try {
      this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (this.sortTempFileNoOFRecordsInCompression < 1) {
        LOGGER.error("Invalid value for: "
            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
            + ":Only Positive Integer value(greater than zero) is allowed.Default value will "
            + "be used");

        this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid value for: " + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
              + ", only Positive Integer value is allowed. Default value will be used");

      this.sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }

    if (isSortFileCompressionEnabled) {
      LOGGER.info("Compression will be used for writing the sort temp File");
    }

    prefetch = CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE;
    bufferSize = CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE;

    initAggType();
  }

  private void initAggType() {
    Arrays.fill(aggType, 'n');
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(schemaName + CarbonCommonConstants.UNDERSCORE + tableName);
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(tableName);
    for (int i = 0; i < measureColCount; i++) {
      aggType[i] = DataTypeUtil.getAggType(measures.get(i).getDataType());
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

    if (sortBufferSize == currentSize) {
      LOGGER.debug("************ Writing to temp file ********** ");

      File[] fileList;
      if (procFiles.size() >= numberOfIntermediateFileToBeMerged) {
        synchronized (lockObject) {
          fileList = procFiles.toArray(new File[procFiles.size()]);
          this.procFiles = new ArrayList<File>(1);
        }

        LOGGER.debug("Sumitting request for intermediate merging no of files: " + fileList.length);

        startIntermediateMerging(fileList);
      }
      Object[][] recordHolderListLocal = recordHolderList;
      try {
        semaphore.acquire();
        dataSorterAndWriterExecutorService.submit(new DataSorterAndWriter(recordHolderListLocal));
      } catch (InterruptedException e) {
        LOGGER.error(
            "exception occurred while trying to acquire a semaphore lock: " + e.getMessage());
        throw new CarbonSortKeyAndGroupByException(e.getMessage());
      }
      // create the new holder Array
      this.recordHolderList = new Object[this.sortBufferSize][];
      this.entryCount = 0;
    }
    recordHolderList[entryCount++] = row;
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
    LOGGER.info("File based sorting will be used");
    if (this.entryCount > 0) {
      Object[][] toSort;
      toSort = new Object[entryCount][];
      System.arraycopy(recordHolderList, 0, toSort, 0, entryCount);

      if (noDictionaryCount > 0) {
        Arrays.sort(toSort, new RowComparator(noDictionaryDimnesionColumn, noDictionaryCount));
      } else {

        Arrays.sort(toSort, new RowComparatorForNormalDims(this.dimColCount));
      }
      recordHolderList = toSort;

      // create new file
      File file =
          new File(this.tempFileLocation + File.separator + this.tableName + System.nanoTime() +
              CarbonCommonConstants.SORT_TEMP_FILE_EXT);
      writeDataTofile(recordHolderList, this.entryCount, file);

    }

    startFileBasedMerge();
    procFiles = null;
    this.recordHolderList = null;
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    // stream
    if (isSortFileCompressionEnabled || prefetch) {
      writeSortTempFile(recordHolderList, entryCountLocal, file);
      return;
    }
    writeData(recordHolderList, entryCountLocal, file);
  }

  private void writeSortTempFile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    TempSortFileWriter writer = null;

    try {
      writer = getWriter();
      writer.initiaize(file, entryCountLocal);
      writer.writeSortTempFile(recordHolderList);
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e, "Problem while writing the sort temp file");
      throw e;
    } finally {
      writer.finish();
    }
  }

  private void writeData(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), fileWriteBufferSize));

      // write number of entries to the file
      stream.writeInt(entryCountLocal);
      Object[] row = null;
      for (int i = 0; i < entryCountLocal; i++) {
        // get row from record holder list
        row = recordHolderList[i];
        int fieldIndex = 0;

        for (int dimCount = 0; dimCount < this.dimColCount; dimCount++) {
          stream.writeInt(RemoveDictionaryUtil.getDimension(fieldIndex++, row));
        }

        // if any high cardinality dims are present then write it to the file.
        if ((this.noDictionaryCount + this.complexDimColCount) > 0) {
          stream.write(RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row));
        }

        // as measures are stored in separate array.
        fieldIndex = 0;
        for (int mesCount = 0; mesCount < this.measureColCount; mesCount++) {
          if (null != RemoveDictionaryUtil.getMeasure(fieldIndex, row)) {
            stream.write((byte) 1);
            if (aggType[mesCount] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
              Double val = (Double) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeDouble(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_INT_MEASURE) {
              Long val = (Long) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeLong(val);
            } else if (aggType[mesCount] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
              BigDecimal val = (BigDecimal) RemoveDictionaryUtil.getMeasure(fieldIndex, row);
              byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
              stream.writeInt(bigDecimalInBytes.length);
              stream.write(bigDecimalInBytes);
            }
          } else {
            stream.write((byte) 0);
          }
          fieldIndex++;
        }
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
    }
  }

  private TempSortFileWriter getWriter() {
    TempSortFileWriter chunkWriter = null;
    TempSortFileWriter writer = TempSortFileWriterFactory.getInstance()
        .getTempSortFileWriter(isSortFileCompressionEnabled, dimColCount, complexDimColCount,
            measureColCount, noDictionaryCount, fileWriteBufferSize);

    if (prefetch && !isSortFileCompressionEnabled) {
      chunkWriter = new SortTempFileChunkWriter(writer, bufferSize);
    } else {
      chunkWriter = new SortTempFileChunkWriter(writer, sortTempFileNoOFRecordsInCompression);
    }

    return chunkWriter;
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param intermediateFiles
   */
  private void startIntermediateMerging(File[] intermediateFiles) {
    File file = new File(this.tempFileLocation + File.separator + this.tableName + System.nanoTime()
        + CarbonCommonConstants.MERGERD_EXTENSION);

    FileMergerParameters parameters = new FileMergerParameters();
    parameters.setIsNoDictionaryDimensionColumn(noDictionaryDimnesionColumn);
    parameters.setDimColCount(dimColCount);
    parameters.setComplexDimColCount(complexDimColCount);
    parameters.setMeasureColCount(measureColCount);
    parameters.setIntermediateFiles(intermediateFiles);
    parameters.setFileReadBufferSize(fileBufferSize);
    parameters.setFileWriteBufferSize(fileBufferSize);
    parameters.setOutFile(file);
    parameters.setCompressionEnabled(isSortFileCompressionEnabled);
    parameters.setNoOfRecordsInCompression(sortTempFileNoOFRecordsInCompression);
    parameters.setPrefetch(prefetch);
    parameters.setPrefetchBufferSize(bufferSize);
    parameters.setAggType(aggType);
    parameters.setNoDictionaryCount(noDictionaryCount);

    IntermediateFileMerger merger = new IntermediateFileMerger(parameters);
    executorService.submit(merger);
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
    LOGGER.info("Sort size for cube: " + this.sortBufferSize);
    // set number of intermedaite file to merge
    this.numberOfIntermediateFileToBeMerged = Integer.parseInt(instance
        .getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
            CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE));

    LOGGER.info(
        "Number of intermediate file to be merged: " + this.numberOfIntermediateFileToBeMerged);

    // get file buffer size
    this.fileBufferSize = CarbonDataProcessorUtil
        .getFileBufferSize(this.numberOfIntermediateFileToBeMerged, CarbonProperties.getInstance(),
            CarbonCommonConstants.CONSTANT_SIZE_TEN);

    LOGGER.info("File Buffer Size: " + this.fileBufferSize);
  }

  /**
   * This will be used to get the sort temo location
   *
   * @param instance
   */
  private void updateSortTempFileLocation(CarbonProperties instance) {
    // get the base location
    String tempLocationKey = schemaName + '_' + cubeName;
    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier(schemaName, cubeName);
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(baseStorePath, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionID, segmentId);
    this.tempFileLocation = carbonDataDirectoryPath + File.separator + taskNo
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS + File.separator
        + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    LOGGER.info("temp file location" + this.tempFileLocation);
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
   * Below method will be used to start file based merge
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startFileBasedMerge() throws CarbonSortKeyAndGroupByException {
    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      dataSorterAndWriterExecutorService.shutdown();
      dataSorterAndWriterExecutorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
    }
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
      dataSorterAndWriterExecutorService.shutdownNow();
      executorService.shutdownNow();
      observer.setFailed(true);
      LOGGER.error(exception);
      throw new CarbonSortKeyAndGroupByException(exception);
    }
  }

  /**
   * This class is responsible for sorting and writing the object
   * array which holds the records equal to given array size
   */
  private class DataSorterAndWriter implements Callable<Void> {
    private Object[][] recordHolderArray;

    public DataSorterAndWriter(Object[][] recordHolderArray) {
      this.recordHolderArray = recordHolderArray;
    }

    @Override public Void call() throws Exception {
      try {
        long startTime = System.currentTimeMillis();
        if (noDictionaryCount > 0) {
          Arrays.sort(recordHolderArray,
              new RowComparator(noDictionaryDimnesionColumn, noDictionaryCount));
        } else {
          Arrays.sort(recordHolderArray, new RowComparatorForNormalDims(dimColCount));
        }
        // create a new file every time
        File sortTempFile = new File(
            tempFileLocation + File.separator + tableName + System.nanoTime()
                + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
        writeDataTofile(recordHolderArray, recordHolderArray.length, sortTempFile);
        // add sort temp filename to and arrayList. When the list size reaches 20 then
        // intermediate merging of sort temp files will be triggered
        synchronized (lockObject) {
          procFiles.add(sortTempFile);
        }
        LOGGER.info("Time taken to sort and write sort temp file " + sortTempFile + " is: " + (
            System.currentTimeMillis() - startTime));
      } catch (Throwable e) {
        threadStatusObserver.notifyFailed(e);
      } finally {
        semaphore.release();
      }
      return null;
    }
  }
}

