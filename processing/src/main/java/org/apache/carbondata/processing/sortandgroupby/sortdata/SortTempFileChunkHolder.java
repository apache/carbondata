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

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

public class SortTempFileChunkHolder implements Comparable<SortTempFileChunkHolder> {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortTempFileChunkHolder.class.getName());

  /**
   * temp file
   */
  private File tempFile;

  /**
   * read stream
   */
  private DataInputStream stream;

  /**
   * entry count
   */
  private int entryCount;

  /**
   * number record read
   */
  private int numberOfObjectRead;

  /**
   * return row
   */
  private Object[] returnRow;

  /**
   * number of measures
   */
  private int measureCount;

  /**
   * number of dimensionCount
   */
  private int dimensionCount;

  /**
   * number of complexDimensionCount
   */
  private int complexDimensionCount;

  /**
   * fileBufferSize for file reader stream size
   */
  private int fileBufferSize;

  private Object[][] currentBuffer;

  private Object[][] backupBuffer;

  private boolean isBackupFilled;

  private boolean prefetch;

  private int bufferSize;

  private int bufferRowCounter;

  private ExecutorService executorService;

  private Future<Void> submit;

  private int prefetchRecordsProceesed;

  /**
   * sortTempFileNoOFRecordsInCompression
   */
  private int sortTempFileNoOFRecordsInCompression;

  /**
   * isSortTempFileCompressionEnabled
   */
  private boolean isSortTempFileCompressionEnabled;

  /**
   * totalRecordFetch
   */
  private int totalRecordFetch;

  private int noDictionaryCount;

  private char[] aggType;

  /**
   * to store whether dimension is of dictionary type or not
   */
  private boolean[] isNoDictionaryDimensionColumn;

  // TODO temporary configuration, remove after kettle removal
  private boolean useKettle;

  /**
   * Constructor to initialize
   *
   * @param tempFile
   * @param dimensionCount
   * @param complexDimensionCount
   * @param measureCount
   * @param fileBufferSize
   * @param noDictionaryCount
   * @param aggType
   * @param isNoDictionaryDimensionColumn
   */
  public SortTempFileChunkHolder(File tempFile, int dimensionCount, int complexDimensionCount,
      int measureCount, int fileBufferSize, int noDictionaryCount, char[] aggType,
      boolean[] isNoDictionaryDimensionColumn, boolean useKettle) {
    // set temp file
    this.tempFile = tempFile;

    // set measure and dimension count
    this.measureCount = measureCount;
    this.dimensionCount = dimensionCount;
    this.complexDimensionCount = complexDimensionCount;

    this.noDictionaryCount = noDictionaryCount;
    // set mdkey length
    this.fileBufferSize = fileBufferSize;
    this.executorService = Executors.newFixedThreadPool(1);
    this.aggType = aggType;
    this.isNoDictionaryDimensionColumn = isNoDictionaryDimensionColumn;
    this.useKettle = useKettle;
  }

  /**
   * This method will be used to initialize
   *
   * @throws CarbonSortKeyAndGroupByException problem while initializing
   */
  public void initialize() throws CarbonSortKeyAndGroupByException {
    prefetch = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH,
            CarbonCommonConstants.CARBON_MERGE_SORT_PREFETCH_DEFAULT));
    bufferSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
            CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT));
    this.isSortTempFileCompressionEnabled = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE));
    if (this.isSortTempFileCompressionEnabled) {
      LOGGER.info("Compression was used while writing the sortTempFile");
    }

    try {
      this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (this.sortTempFileNoOFRecordsInCompression < 1) {
        LOGGER.error("Invalid value for: "
            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
            + ": Only Positive Integer value(greater than zero) is allowed.Default value will"
            + " be used");

        this.sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid value for: " + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
              + ", only Positive Integer value is allowed.Default value will be used");
      this.sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }

    initialise();
  }

  private void initialise() throws CarbonSortKeyAndGroupByException {
    try {
      if (isSortTempFileCompressionEnabled) {
        this.bufferSize = sortTempFileNoOFRecordsInCompression;
      }
      stream = new DataInputStream(
          new BufferedInputStream(new FileInputStream(tempFile), this.fileBufferSize));
      this.entryCount = stream.readInt();
      if (prefetch) {
        new DataFetcher(false).call();
        totalRecordFetch += currentBuffer.length;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      } else {
        if (isSortTempFileCompressionEnabled) {
          new DataFetcher(false).call();
        }
      }

    } catch (FileNotFoundException e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " No Found", e);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new CarbonSortKeyAndGroupByException(tempFile + " Problem while reading", e);
    }
  }

  /**
   * This method will be used to read new row from file
   *
   * @throws CarbonSortKeyAndGroupByException problem while reading
   */
  public void readRow() throws CarbonSortKeyAndGroupByException {
    if (prefetch) {
      fillDataForPrefetch();
    } else if (isSortTempFileCompressionEnabled) {
      if (bufferRowCounter >= bufferSize) {
        try {
          new DataFetcher(false).call();
          bufferRowCounter = 0;
        } catch (Exception e) {
          LOGGER.error(e);
          throw new CarbonSortKeyAndGroupByException(tempFile + " Problem while reading", e);
        }

      }
      prefetchRecordsProceesed++;
      returnRow = currentBuffer[bufferRowCounter++];
    } else {
      Object[] outRow = getRowFromStream();
      this.returnRow = outRow;
    }
  }

  private void fillDataForPrefetch() {
    if (bufferRowCounter >= bufferSize) {
      if (isBackupFilled) {
        bufferRowCounter = 0;
        currentBuffer = backupBuffer;
        totalRecordFetch += currentBuffer.length;
        isBackupFilled = false;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      } else {
        try {
          submit.get();
        } catch (Exception e) {
          LOGGER.error(e);
        }
        bufferRowCounter = 0;
        currentBuffer = backupBuffer;
        isBackupFilled = false;
        totalRecordFetch += currentBuffer.length;
        if (totalRecordFetch < this.entryCount) {
          submit = executorService.submit(new DataFetcher(true));
        }
      }
    }
    prefetchRecordsProceesed++;
    returnRow = currentBuffer[bufferRowCounter++];
  }

  /**
   * @return
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getRowFromStream() throws CarbonSortKeyAndGroupByException {
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)
    if (useKettle) {
      return getRowFromStreamWithKettle();
    } else {
      return getRowFromStreamWithOutKettle();
    }
  }

  // TODO remove after kettle flow is removed
  private Object[] getRowFromStreamWithKettle() throws CarbonSortKeyAndGroupByException {
    Object[] holder = new Object[3];
    int index = 0;
    Integer[] dim = new Integer[this.dimensionCount];
    Object[] measures = new Object[this.measureCount];
    byte[] finalByteArr = null;
    try {

      // read dimension values

      for (int i = 0; i < this.dimensionCount; i++) {
        dim[index++] = stream.readInt();
      }

      if ((this.noDictionaryCount + this.complexDimensionCount) > 0) {
        short lengthOfByteArray = stream.readShort();
        ByteBuffer buff = ByteBuffer.allocate(lengthOfByteArray + 2);
        buff.putShort(lengthOfByteArray);
        byte[] byteArr = new byte[lengthOfByteArray];
        stream.readFully(byteArr);

        buff.put(byteArr);
        finalByteArr = buff.array();

      }

      index = 0;
      // read measure values
      for (int i = 0; i < this.measureCount; i++) {
        if (stream.readByte() == 1) {
          if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
            measures[index++] = stream.readDouble();
          } else if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
            measures[index++] = stream.readLong();
          } else {
            int len = stream.readInt();
            byte[] buff = new byte[len];
            stream.readFully(buff);
            measures[index++] = buff;
          }
        } else {
          measures[index++] = null;
        }
      }

      NonDictionaryUtil.prepareOutObj(holder, dim, finalByteArr, measures);

      // increment number if record read
      this.numberOfObjectRead++;
    } catch (IOException e) {
      LOGGER.error("Problme while reading the madkey fom sort temp file");
      throw new CarbonSortKeyAndGroupByException("Problem while reading the sort temp file ", e);
    }

    //return out row
    return holder;
  }

  /**
   * Reads row from file
   * @return Object[]
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getRowFromStreamWithOutKettle() throws CarbonSortKeyAndGroupByException {
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)

    Object[] holder = new Object[3];
    int index = 0;
    int nonDicIndex = 0;
    int[] dim = new int[this.dimensionCount];
    byte[][] nonDicArray = new byte[this.noDictionaryCount + this.complexDimensionCount][];
    Object[] measures = new Object[this.measureCount];
    try {
      // read dimension values
      for (int i = 0; i < isNoDictionaryDimensionColumn.length; i++) {
        if (isNoDictionaryDimensionColumn[i]) {
          short len = stream.readShort();
          byte[] array = new byte[len];
          stream.readFully(array);
          nonDicArray[nonDicIndex++] = array;
        } else {
          dim[index++] = stream.readInt();
        }
      }

      for (int i = 0; i < complexDimensionCount; i++) {
        short len = stream.readShort();
        byte[] array = new byte[len];
        stream.readFully(array);
        nonDicArray[nonDicIndex++] = array;
      }

      index = 0;
      // read measure values
      for (int i = 0; i < this.measureCount; i++) {
        if (stream.readByte() == 1) {
          if (aggType[i] == CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE) {
            measures[index++] = stream.readDouble();
          } else if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
            measures[index++] = stream.readLong();
          } else {
            int len = stream.readInt();
            byte[] buff = new byte[len];
            stream.readFully(buff);
            measures[index++] = buff;
          }
        } else {
          measures[index++] = null;
        }
      }

      NonDictionaryUtil.prepareOutObj(holder, dim, nonDicArray, measures);

      // increment number if record read
      this.numberOfObjectRead++;
    } catch (IOException e) {
      LOGGER.error("Problme while reading the madkey fom sort temp file");
      throw new CarbonSortKeyAndGroupByException("Problem while reading the sort temp file ", e);
    }

    //return out row
    return holder;
  }

  /**
   * below method will be used to get the row
   *
   * @return row
   */
  public Object[] getRow() {
    return this.returnRow;
  }

  /**
   * below method will be used to check whether any more records are present
   * in file or not
   *
   * @return more row present in file
   */
  public boolean hasNext() {
    if (prefetch || isSortTempFileCompressionEnabled) {
      return this.prefetchRecordsProceesed < this.entryCount;
    }
    return this.numberOfObjectRead < this.entryCount;
  }

  /**
   * Below method will be used to close streams
   */
  public void closeStream() {
    CarbonUtil.closeStreams(stream);
    executorService.shutdown();
    this.backupBuffer = null;
    this.currentBuffer = null;
  }

  /**
   * This method will number of entries
   *
   * @return entryCount
   */
  public int getEntryCount() {
    return entryCount;
  }

  @Override public int compareTo(SortTempFileChunkHolder other) {
    if (useKettle) {
      return compareWithKettle(other);

    } else {
      return compareWithOutKettle(other);
    }
  }

  // TODO Remove after kettle flow is removed.
  private int compareWithKettle(SortTempFileChunkHolder other) {
    int diff = 0;

    int normalIndex = 0;
    int noDictionaryindex = 0;

    for (boolean isNoDictionary : isNoDictionaryDimensionColumn) {

      if (isNoDictionary) {
        byte[] byteArr1 = (byte[]) returnRow[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()];

        ByteBuffer buff1 = ByteBuffer.wrap(byteArr1);

        // extract a high card dims from complete byte[].
        NonDictionaryUtil
            .extractSingleHighCardDims(byteArr1, noDictionaryindex, noDictionaryCount, buff1);

        byte[] byteArr2 =
            (byte[]) other.returnRow[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()];

        ByteBuffer buff2 = ByteBuffer.wrap(byteArr2);

        // extract a high card dims from complete byte[].
        NonDictionaryUtil
            .extractSingleHighCardDims(byteArr2, noDictionaryindex, noDictionaryCount, buff2);

        int difference = UnsafeComparer.INSTANCE.compareTo(buff1, buff2);
        if (difference != 0) {
          return difference;
        }
        noDictionaryindex++;
      } else {
        int dimFieldA = NonDictionaryUtil.getDimension(normalIndex, returnRow);
        int dimFieldB = NonDictionaryUtil.getDimension(normalIndex, other.returnRow);
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
        normalIndex++;
      }
    }
    return diff;
  }

  private int compareWithOutKettle(SortTempFileChunkHolder other) {
    int diff = 0;
    int index = 0;
    int noDictionaryIndex = 0;
    int[] leftMdkArray = (int[]) returnRow[0];
    int[] rightMdkArray = (int[]) other.returnRow[0];
    byte[][] leftNonDictArray = (byte[][]) returnRow[1];
    byte[][] rightNonDictArray = (byte[][]) other.returnRow[1];
    for (boolean isNoDictionary : isNoDictionaryDimensionColumn) {
      if (isNoDictionary) {
        diff = UnsafeComparer.INSTANCE
            .compareTo(leftNonDictArray[noDictionaryIndex], rightNonDictArray[noDictionaryIndex]);
        if (diff != 0) {
          return diff;
        }
        noDictionaryIndex++;
      } else {
        diff = leftMdkArray[index] - rightMdkArray[index];
        if (diff != 0) {
          return diff;
        }
        index++;
      }

    }
    return diff;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SortTempFileChunkHolder)) {
      return false;
    }
    SortTempFileChunkHolder o = (SortTempFileChunkHolder) obj;



    return o.compareTo(o) == 0;
  }

  @Override public int hashCode() {
    int hash = 0;
    hash += 31 * measureCount;
    hash += 31 * dimensionCount;
    hash += 31 * complexDimensionCount;
    hash += 31 * noDictionaryCount;
    hash += tempFile.hashCode();
    return hash;
  }

  private final class DataFetcher implements Callable<Void> {
    private boolean isBackUpFilling;

    private int numberOfRecords;

    private DataFetcher(boolean backUp) {
      isBackUpFilling = backUp;
      calculateNumberOfRecordsToBeFetched();
    }

    private void calculateNumberOfRecordsToBeFetched() {
      int numberOfRecordsLeftToBeRead = entryCount - totalRecordFetch;
      numberOfRecords =
          bufferSize < numberOfRecordsLeftToBeRead ? bufferSize : numberOfRecordsLeftToBeRead;
    }

    @Override public Void call() throws Exception {
      try {
        if (isBackUpFilling) {
          backupBuffer = prefetchRecordsFromFile(numberOfRecords);
          isBackupFilled = true;
        } else {
          currentBuffer = prefetchRecordsFromFile(numberOfRecords);
        }
      } catch (Exception e) {
        LOGGER.error(e);
      }
      return null;
    }

  }

  /**
   * This method will read the records from sort temp file and keep it in a buffer
   *
   * @param numberOfRecords
   * @return
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[][] prefetchRecordsFromFile(int numberOfRecords)
      throws CarbonSortKeyAndGroupByException {
    Object[][] records = new Object[numberOfRecords][];
    for (int i = 0; i < numberOfRecords; i++) {
      records[i] = getRowFromStream();
    }
    return records;
  }
}
