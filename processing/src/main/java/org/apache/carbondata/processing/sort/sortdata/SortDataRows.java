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

package org.apache.carbondata.processing.sort.sortdata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ReUsableByteArrayDataOutputStream;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

import org.apache.log4j.Logger;

public class SortDataRows {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SortDataRows.class.getName());

  private int entryCount;

  private Object[][] recordHolderList;

  private ThreadStatusObserver threadStatusObserver;

  private SortParameters parameters;
  private SortStepRowHandler sortStepRowHandler;
  private ThreadLocal<ReUsableByteArrayDataOutputStream> reUsableByteArrayDataOutputStream;
  private int sortBufferSize;

  private int instanceId;

  private SortIntermediateFileMerger intermediateFileMerger;

  public SortDataRows(SortParameters parameters,
      SortIntermediateFileMerger intermediateFileMerger) {
    this.parameters = parameters;
    this.sortStepRowHandler = new SortStepRowHandler(parameters);
    this.intermediateFileMerger = intermediateFileMerger;

    int batchSize = CarbonProperties.getInstance().getBatchSize();

    this.sortBufferSize = Math.max(parameters.getSortBufferSize(), batchSize);
    // observer of writing file in thread
    this.threadStatusObserver = new ThreadStatusObserver();
    this.reUsableByteArrayDataOutputStream = new ThreadLocal<ReUsableByteArrayDataOutputStream>() {
      @Override
      protected ReUsableByteArrayDataOutputStream initialValue() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        return new ReUsableByteArrayDataOutputStream(byteStream);
      }
    };
  }

  public void initialize() {
    // create holder list which will hold incoming rows
    // size of list will be sort buffer size + 1 to avoid creation of new
    // array in list array
    this.recordHolderList = new Object[sortBufferSize][];
  }

  public void setInstanceId(int instanceId) {
    this.instanceId = instanceId;
  }

  public void addRow(Object[] row) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    int currentSize = entryCount;

    if (sortBufferSize == currentSize) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("************ Writing to temp file ********** ");
      }
      Object[][] recordHolderListLocal = recordHolderList;
      handlePreviousPage(recordHolderListLocal);
      // create the new holder Array
      this.recordHolderList = new Object[this.sortBufferSize][];
      this.entryCount = 0;
    }
    recordHolderList[entryCount++] = row;
  }

  public void addRowBatch(Object[][] rowBatch, int size) throws CarbonSortKeyAndGroupByException {
    // if record holder list size is equal to sort buffer size then it will
    // sort the list and then write current list data to file
    int sizeLeft = 0;
    if (entryCount + size >= sortBufferSize) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("************ Writing to temp file ********** ");
      }
      Object[][] recordHolderListLocal = recordHolderList;
      sizeLeft = sortBufferSize - entryCount;
      if (sizeLeft > 0) {
        System.arraycopy(rowBatch, 0, recordHolderListLocal, entryCount, sizeLeft);
      }
      handlePreviousPage(recordHolderListLocal);
      // create the new holder Array
      this.recordHolderList = new Object[this.sortBufferSize][];
      this.entryCount = 0;
      size = size - sizeLeft;
      if (size == 0) {
        return;
      }
    }
    System.arraycopy(rowBatch, sizeLeft, recordHolderList, entryCount, size);
    entryCount += size;
  }

  /**
   * sort and write data
   * @param recordHolderArray
   */
  private void handlePreviousPage(Object[][] recordHolderArray)
          throws CarbonSortKeyAndGroupByException {
    try {
      long startTime = System.currentTimeMillis();
      if (parameters.getNumberOfNoDictSortColumns() > 0) {
        Arrays.sort(recordHolderArray,
                new NewRowComparator(parameters.getNoDictionarySortColumn(),
                        parameters.getNoDictDataType()));
      } else {
        Arrays.sort(recordHolderArray,
                new NewRowComparatorForNormalDims(parameters.getNumberOfSortColumns()));
      }

      // create a new file and choose folder randomly every time
      String[] tmpFileLocation = parameters.getTempFileLocation();
      String locationChosen = tmpFileLocation[new Random().nextInt(tmpFileLocation.length)];
      File sortTempFile = new File(
              locationChosen + File.separator + parameters.getTableName()
                      + '_' + parameters.getRangeId() + '_' + instanceId + '_' + System.nanoTime()
                      + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
      writeDataToFile(recordHolderArray, recordHolderArray.length, sortTempFile);
      // add sort temp filename to arrayList. When the list size reaches 20 then
      // intermediate merging of sort temp files will be triggered
      intermediateFileMerger.addFileToMerge(sortTempFile);
      LOGGER.info("Time taken to sort and write sort temp file " + sortTempFile + " is: " + (
              System.currentTimeMillis() - startTime) + ", sort temp file size in MB is "
              + sortTempFile.length() * 0.1 * 10 / 1024 / 1024);
    } catch (Throwable e) {
      threadStatusObserver.notifyFailed(e);
    }
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
      if (parameters.getNumberOfNoDictSortColumns() > 0) {
        Arrays.sort(toSort, new NewRowComparator(parameters.getNoDictionarySortColumn(),
            parameters.getNoDictDataType()));
      } else {
        Arrays.sort(toSort, new NewRowComparatorForNormalDims(parameters.getNumberOfSortColumns()));
      }
      recordHolderList = toSort;

      // create new file and choose folder randomly
      String[] tmpLocation = parameters.getTempFileLocation();
      String locationChosen = tmpLocation[new Random().nextInt(tmpLocation.length)];
      File file = new File(locationChosen + File.separator + parameters.getTableName()
          + '_' + parameters.getRangeId() + '_' + instanceId + '_' + System.nanoTime()
          + CarbonCommonConstants.SORT_TEMP_FILE_EXT);
      writeDataToFile(recordHolderList, this.entryCount, file);
    }

    this.recordHolderList = null;
  }

  /**
   * Below method will be used to write data to sort temp file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataToFile(Object[][] recordHolderList, int entryCountLocal, File file)
      throws CarbonSortKeyAndGroupByException {
    DataOutputStream stream = null;
    try {
      // open stream
      stream = FileFactory.getDataOutputStream(file.getPath(),
          parameters.getFileWriteBufferSize(), parameters.getSortTempCompressorName());
      // write number of entries to the file
      stream.writeInt(entryCountLocal);
      for (int i = 0; i < entryCountLocal; i++) {
        sortStepRowHandler.writeRawRowAsIntermediateSortTempRowToOutputStream(
            recordHolderList[i], stream, reUsableByteArrayDataOutputStream.get());
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    } finally {
      // close streams
      CarbonUtil.closeStreams(stream);
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
      close();
      parameters.getObserver().setFailed(true);
      LOGGER.error(exception);
      throw new CarbonSortKeyAndGroupByException(exception);
    }
  }

  public void close() {
    intermediateFileMerger.close();
  }

}

