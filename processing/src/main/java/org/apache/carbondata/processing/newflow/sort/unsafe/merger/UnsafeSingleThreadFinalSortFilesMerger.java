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

package org.apache.carbondata.processing.newflow.sort.unsafe.merger;

import java.io.File;
import java.io.FileFilter;
import java.util.AbstractQueue;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.SortTempChunkHolder;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeFinalMergePageHolder;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeInmemoryHolder;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeSortTempFileChunkHolder;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

public class UnsafeSingleThreadFinalSortFilesMerger extends CarbonIterator<Object[]> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeSingleThreadFinalSortFilesMerger.class.getName());

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * recordHolderHeap
   */
  private AbstractQueue<SortTempChunkHolder> recordHolderHeapLocal;

  private SortParameters parameters;

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
  private int noDictionaryCount;

  private int complexDimensionCount;

  private boolean[] isNoDictionaryDimensionColumn;

  /**
   * tempFileLocation
   */
  private String tempFileLocation;

  private String tableName;

  public UnsafeSingleThreadFinalSortFilesMerger(SortParameters parameters) {
    this.parameters = parameters;
    // set measure and dimension count
    this.measureCount = parameters.getMeasureColCount();
    this.dimensionCount = parameters.getDimColCount();
    this.complexDimensionCount = parameters.getComplexDimColCount();

    this.noDictionaryCount = parameters.getNoDictionaryCount();
    this.isNoDictionaryDimensionColumn = parameters.getNoDictionaryDimnesionColumn();
    this.tempFileLocation = parameters.getTempFileLocation();
    this.tableName = parameters.getTableName();
  }

  /**
   * This method will be used to merger the merged files
   *
   */
  public void startFinalMerge(UnsafeCarbonRowPage[] rowPages,
      List<UnsafeInMemoryIntermediateDataMerger> merges) throws CarbonDataWriterException {
    startSorting(rowPages, merges);
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   */
  private void startSorting(UnsafeCarbonRowPage[] rowPages,
      List<UnsafeInMemoryIntermediateDataMerger> merges) throws CarbonDataWriterException {
    try {
      File[] filesToMergeSort = getFilesToMergeSort();
      this.fileCounter = rowPages.length + filesToMergeSort.length + merges.size();

      LOGGER.info("Number of row pages: " + this.fileCounter);

      // create record holder heap
      createRecordHolderQueue();

      // iterate over file list and create chunk holder and add to heap
      LOGGER.info("Started adding first record from each page");
      for (final UnsafeCarbonRowPage rowPage : rowPages) {

        SortTempChunkHolder sortTempFileChunkHolder = new UnsafeInmemoryHolder(rowPage,
            parameters.getDimColCount() + parameters.getComplexDimColCount() + parameters
                .getMeasureColCount());

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      for (final UnsafeInMemoryIntermediateDataMerger merger : merges) {

        SortTempChunkHolder sortTempFileChunkHolder =
            new UnsafeFinalMergePageHolder(merger, parameters.getNoDictionaryDimnesionColumn(),
                parameters.getDimColCount() + parameters.getComplexDimColCount() + parameters
                    .getMeasureColCount());

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      for (final File file : filesToMergeSort) {

        SortTempChunkHolder sortTempFileChunkHolder =
            new UnsafeSortTempFileChunkHolder(file, parameters);

        // initialize
        sortTempFileChunkHolder.readRow();

        recordHolderHeapLocal.add(sortTempFileChunkHolder);
      }

      LOGGER.info("Heap Size" + this.recordHolderHeapLocal.size());
    } catch (Exception e) {
      LOGGER.error(e);
      throw new CarbonDataWriterException(e.getMessage());
    }
  }

  private File[] getFilesToMergeSort() {
    // get all the merged files
    File file = new File(tempFileLocation);

    File[] fileList = file.listFiles(new FileFilter() {
      public boolean accept(File pathname) {
        return pathname.getName().startsWith(tableName);
      }
    });

    if (null == fileList || fileList.length < 0) {
      return new File[0];
    }
    return fileList;
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   */
  private void createRecordHolderQueue() {
    // creating record holder heap
    this.recordHolderHeapLocal = new PriorityQueue<SortTempChunkHolder>(fileCounter);
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   */
  public Object[] next() {
    return convertRow(getSortedRecordFromFile());
  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   */
  private Object[] getSortedRecordFromFile() throws CarbonDataWriterException {
    Object[] row = null;

    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    SortTempChunkHolder poll = this.recordHolderHeapLocal.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.close();

      // change the file counter
      --this.fileCounter;

      // reaturn row
      return row;
    }

    // read new row
    try {
      poll.readRow();
    } catch (Exception e) {
      throw new CarbonDataWriterException(e.getMessage(), e);
    }

    // add to heap
    this.recordHolderHeapLocal.add(poll);

    // return row
    return row;
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  public boolean hasNext() {
    return this.fileCounter > 0;
  }

  private Object[] convertRow(Object[] data) {
    // create new row of size 3 (1 for dims , 1 for high card , 1 for measures)

    Object[] holder = new Object[3];
    int index = 0;
    int nonDicIndex = 0;
    int allCount = 0;
    int[] dim = new int[this.dimensionCount];
    byte[][] nonDicArray = new byte[this.noDictionaryCount + this.complexDimensionCount][];
    Object[] measures = new Object[this.measureCount];
    try {
      // read dimension values
      for (int i = 0; i < isNoDictionaryDimensionColumn.length; i++) {
        if (isNoDictionaryDimensionColumn[i]) {
          nonDicArray[nonDicIndex++] = (byte[]) data[i];
        } else {
          dim[index++] = (int) data[allCount];
        }
        allCount++;
      }

      for (int i = 0; i < complexDimensionCount; i++) {
        nonDicArray[nonDicIndex++] = (byte[]) data[allCount];
        allCount++;
      }

      index = 0;
      // read measure values
      for (int i = 0; i < this.measureCount; i++) {
        measures[index++] = data[allCount];
        allCount++;
      }

      NonDictionaryUtil.prepareOutObj(holder, dim, nonDicArray, measures);

      // increment number if record read
    } catch (Exception e) {
      throw new RuntimeException("Problem while converting row ", e);
    }

    //return out row
    return holder;
  }

  public void clear() {
    if (null != recordHolderHeapLocal) {
      for (SortTempChunkHolder pageHolder : recordHolderHeapLocal) {
        pageHolder.close();
      }
      recordHolderHeapLocal = null;
    }
  }
}
