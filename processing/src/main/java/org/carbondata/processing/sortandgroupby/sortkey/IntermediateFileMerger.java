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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

public class IntermediateFileMerger implements Callable<Void> {

  /**
   * LOGGER
   */
  private static final LogService FILEMERGERLOGGER =
      LogServiceFactory.getLogService(IntermediateFileMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<CarbonSortTempFileChunkHolder> recordHolderHeap;

  /**
   * measure count
   */
  private int measureCount;

  /**
   * mdKeyLenght
   */
  private int mdKeyLength;

  /**
   * intermediateFiles
   */
  private File[] intermediateFiles;

  /**
   * outFile
   */
  private File outFile;

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * mdkeyIndex
   */
  private int mdKeyIndex;

  /**
   * fileBufferSize
   */
  private int fileReadBufferSize;

  /**
   * fileWriteSize
   */
  private int fileWriteBufferSize;

  /**
   * stream
   */
  private DataOutputStream stream;

  /**
   * totalNumberOfRecords
   */
  private int totalNumberOfRecords;

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
   * records
   */
  private Object[][] records;

  /**
   * entryCount
   */
  private int entryCount;

  /**
   * writer
   */
  private CarbonSortTempFileWriter writer;

  /**
   * type
   */
  private char[] type;

  /**
   * prefetch
   */
  private boolean prefetch;

  /**
   * prefetchBufferSize
   */
  private int prefetchBufferSize;

  /**
   * totalSize
   */
  private int totalSize;

  private String[] aggregator;

  /**
   * noDictionaryCount
   */
  private int noDictionaryCount;

  /**
   * IntermediateFileMerger Constructor
   *
   * @param intermediateFiles intermediateFiles
   * @param measureCount      measureCount
   * @param mdKeyLength       mdKeyLength
   * @param outFile           outFile
   */
  public IntermediateFileMerger(File[] intermediateFiles, int fileReadBufferSize, int measureCount,
      int mdKeyLength, File outFile, int mdkeyIndex, int fileWriteBufferSize,
      boolean isFactMdkeyInInputRow, int factMdkeyLength,
      int sortTempFileNoOFRecordsInCompression, boolean isSortTempFileCompressionEnabled,
      char[] type, boolean prefetch, int prefetchBufferSize, String[] aggregator,
      int noDictionaryCount) {
    this.intermediateFiles = intermediateFiles;
    this.measureCount = measureCount;
    this.mdKeyLength = mdKeyLength;
    this.outFile = outFile;
    this.fileCounter = intermediateFiles.length;
    this.mdKeyIndex = mdkeyIndex;
    this.fileReadBufferSize = fileReadBufferSize;
    this.fileWriteBufferSize = fileWriteBufferSize;
    this.isFactMdkeyInInputRow = isFactMdkeyInInputRow;
    this.factMdkeyLength = factMdkeyLength;
    this.sortTempFileNoOFRecordsInCompression = sortTempFileNoOFRecordsInCompression;
    this.isSortTempFileCompressionEnabled = isSortTempFileCompressionEnabled;
    this.type = type;
    this.prefetch = prefetch;
    this.prefetchBufferSize = prefetchBufferSize;
    this.aggregator = aggregator;
    this.noDictionaryCount = noDictionaryCount;
  }

  @Override public Void call() throws Exception {
    boolean isFailed = false;
    try {
      startSorting();
      initialize();
      while (hasNext()) {
        writeDataTofile(next());
      }
      if (isSortTempFileCompressionEnabled || prefetch) {
        if (entryCount > 0) {
          if (entryCount < totalSize) {
            Object[][] tempArr = new Object[entryCount][];
            System.arraycopy(records, 0, tempArr, 0, entryCount);
            records = tempArr;
            this.writer.writeSortTempFile(tempArr);
          } else {
            this.writer.writeSortTempFile(records);
          }
        }
      }
    } catch (Exception ex) {
      FILEMERGERLOGGER.error(ex,
          "Problem while intermediate merging");
      isFailed = true;
    } finally {
      CarbonUtil.closeStreams(this.stream);
      records = null;
      if (null != writer) {
        writer.finish();
      }
      if (!isFailed) {
        try {
          finish();
        } catch (CarbonSortKeyAndGroupByException e) {
          FILEMERGERLOGGER.error(e,
              "Problem while deleting the merge file");
        }
      } else {
        if (this.outFile.delete()) {
          FILEMERGERLOGGER.error("Problem while deleting the merge file");
        }
      }

    }
    return null;
  }

  /**
   * This method is responsible for initialising the out stream
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void initialize() throws CarbonSortKeyAndGroupByException {
    if (!isSortTempFileCompressionEnabled && !prefetch) {
      try {
        this.stream = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(this.outFile), this.fileWriteBufferSize));
        this.stream.writeInt(this.totalNumberOfRecords);
      } catch (FileNotFoundException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while getting the file", e);
      } catch (IOException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while writing the data to file", e);
      }
    } else if (prefetch && !isSortTempFileCompressionEnabled) {
      writer = new CarbonUnCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
          isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type);
      totalSize = prefetchBufferSize;
      writer.initiaize(outFile, totalNumberOfRecords);
    } else {
      writer = new CarbonCompressedSortTempFileWriter(measureCount, mdKeyIndex, mdKeyLength,
          isFactMdkeyInInputRow, factMdkeyLength, fileWriteBufferSize, type);
      totalSize = sortTempFileNoOFRecordsInCompression;
      writer.initiaize(outFile, totalNumberOfRecords);
    }

  }

  /**
   * This method will be used to get the sorted record from file
   *
   * @return sorted record sorted record
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] getSortedRecordFromFile() throws CarbonSortKeyAndGroupByException {
    Object[] row = null;
    // poll the top object from heap
    // heap maintains binary tree which is based on heap condition that will
    // be based on comparator we are passing the heap
    // when will call poll it will always delete root of the tree and then
    // it does trickel down operation complexity is log(n)
    CarbonSortTempFileChunkHolder chunkHolderPoll = this.recordHolderHeap.poll();
    // get the row from chunk
    row = chunkHolderPoll.getRow();
    // check if there no entry present
    if (!chunkHolderPoll.hasNext()) {
      // if chunk is empty then close the stream
      chunkHolderPoll.closeStream();
      // change the file counter
      --this.fileCounter;
      // reaturn row
      return row;
    }
    // read new row
    chunkHolderPoll.readRow();
    // add to heap
    this.recordHolderHeap.add(chunkHolderPoll);
    // return row
    return row;
  }

  /**
   * Below method will be used to start storing process This method will get
   * all the temp files present in sort temp folder then it will create the
   * record holder heap and then it will read first record from each file and
   * initialize the heap
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void startSorting() throws CarbonSortKeyAndGroupByException {
    FILEMERGERLOGGER.info("Number of temp file: " + this.fileCounter);
    // create record holder heap
    createRecordHolderQueue(this.intermediateFiles);
    // iterate over file list and create chunk holder and add to heap
    FILEMERGERLOGGER.info("Started adding first record from each file");
    CarbonSortTempFileChunkHolder carbonSortTempFileChunkHolder = null;
    for (File tempFile : this.intermediateFiles) {
      // create chunk holder
      carbonSortTempFileChunkHolder =
          new CarbonSortTempFileChunkHolder(tempFile, this.measureCount, this.mdKeyLength,
              this.fileReadBufferSize, this.isFactMdkeyInInputRow, this.factMdkeyLength,
              this.aggregator, this.noDictionaryCount, this.type);
      // initialize
      carbonSortTempFileChunkHolder.initialize();
      carbonSortTempFileChunkHolder.readRow();
      this.totalNumberOfRecords += carbonSortTempFileChunkHolder.getEntryCount();
      // add to heap
      this.recordHolderHeap.add(carbonSortTempFileChunkHolder);
    }
    FILEMERGERLOGGER.info("Heap Size" + this.recordHolderHeap.size());
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   *
   * @param listFiles list of temp files
   */
  private void createRecordHolderQueue(File[] listFiles) {
    // creating record holder heap
    this.recordHolderHeap = new PriorityQueue<CarbonSortTempFileChunkHolder>(listFiles.length,
        new Comparator<CarbonSortTempFileChunkHolder>() {
          public int compare(CarbonSortTempFileChunkHolder r1, CarbonSortTempFileChunkHolder r2) {
            byte[] b1 = (byte[]) r1.getRow()[mdKeyIndex];
            byte[] b2 = (byte[]) r2.getRow()[mdKeyIndex];
            int cmp = 0;

            for (int i = 0; i < mdKeyLength; i++) {
              int a = b1[i] & 0xFF;
              int b = b2[i] & 0xFF;
              cmp = a - b;
              if (cmp != 0) {
                return cmp;
              }
            }
            return cmp;
          }
        });
  }

  /**
   * This method will be used to get the sorted row
   *
   * @return sorted row
   * @throws CarbonSortKeyAndGroupByException
   */
  private Object[] next() throws CarbonSortKeyAndGroupByException {
    return getSortedRecordFromFile();
  }

  /**
   * This method will be used to check whether any more element is present or
   * not
   *
   * @return more element is present
   */
  private boolean hasNext() {
    return this.fileCounter > 0;
  }

  /**
   * Below method will be used to write data to file
   *
   * @throws CarbonSortKeyAndGroupByException problem while writing
   */
  private void writeDataTofile(Object[] row) throws CarbonSortKeyAndGroupByException {
    if (isSortTempFileCompressionEnabled || prefetch) {
      if (entryCount == 0) {
        records = new Object[totalSize][];
        records[entryCount++] = row;
        return;
      }
      records[entryCount++] = row;
      if (entryCount == totalSize) {
        entryCount = 0;
        this.writer.writeSortTempFile(records);
        records = new Object[totalSize][];
      }
      return;
    }
    try {
      int aggregatorIndexInRowObject = 0;
      // get row from record holder list
      MeasureAggregator[] aggregator = (MeasureAggregator[]) row[aggregatorIndexInRowObject];
      CarbonDataProcessorUtil.writeMeasureAggregatorsToSortTempFile(type, stream, aggregator);
      stream.writeDouble((Double) row[aggregatorIndexInRowObject + 1]);

      // writing the high cardinality data.
      if (noDictionaryCount > 0) {
        int NoDictionaryIndex = this.mdKeyIndex - 1;
        byte[] singleNoDictionaryArr = (byte[]) row[NoDictionaryIndex];
        stream.write(singleNoDictionaryArr);
      }

      // write mdkye
      stream.write((byte[]) row[this.mdKeyIndex]);
      if (this.isFactMdkeyInInputRow) {
        stream.write((byte[]) row[row.length - 1]);
      }
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the file", e);
    }
  }

  private void finish() throws CarbonSortKeyAndGroupByException {
    if (recordHolderHeap != null) {
      int size = recordHolderHeap.size();
      for (int i = 0; i < size; i++) {
        recordHolderHeap.poll().closeStream();
      }
    }
    try {
      CarbonUtil.deleteFiles(this.intermediateFiles);
    } catch (CarbonUtilException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while deleting the intermediate files");
    }
  }
}
