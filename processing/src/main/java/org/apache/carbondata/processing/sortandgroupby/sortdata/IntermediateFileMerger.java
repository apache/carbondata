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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public class IntermediateFileMerger implements Callable<Void> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(IntermediateFileMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<SortTempFileChunkHolder> recordHolderHeap;

  /**
   * fileCounter
   */
  private int fileCounter;

  /**
   * stream
   */
  private DataOutputStream stream;

  /**
   * totalNumberOfRecords
   */
  private int totalNumberOfRecords;

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
  private TempSortFileWriter writer;

  /**
   * totalSize
   */
  private int totalSize;

  private SortParameters mergerParameters;

  private File[] intermediateFiles;

  private File outPutFile;

  private boolean[] noDictionarycolumnMapping;

  /**
   * IntermediateFileMerger Constructor
   */
  public IntermediateFileMerger(SortParameters mergerParameters, File[] intermediateFiles,
      File outPutFile) {
    this.mergerParameters = mergerParameters;
    this.fileCounter = intermediateFiles.length;
    this.intermediateFiles = intermediateFiles;
    this.outPutFile = outPutFile;
    noDictionarycolumnMapping = mergerParameters.getNoDictionaryDimnesionColumn();
  }

  @Override public Void call() throws Exception {
    long intermediateMergeStartTime = System.currentTimeMillis();
    int fileConterConst = fileCounter;
    boolean isFailed = false;
    try {
      startSorting();
      initialize();
      while (hasNext()) {
        writeDataTofile(next());
      }
      if (mergerParameters.isSortFileCompressionEnabled() || mergerParameters.isPrefetch()) {
        if (entryCount > 0) {
          if (entryCount < totalSize) {
            Object[][] temp = new Object[entryCount][];
            System.arraycopy(records, 0, temp, 0, entryCount);
            records = temp;
            this.writer.writeSortTempFile(temp);
          } else {
            this.writer.writeSortTempFile(records);
          }
        }
      }
      double intermediateMergeCostTime =
          (System.currentTimeMillis() - intermediateMergeStartTime) / 1000.0;
      LOGGER.info("============================== Intermediate Merge of " + fileConterConst +
          " Sort Temp Files Cost Time: " + intermediateMergeCostTime + "(s)");
    } catch (Exception e) {
      LOGGER.error(e, "Problem while intermediate merging");
      isFailed = true;
    } finally {
      records = null;
      CarbonUtil.closeStreams(this.stream);
      if (null != writer) {
        writer.finish();
      }
      if (!isFailed) {
        try {
          finish();
        } catch (CarbonSortKeyAndGroupByException e) {
          LOGGER.error(e, "Problem while deleting the merge file");
        }
      } else {
        if (outPutFile.delete()) {
          LOGGER.error("Problem while deleting the merge file");
        }
      }
    }

    return null;
  }

  /**
   * This method is responsible for initializing the out stream
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void initialize() throws CarbonSortKeyAndGroupByException {
    if (!mergerParameters.isSortFileCompressionEnabled() && !mergerParameters.isPrefetch()) {
      try {
        this.stream = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(outPutFile),
                mergerParameters.getFileWriteBufferSize()));
        this.stream.writeInt(this.totalNumberOfRecords);
      } catch (FileNotFoundException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while getting the file", e);
      } catch (IOException e) {
        throw new CarbonSortKeyAndGroupByException("Problem while writing the data to file", e);
      }
    } else {
      writer = TempSortFileWriterFactory.getInstance()
          .getTempSortFileWriter(mergerParameters.isSortFileCompressionEnabled(),
              mergerParameters.getDimColCount(), mergerParameters.getComplexDimColCount(),
              mergerParameters.getMeasureColCount(), mergerParameters.getNoDictionaryCount(),
              mergerParameters.getFileWriteBufferSize());
      writer.initiaize(outPutFile, totalNumberOfRecords);

      if (mergerParameters.isPrefetch()) {
        totalSize = mergerParameters.getBufferSize();
      } else {
        totalSize = mergerParameters.getSortTempFileNoOFRecordsInCompression();
      }
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
    SortTempFileChunkHolder poll = this.recordHolderHeap.poll();

    // get the row from chunk
    row = poll.getRow();

    // check if there no entry present
    if (!poll.hasNext()) {
      // if chunk is empty then close the stream
      poll.closeStream();

      // change the file counter
      --this.fileCounter;

      // reaturn row
      return row;
    }

    // read new row
    poll.readRow();

    // add to heap
    this.recordHolderHeap.add(poll);

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
    LOGGER.info("Number of temp file: " + this.fileCounter);

    // create record holder heap
    createRecordHolderQueue(intermediateFiles);

    // iterate over file list and create chunk holder and add to heap
    LOGGER.info("Started adding first record from each file");

    SortTempFileChunkHolder sortTempFileChunkHolder = null;

    for (File tempFile : intermediateFiles) {
      // create chunk holder
      sortTempFileChunkHolder =
          new SortTempFileChunkHolder(tempFile, mergerParameters.getDimColCount(),
              mergerParameters.getComplexDimColCount(), mergerParameters.getMeasureColCount(),
              mergerParameters.getFileBufferSize(), mergerParameters.getNoDictionaryCount(),
              mergerParameters.getMeasureDataType(),
              mergerParameters.getNoDictionaryDimnesionColumn(),
              mergerParameters.getNoDictionarySortColumn());

      // initialize
      sortTempFileChunkHolder.initialize();
      sortTempFileChunkHolder.readRow();
      this.totalNumberOfRecords += sortTempFileChunkHolder.getEntryCount();

      // add to heap
      this.recordHolderHeap.add(sortTempFileChunkHolder);
    }

    LOGGER.info("Heap Size" + this.recordHolderHeap.size());
  }

  /**
   * This method will be used to create the heap which will be used to hold
   * the chunk of data
   *
   * @param listFiles list of temp files
   */
  private void createRecordHolderQueue(File[] listFiles) {
    // creating record holder heap
    this.recordHolderHeap = new PriorityQueue<>(listFiles.length);
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
    if (mergerParameters.isSortFileCompressionEnabled() || mergerParameters.isPrefetch()) {
      if (entryCount == 0) {
        records = new Object[totalSize][];
        records[entryCount++] = row;
        return;
      }

      records[entryCount++] = row;
      if (entryCount == totalSize) {
        this.writer.writeSortTempFile(records);
        entryCount = 0;
        records = new Object[totalSize][];
      }
      return;
    }
    try {
      DataType[] aggType = mergerParameters.getMeasureDataType();
      int[] mdkArray = (int[]) row[0];
      byte[][] nonDictArray = (byte[][]) row[1];
      int mdkIndex = 0;
      int nonDictKeyIndex = 0;
      // write dictionary and non dictionary dimensions here.
      for (boolean nodictinary : noDictionarycolumnMapping) {
        if (nodictinary) {
          byte[] col = nonDictArray[nonDictKeyIndex++];
          stream.writeShort(col.length);
          stream.write(col);
        } else {
          stream.writeInt(mdkArray[mdkIndex++]);
        }
      }

      int fieldIndex = 0;
      for (int counter = 0; counter < mergerParameters.getMeasureColCount(); counter++) {
        if (null != NonDictionaryUtil.getMeasure(fieldIndex, row)) {
          stream.write((byte) 1);
          switch (aggType[counter]) {
            case SHORT:
              stream.writeShort((short)NonDictionaryUtil.getMeasure(fieldIndex, row));
              break;
            case INT:
              stream.writeInt((int)NonDictionaryUtil.getMeasure(fieldIndex, row));
              break;
            case LONG:
              stream.writeLong((long)NonDictionaryUtil.getMeasure(fieldIndex, row));
              break;
            case DOUBLE:
              stream.writeDouble((Double) NonDictionaryUtil.getMeasure(fieldIndex, row));
              break;
            case DECIMAL:
              byte[] bigDecimalInBytes = (byte[]) NonDictionaryUtil.getMeasure(fieldIndex, row);
              stream.writeInt(bigDecimalInBytes.length);
              stream.write(bigDecimalInBytes);
              break;
          }
        } else {
          stream.write((byte) 0);
        }
        fieldIndex++;
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
      CarbonUtil.deleteFiles(intermediateFiles);
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while deleting the intermediate files");
    }
  }
}
