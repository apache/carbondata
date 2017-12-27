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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

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

  private SortParameters mergerParameters;

  private File[] intermediateFiles;

  private File outPutFile;
  private int dimCnt;
  private int noDictDimCnt;
  private int complexCnt;
  private int measureCnt;
  private boolean[] isNoDictionaryDimensionColumn;
  private DataType[] measureDataTypes;
  private int writeBufferSize;
  private String compressorName;

  private Throwable throwable;

  /**
   * IntermediateFileMerger Constructor
   */
  public IntermediateFileMerger(SortParameters mergerParameters, File[] intermediateFiles,
      File outPutFile) {
    this.mergerParameters = mergerParameters;
    this.fileCounter = intermediateFiles.length;
    this.intermediateFiles = intermediateFiles;
    this.outPutFile = outPutFile;
    this.dimCnt = mergerParameters.getDimColCount();
    this.noDictDimCnt = mergerParameters.getNoDictionaryCount();
    this.complexCnt = mergerParameters.getComplexDimColCount();
    this.measureCnt = mergerParameters.getMeasureColCount();
    this.isNoDictionaryDimensionColumn = mergerParameters.getNoDictionaryDimnesionColumn();
    this.measureDataTypes = mergerParameters.getMeasureDataType();
    this.writeBufferSize = mergerParameters.getBufferSize();
    this.compressorName = mergerParameters.getSortTempCompressorName();
  }

  @Override public Void call() throws Exception {
    long intermediateMergeStartTime = System.currentTimeMillis();
    int fileConterConst = fileCounter;
    try {
      startSorting();
      initialize();
      while (hasNext()) {
        writeDataToFile(next());
      }
      double intermediateMergeCostTime =
          (System.currentTimeMillis() - intermediateMergeStartTime) / 1000.0;
      LOGGER.info("============================== Intermediate Merge of " + fileConterConst +
          " Sort Temp Files Cost Time: " + intermediateMergeCostTime + "(s)");
    } catch (Exception e) {
      LOGGER.error(e, "Problem while intermediate merging");
      clear();
      throwable = e;
    } finally {
      CarbonUtil.closeStreams(this.stream);
      if (null == throwable) {
        try {
          finish();
        } catch (CarbonSortKeyAndGroupByException e) {
          LOGGER.error(e, "Problem while deleting the merge file");
          throwable = e;
        }
      } else {
        if (!outPutFile.delete()) {
          LOGGER.error("Problem while deleting the merge file");
        }
      }
    }
    if (null != throwable) {
      throw new CarbonSortKeyAndGroupByException(throwable);
    }
    return null;
  }

  /**
   * This method is responsible for initializing the out stream
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  private void initialize() throws CarbonSortKeyAndGroupByException {
    try {
      stream = FileFactory.getDataOutputStream(outPutFile.getPath(), FileFactory.FileType.LOCAL,
          writeBufferSize, compressorName);
      this.stream.writeInt(this.totalNumberOfRecords);
    } catch (FileNotFoundException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while getting the file", e);
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while writing the data to file", e);
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
          new SortTempFileChunkHolder(tempFile, mergerParameters, mergerParameters.getTableName());

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
  private void writeDataToFile(Object[] row) throws CarbonSortKeyAndGroupByException {
    try {
      int[] mdkArray = (int[]) row[0];
      byte[][] nonDictArray = (byte[][]) row[1];
      int mdkIndex = 0;
      int nonDictKeyIndex = 0;
      // write dictionary and non dictionary dimensions here.
      for (boolean nodictinary : isNoDictionaryDimensionColumn) {
        if (nodictinary) {
          byte[] col = nonDictArray[nonDictKeyIndex++];
          stream.writeShort(col.length);
          stream.write(col);
        } else {
          stream.writeInt(mdkArray[mdkIndex++]);
        }
      }
      // write complex
      for (; nonDictKeyIndex < noDictDimCnt + complexCnt; nonDictKeyIndex++) {
        byte[] col = nonDictArray[nonDictKeyIndex++];
        stream.writeShort(col.length);
        stream.write(col);
      }
      // write measure
      int fieldIndex = 0;
      for (int counter = 0; counter < measureCnt; counter++) {
        if (null != NonDictionaryUtil.getMeasure(fieldIndex, row)) {
          stream.write((byte) 1);
          DataType dataType = measureDataTypes[counter];
          if (dataType == DataTypes.BOOLEAN) {
            stream.writeBoolean((boolean)NonDictionaryUtil.getMeasure(fieldIndex, row));
          } else if (dataType == DataTypes.SHORT) {
            stream.writeShort((short) NonDictionaryUtil.getMeasure(fieldIndex, row));
          } else if (dataType == DataTypes.INT) {
            stream.writeInt((int) NonDictionaryUtil.getMeasure(fieldIndex, row));
          } else if (dataType == DataTypes.LONG) {
            stream.writeLong((long) NonDictionaryUtil.getMeasure(fieldIndex, row));
          } else if (dataType == DataTypes.DOUBLE) {
            stream.writeDouble((Double) NonDictionaryUtil.getMeasure(fieldIndex, row));
          } else if (DataTypes.isDecimal(dataType)) {
            byte[] bigDecimalInBytes = DataTypeUtil
                .bigDecimalToByte((BigDecimal) NonDictionaryUtil.getMeasure(fieldIndex, row));
            stream.writeInt(bigDecimalInBytes.length);
            stream.write(bigDecimalInBytes);
          } else {
            throw new IllegalArgumentException("unsupported data type:" + dataType);
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
    clear();
    try {
      CarbonUtil.deleteFiles(intermediateFiles);
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while deleting the intermediate files");
    }
  }

  private void clear() {
    if (recordHolderHeap != null) {
      SortTempFileChunkHolder sortTempFileChunkHolder;
      while (!recordHolderHeap.isEmpty()) {
        sortTempFileChunkHolder = recordHolderHeap.poll();
        if (null != sortTempFileChunkHolder) {
          sortTempFileChunkHolder.closeStream();
        }
      }
    }
    recordHolderHeap = null;
  }
}
