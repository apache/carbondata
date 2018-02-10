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

package org.apache.carbondata.processing.loading.sort.unsafe.merger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.SortTempChunkHolder;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeSortTempFileChunkHolder;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;

public class UnsafeIntermediateFileMerger implements Callable<Void> {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeIntermediateFileMerger.class.getName());

  /**
   * recordHolderHeap
   */
  private AbstractQueue<SortTempChunkHolder> recordHolderHeap;

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
  private int complexCnt;
  private int measureCnt;
  private boolean[] isNoDictionaryDimensionColumn;
  private DataType[] measureDataTypes;
  private int writeBufferSize;
  private String compressorName;

  private long[] nullSetWords;

  private ByteBuffer rowData;

  private Throwable throwable;

  /**
   * IntermediateFileMerger Constructor
   */
  public UnsafeIntermediateFileMerger(SortParameters mergerParameters, File[] intermediateFiles,
      File outPutFile) {
    this.mergerParameters = mergerParameters;
    this.fileCounter = intermediateFiles.length;
    this.intermediateFiles = intermediateFiles;
    this.outPutFile = outPutFile;
    this.dimCnt = mergerParameters.getDimColCount();
    this.complexCnt = mergerParameters.getComplexDimColCount();
    this.measureCnt = mergerParameters.getMeasureColCount();
    this.isNoDictionaryDimensionColumn = mergerParameters.getNoDictionaryDimnesionColumn();
    this.measureDataTypes = mergerParameters.getMeasureDataType();
    this.writeBufferSize = mergerParameters.getBufferSize();
    this.compressorName = mergerParameters.getSortTempCompressorName();
    this.nullSetWords = new long[((measureCnt - 1) >> 6) + 1];
    // Take size of 2 MB for each row. I think it is high enough to use
    rowData = ByteBuffer.allocate(2 * 1024 * 1024);
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
      LOGGER.info("============================== Intermediate Merge of " + fileConterConst
          + " Sort Temp Files Cost Time: " + intermediateMergeCostTime + "(s)");
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
    SortTempChunkHolder poll = this.recordHolderHeap.poll();

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

    SortTempChunkHolder sortTempFileChunkHolder = null;

    for (File tempFile : intermediateFiles) {
      // create chunk holder
      sortTempFileChunkHolder = new UnsafeSortTempFileChunkHolder(tempFile, mergerParameters);

      sortTempFileChunkHolder.readRow();
      this.totalNumberOfRecords += sortTempFileChunkHolder.numberOfRows();

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
    this.recordHolderHeap = new PriorityQueue<SortTempChunkHolder>(listFiles.length);
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
  private void writeDataToFile(Object[] row) throws CarbonSortKeyAndGroupByException, IOException {
    int dimCount = 0;
    int size = 0;
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      if (isNoDictionaryDimensionColumn[dimCount]) {
        byte[] col = (byte[]) row[dimCount];
        rowData.putShort((short) col.length);
        size += 2;
        rowData.put(col);
        size += col.length;
      } else {
        rowData.putInt((int) row[dimCount]);
        size += 4;
      }
    }

    // write complex dimensions here.
    int dimensionSize = dimCnt + complexCnt;
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      rowData.putShort((short)col.length);
      size += 2;
      rowData.put(col);
      size += col.length;
    }
    Arrays.fill(nullSetWords, 0);
    int nullSetSize = nullSetWords.length * 8;
    int nullLoc = size;
    size += nullSetSize;
    for (int mesCount = 0; mesCount < measureCnt; mesCount++) {
      Object value = row[mesCount + dimensionSize];
      if (null != value) {
        DataType dataType = measureDataTypes[mesCount];
        if (dataType == DataTypes.SHORT) {
          rowData.putShort(size, (Short) value);
          size += 2;
        } else if (dataType == DataTypes.INT) {
          rowData.putInt(size, (Integer) value);
          size += 4;
        } else if (dataType == DataTypes.LONG) {
          rowData.putLong(size, (Long) value);
          size += 8;
        } else if (dataType == DataTypes.DOUBLE) {
          rowData.putDouble(size, (Double) value);
          size += 8;
        } else if (DataTypes.isDecimal(dataType)) {
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(((BigDecimal) value));
          rowData.putShort(size, (short) bigDecimalInBytes.length);
          size += 2;
          for (int i = 0; i < bigDecimalInBytes.length; i++) {
            rowData.put(size++, bigDecimalInBytes[i]);
          }
        }
        UnsafeCarbonRowPage.set(nullSetWords, mesCount);
      } else {
        UnsafeCarbonRowPage.unset(nullSetWords, mesCount);
      }
    }
    for (int i = 0; i < nullSetWords.length; i++) {
      rowData.putLong(nullLoc, nullSetWords[i]);
      nullLoc += 8;
    }
    byte[] rowBytes = new byte[size];
    rowData.position(0);
    rowData.get(rowBytes);
    stream.write(rowBytes);
    rowData.clear();
  }

  private void finish() throws CarbonSortKeyAndGroupByException {
    clear();
    try {
      CarbonUtil.deleteFiles(intermediateFiles);
      rowData.clear();
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while deleting the intermediate files");
    }
  }

  private void clear() {
    if (null != recordHolderHeap) {
      SortTempChunkHolder sortTempChunkHolder;
      while (!recordHolderHeap.isEmpty()) {
        sortTempChunkHolder = recordHolderHeap.poll();
        if (null != sortTempChunkHolder) {
          sortTempChunkHolder.close();
        }
      }
    }
  }
}
