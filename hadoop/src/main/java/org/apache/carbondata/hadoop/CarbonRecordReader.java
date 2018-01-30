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
package org.apache.carbondata.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads the data from Carbon store.
 */
public class CarbonRecordReader<T> extends AbstractRecordReader<T> {

  protected QueryModel queryModel;

  protected CarbonReadSupport<T> readSupport;

  protected CarbonIterator<Object[]> carbonIterator;

  protected QueryExecutor queryExecutor;
  private InputMetricsStats inputMetricsStats;

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport,
      InputMetricsStats inputMetricsStats) {
    this(queryModel, readSupport);
    this.inputMetricsStats = inputMetricsStats;
  }

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport) {
    this.queryModel = queryModel;
    this.readSupport = readSupport;
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // The input split can contain single HDFS block or multiple blocks, so firstly get all the
    // blocks and then set them in the query model.
    List<CarbonInputSplit> splitList;
    if (inputSplit instanceof CarbonInputSplit) {
      splitList = new ArrayList<>(1);
      splitList.add((CarbonInputSplit) inputSplit);
    } else if (inputSplit instanceof CarbonMultiBlockSplit) {
      // contains multiple blocks, this is an optimization for concurrent query.
      CarbonMultiBlockSplit multiBlockSplit = (CarbonMultiBlockSplit) inputSplit;
      splitList = multiBlockSplit.getAllSplits();
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    List<TableBlockInfo> tableBlockInfoList = CarbonInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  public void initializeForFileLevelRead(String filePath)
      throws IOException, InterruptedException {
    CarbonFile file = FileFactory.getCarbonFile(filePath);
    long fileLength = file.getSize();
    long footerOffset = getFooterOffset(filePath, fileLength);

    TableBlockInfo blockInfo = new TableBlockInfo(
        filePath, "0", footerOffset, "0", new String[]{}, fileLength,
        null, ColumnarFormatVersion.V3, new String[]{});

    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>(1);
    tableBlockInfoList.add(blockInfo);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  private long getFooterOffset(String filePath, long fileLength) throws IOException {
    DataInputStream dataInputStream = null;
    try {
      dataInputStream = FileFactory.getDataInputStream(filePath, FileFactory.getFileType(filePath));
      long offset = fileLength - 8;
      if (dataInputStream.skipBytes((int) (offset)) != offset) {
        throw new IOException("failed to skip to footer in file: " + filePath);
      }
      long footerOffset = dataInputStream.readLong();
      if (footerOffset >= fileLength) {
        throw new IOException("corrupted data file (" + filePath + "), footer offset: " +
            footerOffset + ", file length: " + fileLength);
      }
      return footerOffset;
    } finally {
      if (dataInputStream != null) {
        dataInputStream.close();
      }
    }
  }

  @Override public boolean nextKeyValue() {
    return carbonIterator.hasNext();

  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public T getCurrentValue() throws IOException, InterruptedException {
    rowCount += 1;
    if (null != inputMetricsStats) {
      inputMetricsStats.incrementRecordRead(1L);
    }
    return readSupport.readRow(carbonIterator.next());
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrive.
    return 0;
  }

  @Override public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    // clear dictionary cache
    Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
    if (null != columnToDictionaryMapping) {
      for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
        CarbonUtil.clearDictionaryCache(entry.getValue());
      }
    }
    // close read support
    readSupport.close();
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }
}
