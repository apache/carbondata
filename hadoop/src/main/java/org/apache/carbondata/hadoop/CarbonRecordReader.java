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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.conf.Configuration;
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

  /**
   * Whether to clear datamap when reader is closed. In some scenarios such as datamap rebuild,
   * we will set it to true and will clear the datamap after rebuild
   */
  private boolean skipClearDataMapAtClose = false;

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport,
      InputMetricsStats inputMetricsStats, Configuration configuration) {
    this(queryModel, readSupport, configuration);
    this.inputMetricsStats = inputMetricsStats;
  }

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport,
      Configuration configuration) {
    this.queryModel = queryModel;
    this.readSupport = readSupport;
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel, configuration);
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // The input split can contain single HDFS block or multiple blocks, so firstly get all the
    // blocks and then set them in the query model.
    List<CarbonInputSplit> splitList;
    if (inputSplit instanceof CarbonInputSplit) {
      splitList = new ArrayList<>(1);
      CarbonInputSplit carbonInputSplit = ((CarbonInputSplit) inputSplit);
      String splitPath = carbonInputSplit.getFilePath();
      // BlockFooterOffSet will be null in case of CarbonVectorizedReader as this has to be set
      // where multiple threads are able to read small set of files to calculate footer instead
      // of the main thread setting this for all the files.
      if ((null != carbonInputSplit.getDetailInfo()
          && carbonInputSplit.getDetailInfo().getBlockFooterOffset() == 0L) || (
          null == carbonInputSplit.getDetailInfo() && carbonInputSplit.getStart() == 0)) {
        FileReader reader = FileFactory.getFileHolder(FileFactory.getFileType(splitPath),
            context.getConfiguration());
        ByteBuffer buffer = reader
            .readByteBuffer(FileFactory.getUpdatedFilePath(splitPath), inputSplit.getLength() - 8,
                8);
        if (carbonInputSplit.getDetailInfo() == null) {
          carbonInputSplit.setStart(buffer.getLong());
        } else {
          carbonInputSplit.getDetailInfo().setBlockFooterOffset(buffer.getLong());
        }
        reader.finish();
      }
      splitList.add((CarbonInputSplit) inputSplit);
    } else if (inputSplit instanceof CarbonMultiBlockSplit) {
      // contains multiple blocks, this is an optimization for concurrent query.
      CarbonMultiBlockSplit multiBlockSplit = (CarbonMultiBlockSplit) inputSplit;
      splitList = multiBlockSplit.getAllSplits();
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    // It should use the exists tableBlockInfos if tableBlockInfos of queryModel is not empty
    // otherwise the prune is no use before this method
    if (!queryModel.isFG()) {
      List<TableBlockInfo> tableBlockInfoList = CarbonInputSplit.createBlocks(splitList);
      queryModel.setTableBlockInfos(tableBlockInfoList);
    }
    readSupport.initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
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

  /**
   * get batch result
   *
   * @return rows
   */
  public List<Object[]> getBatchValue() {
    if (null != inputMetricsStats) {
      inputMetricsStats.incrementRecordRead(1L);
    }
    List<Object[]> objects = ((ChunkRowIterator) carbonIterator).nextBatch();
    rowCount += objects.size();
    return objects;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrieve.
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
    if (!skipClearDataMapAtClose) {
      // Clear the datamap cache
      DataMapStoreManager.getInstance().clearDataMaps(
          queryModel.getTable().getAbsoluteTableIdentifier(), false);
    }
    // close read support
    readSupport.close();
    carbonIterator.close();
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }

  public void setSkipClearDataMapAtClose(boolean skipClearDataMapAtClose) {
    this.skipClearDataMapAtClose = skipClearDataMapAtClose;
  }
}
