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
package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.carbon.datastore.block.BlockletInfos;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.scan.executor.QueryExecutor;
import org.apache.carbondata.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.BatchResult;
import org.apache.carbondata.scan.result.iterator.ChunkRowIterator;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads the data from Carbon store.
 */
public class CarbonRecordReader<T> extends RecordReader<Void, T> {

  private QueryModel queryModel;

  private CarbonReadSupport<T> readSupport;

  private CarbonIterator<Object[]> carbonIterator;

  private QueryExecutor queryExecutor;

  public CarbonRecordReader(QueryModel queryModel, CarbonReadSupport<T> readSupport) {
    this.queryModel = queryModel;
    this.readSupport = readSupport;
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor();
  }

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    CarbonInputSplit carbonInputSplit = (CarbonInputSplit) split;
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<TableBlockInfo>();
    BlockletInfos blockletInfos = new BlockletInfos(carbonInputSplit.getNumberOfBlocklets(), 0,
        carbonInputSplit.getNumberOfBlocklets());
    tableBlockInfoList.add(
        new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
            carbonInputSplit.getSegmentId(), carbonInputSplit.getLocations(),
            carbonInputSplit.getLength(), blockletInfos));
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport
        .intialize(queryModel.getProjectionColumns(), queryModel.getAbsoluteTableIdentifier());
    try {
      carbonIterator =
          new ChunkRowIterator((CarbonIterator<BatchResult>) queryExecutor.execute(queryModel));
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
    return readSupport.readRow(carbonIterator.next());
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrive.
    return 0;
  }

  @Override public void close() throws IOException {
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
