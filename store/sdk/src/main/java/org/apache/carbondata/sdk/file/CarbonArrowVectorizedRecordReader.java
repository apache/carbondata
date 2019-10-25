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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.sdk.file.arrow.ArrowConverter;
import org.apache.carbondata.sdk.file.arrow.ArrowFieldWriter;
import org.apache.carbondata.sdk.file.arrow.ArrowWriter;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * A specialized RecordReader that reads into CarbonColumnarBatches directly using the
 * arrow vectors API
 */
public class CarbonArrowVectorizedRecordReader extends AbstractRecordReader<Object> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonArrowVectorizedRecordReader.class.getName());

  private CarbonColumnarBatch carbonColumnarBatch;

  private QueryExecutor queryExecutor;

  private AbstractDetailQueryResultIterator iterator;

  private QueryModel queryModel;
  //This holds mapping of  fetch index with respect to project col index.
  // it is used when same col is used in projection many times.So need to fetch only that col.
  private List<Integer> projectionMapping = new ArrayList<>();

  private ArrowConverter arrowConverter;

  public CarbonArrowVectorizedRecordReader(QueryModel queryModel) {
    this.queryModel = queryModel;
  }

  @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    List<CarbonInputSplit> splitList;
    if (inputSplit instanceof CarbonInputSplit) {
      // Read the footer offset and set.
      CarbonInputSplit carbonInputSplit = ((CarbonInputSplit) inputSplit);
      String splitPath = carbonInputSplit.getFilePath();
      if ((null != carbonInputSplit.getDetailInfo()
          && carbonInputSplit.getDetailInfo().getBlockFooterOffset() == 0L) || (
          null == carbonInputSplit.getDetailInfo() && carbonInputSplit.getStart() == 0)) {
        FileReader reader = FileFactory.getFileHolder(FileFactory.getFileType(splitPath),
            taskAttemptContext.getConfiguration());
        ByteBuffer buffer = reader.readByteBuffer(FileFactory.getUpdatedFilePath(splitPath),
            ((CarbonInputSplit) inputSplit).getLength() - 8, 8);
        if (carbonInputSplit.getDetailInfo() == null) {
          carbonInputSplit.setStart(buffer.getLong());
        } else {
          carbonInputSplit.getDetailInfo().setBlockFooterOffset(buffer.getLong());
        }
        reader.finish();
      }
      splitList = new ArrayList<>(1);
      splitList.add((CarbonInputSplit) inputSplit);
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    List<TableBlockInfo> tableBlockInfoList = CarbonInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    queryModel.setVectorReader(true);
    try {
      queryExecutor =
          QueryExecutorFactory.getQueryExecutor(queryModel, taskAttemptContext.getConfiguration());
      iterator = (AbstractDetailQueryResultIterator) queryExecutor.execute(queryModel);
      initBatch((CarbonInputSplit) (inputSplit));
    } catch (QueryExecutionException e) {
      LOGGER.error(e);
      throw new InterruptedException(e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    return nextBatch();
  }

  private boolean nextBatch() {
    carbonColumnarBatch.reset();
    if (iterator.hasNext()) {
      iterator.processNextBatch(carbonColumnarBatch);
      return true;
    }
    return false;
  }

  private void initBatch(CarbonInputSplit carbonInputSplit) {
    if (carbonColumnarBatch == null) {

      arrowConverter = new ArrowConverter(queryModel, 0);
      ArrowWriter arrowWriter = arrowConverter.getArrowWriter();
      ArrowFieldWriter[] arrowFieldWriters = arrowWriter.getChildren();
      CarbonColumn[] projectionColumns = queryModel.getProjectionColumns();

      CarbonColumnVector[] vectors = new CarbonColumnVector[arrowWriter.getChildren().length];

      int rowCount = carbonInputSplit.getDetailInfo().getRowCount();

      Map<String, Integer> colmap = new HashMap<>();
      for (int i = 0; i < projectionColumns.length; i++) {
        vectors[i] = new CarbonArrowColumnVectorImpl(rowCount, projectionColumns[i].getDataType(),
            arrowFieldWriters[i]);
        if (colmap.containsKey(projectionColumns[i].getColName())) {
          int reusedIndex = colmap.get(projectionColumns[i].getColName());
          projectionMapping.add(reusedIndex);
        } else {
          colmap.put(projectionColumns[i].getColName(), i);
          projectionMapping.add(i);
        }
      }
      carbonColumnarBatch = new CarbonColumnarBatch(vectors, rowCount, new boolean[] {});
    }
  }

  // if same col is given in projection many time then below logic is used to scan only once
  // Ex. project cols=C1,C2,C3,C2 , projectionMapping holds[0,1,2,1]
  // Row will be formed based on projectionMapping.
  @Override public Object getCurrentValue() throws IOException, InterruptedException {
    // TODO: set row count and check use projection mapping
    for (CarbonColumnVector vector : carbonColumnarBatch.columnVectors) {
      vector.getData(0);
    }
    return null;
  }

  public ArrowConverter getArrowConverter() {
    return arrowConverter;
  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Operation not allowed on CarbonVectorizedReader");
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrieve.
    return 0;
  }

  @Override public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    if (carbonColumnarBatch != null) {
      carbonColumnarBatch = null;
    }
    if (iterator != null) {
      iterator.close();
    }
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }
}
