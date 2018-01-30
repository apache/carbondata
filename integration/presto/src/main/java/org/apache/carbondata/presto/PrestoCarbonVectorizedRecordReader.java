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

package org.apache.carbondata.presto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * carbondata column APIs and fills the data directly into columns.
 */
class PrestoCarbonVectorizedRecordReader extends AbstractRecordReader<Object> {

  private int batchIdx = 0;

  private int numBatched = 0;

  private CarbonVectorBatch columnarBatch;

  private CarbonColumnarBatch carbonColumnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  private QueryModel queryModel;

  private AbstractDetailQueryResultIterator iterator;

  private QueryExecutor queryExecutor;

  public PrestoCarbonVectorizedRecordReader(QueryExecutor queryExecutor, QueryModel queryModel, AbstractDetailQueryResultIterator iterator) {
    this.queryModel = queryModel;
    this.iterator = iterator;
    this.queryExecutor = queryExecutor;
    enableReturningBatches();
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
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
    queryModel.setVectorReader(true);
    try {
      queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
      iterator = (AbstractDetailQueryResultIterator) queryExecutor.execute(queryModel);
    } catch (QueryExecutionException e) {
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    if (columnarBatch != null) {
      columnarBatch = null;
    }
    // clear dictionary cache
    Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
    if (null != columnToDictionaryMapping) {
      for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
        CarbonUtil.clearDictionaryCache(entry.getValue());
      }
    }
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      rowCount += columnarBatch.numValidRows();
      return columnarBatch;
    } else {
      return null;
    }
  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrive.
    return 0;
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */

  private void initBatch() {
    List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
    StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
    for (int i = 0; i < queryDimension.size(); i++) {
      ProjectionDimension dim = queryDimension.get(i);
      if (dim.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
           generator.getReturnType());
      } else if (!dim.getDimension().hasEncoding(Encoding.DICTIONARY)) {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            dim.getDimension().getDataType());
      } else if (dim.getDimension().isComplex()) {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
           dim.getDimension().getDataType());
      } else {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            DataTypes.INT);
      }
    }

    for (ProjectionMeasure msr : queryMeasures) {
      DataType dataType = msr.getMeasure().getDataType();
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT || dataType == DataTypes.INT
          || dataType == DataTypes.LONG) {
        fields[msr.getOrdinal()] =
            new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
      } else if (DataTypes.isDecimal(dataType)) {
        fields[msr.getOrdinal()] =
            new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
      } else {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes.DOUBLE);
      }
    }

    columnarBatch = CarbonVectorBatch.allocate(fields);
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    boolean[] filteredRows = new boolean[columnarBatch.capacity()];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new CarbonColumnVectorWrapper(columnarBatch.column(i), filteredRows);
    }
    carbonColumnarBatch = new CarbonColumnarBatch(vectors, columnarBatch.capacity(), filteredRows);
  }


  private CarbonVectorBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /*
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  private void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch() {
    columnarBatch.reset();
    carbonColumnarBatch.reset();
    if (iterator.hasNext()) {
      iterator.processNextBatch(carbonColumnarBatch);
      int actualSize = carbonColumnarBatch.getActualSize();
      columnarBatch.setNumRows(actualSize);
      numBatched = actualSize;
      batchIdx = 0;
      return true;
    }
    return false;
  }


}
