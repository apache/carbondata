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

package org.apache.carbondata.spark.vectorreader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.spark.util.CarbonScalaUtil;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * carbondata column APIs and fills the data directly into columns.
 */
class VectorizedCarbonRecordReader extends AbstractRecordReader<Object> {

  private int batchIdx = 0;

  private int numBatched = 0;

  private ColumnarBatch columnarBatch;

  private CarbonColumnarBatch carbonColumnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The default config on whether columnarBatch should be offheap.
   */
  private static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.OFF_HEAP;

  private QueryModel queryModel;

  private AbstractDetailQueryResultIterator iterator;

  private QueryExecutor queryExecutor;

  public VectorizedCarbonRecordReader(QueryModel queryModel) {
    this.queryModel = queryModel;
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
      columnarBatch.close();
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
    }
    rowCount += 1;
    return columnarBatch.getRow(batchIdx - 1);
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

  private void initBatch(MemoryMode memMode) {
    List<QueryDimension> queryDimension = queryModel.getQueryDimension();
    List<QueryMeasure> queryMeasures = queryModel.getQueryMeasures();
    StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
    for (int i = 0; i < queryDimension.size(); i++) {
      QueryDimension dim = queryDimension.get(i);
      if (dim.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
            CarbonScalaUtil.convertCarbonToSparkDataType(generator.getReturnType()), true, null);
      } else if (!dim.getDimension().hasEncoding(Encoding.DICTIONARY)) {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
            CarbonScalaUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
            null);
      } else if (dim.getDimension().isComplex()) {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
            CarbonScalaUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
            null);
      } else {
        fields[dim.getQueryOrder()] = new StructField(dim.getColumnName(),
            CarbonScalaUtil.convertCarbonToSparkDataType(DataType.INT), true, null);
      }
    }

    for (int i = 0; i < queryMeasures.size(); i++) {
      QueryMeasure msr = queryMeasures.get(i);
      switch (msr.getMeasure().getDataType()) {
        case SHORT:
        case INT:
        case LONG:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
              CarbonScalaUtil.convertCarbonToSparkDataType(msr.getMeasure().getDataType()), true,
              null);
          break;
        case DECIMAL:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
              new DecimalType(msr.getMeasure().getPrecision(),
                  msr.getMeasure().getScale()), true, null);
          break;
        default:
          fields[msr.getQueryOrder()] = new StructField(msr.getColumnName(),
              CarbonScalaUtil.convertCarbonToSparkDataType(DataType.DOUBLE), true, null);
      }
    }

    columnarBatch = ColumnarBatch.allocate(new StructType(fields), memMode);
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      vectors[i] = new ColumnarVectorWrapper(columnarBatch.column(i));
    }
    carbonColumnarBatch = new CarbonColumnarBatch(vectors, columnarBatch.capacity());
  }

  private void initBatch() {
    initBatch(DEFAULT_MEMORY_MODE);
  }

  private ColumnarBatch resultBatch() {
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
