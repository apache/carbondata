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

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.stats.TaskStatistics;
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

  private long taskId;

  private long queryStartTime;

  private CarbonPrestoDecodeReadSupport readSupport;

  public PrestoCarbonVectorizedRecordReader(QueryExecutor queryExecutor, QueryModel queryModel,
      AbstractDetailQueryResultIterator iterator, CarbonPrestoDecodeReadSupport readSupport) {
    this.queryModel = queryModel;
    this.iterator = iterator;
    this.queryExecutor = queryExecutor;
    this.readSupport = readSupport;
    enableReturningBatches();
    this.queryStartTime = System.currentTimeMillis();
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, UnsupportedOperationException {
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
    queryExecutor =
        QueryExecutorFactory.getQueryExecutor(queryModel, taskAttemptContext.getConfiguration());
    iterator = (AbstractDetailQueryResultIterator) queryExecutor.execute(queryModel);
  }

  @Override
  public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    if (columnarBatch != null) {
      columnarBatch = null;
    }
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      throw new IOException(e);
    }

    logStatistics(taskId, queryStartTime, queryModel.getStatisticsRecorder());
  }

  @Override
  public boolean nextKeyValue() {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) {
      rowCount += columnarBatch.numValidRows();
      return columnarBatch;
    } else {
      return null;
    }
  }

  @Override
  public Void getCurrentKey() {
    return null;
  }

  @Override
  public float getProgress() {
    // TODO : Implement it based on total number of rows it is going to retrieve.
    return 0;
  }

  public StructField fillChildFields(CarbonDimension dimension) {
    List<CarbonDimension> listOfChildDimensions =
            dimension.getListOfChildDimensions();
    ArrayList<StructField> childFields = null;
    if (listOfChildDimensions != null) {
      childFields = new ArrayList<StructField>();
      for (int i = 0; i < listOfChildDimensions.size(); i++) {
        childFields.add(fillChildFields(listOfChildDimensions.get(i)));
      }
    }
    return new StructField(dimension.getColName(), dimension.getDataType(), childFields);
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
      if (dim.getDimension().isComplex()) {
        List<CarbonDimension> childDimensions =
                dim.getDimension().getListOfChildDimensions();
        ArrayList<StructField> childFields = new ArrayList<StructField>();
        for (int index = 0; index < childDimensions.size(); index++) {
          childFields.add(fillChildFields(childDimensions.get(index)));
        }
        fields[dim.getOrdinal()] =
                new StructField(dim.getColumnName(), dim.getDimension().getDataType(),
                        childFields);
      } else if (dim.getDimension().getDataType() == DataTypes.DATE) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(), generator.getReturnType());
      } else {
        fields[dim.getOrdinal()] =
            new StructField(dim.getColumnName(), dim.getDimension().getDataType());
      }
    }

    for (ProjectionMeasure msr : queryMeasures) {
      fields[msr.getOrdinal()] =
          new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
    }

    columnarBatch =
        CarbonVectorBatch.allocate(fields, readSupport, queryModel.isDirectVectorFill());
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    boolean[] filteredRows = new boolean[columnarBatch.capacity()];
    for (int i = 0; i < fields.length; i++) {
      if (queryModel.isDirectVectorFill()) {
        vectors[i] = new ColumnarVectorWrapperDirect(columnarBatch.column(i));
      } else {
        vectors[i] = new CarbonColumnVectorWrapper(columnarBatch.column(i), filteredRows);
      }
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

  public CarbonVectorBatch getColumnarBatch() {
    return columnarBatch;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  /**
   * For Logging the Statistics
   * @param taskId
   * @param queryStartTime
   * @param recorder
   */
  private void  logStatistics(
      Long taskId,
      Long queryStartTime,
      QueryStatisticsRecorder recorder
  ) {
    if (null != recorder) {
      QueryStatistic queryStatistic = new QueryStatistic();
      queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
          System.currentTimeMillis() - queryStartTime);
      recorder.recordStatistics(queryStatistic);
      // print executor query statistics for each task_id
      TaskStatistics statistics = recorder.statisticsForTask(taskId, queryStartTime);
      recorder.logStatisticsForTask(statistics);
    }
  }

}
