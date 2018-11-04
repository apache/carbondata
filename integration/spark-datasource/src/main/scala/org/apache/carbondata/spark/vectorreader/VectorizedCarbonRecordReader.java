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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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
import org.apache.carbondata.hadoop.InputMetricsStats;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * carbondata column APIs and fills the data directly into columns.
 */
public class VectorizedCarbonRecordReader extends AbstractRecordReader<Object> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(VectorizedCarbonRecordReader.class.getName());

  private int batchIdx = 0;

  private static final int DEFAULT_BATCH_SIZE = 4 * 1024;

  private int numBatched = 0;

  private CarbonVectorProxy vectorProxy;

  private CarbonColumnarBatch carbonColumnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  private boolean[] isNoDictStringField;

  /**
   * The default config on whether columnarBatch should be onheap.
   */
  private static final MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;

  private QueryModel queryModel;

  private AbstractDetailQueryResultIterator iterator;

  private QueryExecutor queryExecutor;

  private InputMetricsStats inputMetricsStats;

  public VectorizedCarbonRecordReader(QueryModel queryModel, InputMetricsStats inputMetricsStats,
      String enableBatch) {
    this.queryModel = queryModel;
    this.inputMetricsStats = inputMetricsStats;
    if (enableBatch.equals("true")) {
      enableReturningBatches();
    }
  }


  /*
 * Can be called before any rows are returned to enable returning columnar batches directly.
 */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
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
      queryExecutor =
          QueryExecutorFactory.getQueryExecutor(queryModel, taskAttemptContext.getConfiguration());
      iterator = (AbstractDetailQueryResultIterator) queryExecutor.execute(queryModel);
    } catch (QueryExecutionException e) {
      if (ExceptionUtils.indexOfThrowable(e, FileNotFoundException.class) > 0) {
        LOGGER.error(e);
        throw new InterruptedException(
            "Insert overwrite may be in progress.Please check " + e.getMessage());
      }
      throw new InterruptedException(e.getMessage());
    } catch (Exception e) {
      if (ExceptionUtils.indexOfThrowable(e, FileNotFoundException.class) > 0) {
        LOGGER.error(e);
        throw new InterruptedException(
            "Insert overwrite may be in progress.Please check " + e.getMessage());
      }
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    logStatistics(rowCount, queryModel.getStatisticsRecorder());
    if (vectorProxy != null) {
      vectorProxy.close();
      vectorProxy = null;
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

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    resultBatch();

    if (returnColumnarBatch) {
      return nextBatch();
    }

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      int value = vectorProxy.numRows();
      rowCount += value;
      if (inputMetricsStats != null) {
        inputMetricsStats.incrementRecordRead((long) value);
      }
      return vectorProxy.getColumnarBatch();
    }
    rowCount += 1;
    return vectorProxy.getRow(batchIdx - 1);
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO : Implement it based on total number of rows it is going to retrieve.
    return 0;
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */

  public void initBatch(MemoryMode memMode, StructType partitionColumns,
      InternalRow partitionValues) {
    List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
    List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
    StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
    this.isNoDictStringField = new boolean[queryDimension.size() + queryMeasures.size()];
    for (int i = 0; i < queryDimension.size(); i++) {
      ProjectionDimension dim = queryDimension.get(i);
      if (dim.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dim.getDimension().getDataType());
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(generator.getReturnType()), true, null);
      } else if (!dim.getDimension().hasEncoding(Encoding.DICTIONARY)) {
        if (dim.getDimension().getDataType() == DataTypes.STRING
            || dim.getDimension().getDataType() == DataTypes.VARCHAR || dim.getDimension()
            .getColumnSchema().isLocalDictColumn()) {
          this.isNoDictStringField[dim.getOrdinal()] = true;
        }
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
            null);
      } else if (dim.getDimension().isComplex()) {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(dim.getDimension().getDataType()), true,
            null);
      } else {
        fields[dim.getOrdinal()] = new StructField(dim.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(DataTypes.INT), true, null);
      }
    }

    for (int i = 0; i < queryMeasures.size(); i++) {
      ProjectionMeasure msr = queryMeasures.get(i);
      DataType dataType = msr.getMeasure().getDataType();
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT || dataType == DataTypes.INT
          || dataType == DataTypes.LONG || dataType == DataTypes.FLOAT
          || dataType == DataTypes.BYTE) {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(msr.getMeasure().getDataType()), true,
            null);
      } else if (DataTypes.isDecimal(dataType)) {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(),
            new DecimalType(msr.getMeasure().getPrecision(), msr.getMeasure().getScale()), true,
            null);
      } else {
        fields[msr.getOrdinal()] = new StructField(msr.getColumnName(),
            CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(DataTypes.DOUBLE), true, null);
      }
    }
    StructType schema = new StructType(fields);
    if (partitionColumns != null) {
      for (StructField field : partitionColumns.fields()) {
        schema = schema.add(field);
      }
    }
    short batchSize = DEFAULT_BATCH_SIZE;
    if (queryModel.isDirectVectorFill()) {
      batchSize = CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    }
    vectorProxy = new CarbonVectorProxy(DEFAULT_MEMORY_MODE, schema, batchSize);

    if (partitionColumns != null) {
      int partitionIdx = fields.length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(vectorProxy.column(i + partitionIdx), partitionValues, i);
        vectorProxy.column(i + partitionIdx).setIsConstant();
      }
    }
    CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];
    boolean[] filteredRows = null;
    if (queryModel.isDirectVectorFill()) {
      for (int i = 0; i < fields.length; i++) {
        vectors[i] = new ColumnarVectorWrapperDirect(vectorProxy, i);
        if (isNoDictStringField[i]) {
          ((ColumnarVectorWrapperDirect) vectors[i]).reserveDictionaryIds();
        }
      }
    } else {
      filteredRows = new boolean[vectorProxy.numRows()];
      for (int i = 0; i < fields.length; i++) {
        vectors[i] = new ColumnarVectorWrapper(vectorProxy, filteredRows, i);
        if (isNoDictStringField[i]) {
          if (vectors[i] instanceof ColumnarVectorWrapper) {
            ((ColumnarVectorWrapper) vectors[i]).reserveDictionaryIds();
          }
        }
      }
    }
    carbonColumnarBatch = new CarbonColumnarBatch(vectors, vectorProxy.numRows(), filteredRows);
  }

  private void initBatch() {
    initBatch(DEFAULT_MEMORY_MODE, new StructType(), InternalRow.empty());
  }

  private void resultBatch() {
    if (vectorProxy == null) initBatch();
  }



  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch() {
    if (null != isNoDictStringField) {
      for (int i = 0; i < isNoDictStringField.length; i++) {
        if (isNoDictStringField[i]) {
          vectorProxy.resetDictionaryIds(i);
        }
      }
    }
    vectorProxy.reset();
    carbonColumnarBatch.reset();
    if (iterator.hasNext()) {
      iterator.processNextBatch(carbonColumnarBatch);
      int actualSize = carbonColumnarBatch.getActualSize();
      vectorProxy.setNumRows(actualSize);
      numBatched = actualSize;
      batchIdx = 0;
      return true;
    }
    return false;
  }

}
