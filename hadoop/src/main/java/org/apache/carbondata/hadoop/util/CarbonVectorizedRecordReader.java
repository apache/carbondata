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

package org.apache.carbondata.hadoop.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.AbstractDetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.hadoop.AbstractRecordReader;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * A specialized RecordReader that reads into CarbonColumnarBatches directly using the
 * carbondata column APIs and fills the data directly into columns.
 */
public class CarbonVectorizedRecordReader extends AbstractRecordReader<Object> {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonVectorizedRecordReader.class.getName());

  private CarbonColumnarBatch carbonColumnarBatch;

  private QueryExecutor queryExecutor;

  private int batchIdx = 0;

  private int numBatched = 0;

  private AbstractDetailQueryResultIterator iterator;

  private QueryModel queryModel;
  //This holds mapping of  fetch index with respect to project col index.
  // it is used when same col is used in projection many times.So need to fetch only that col.
  private List<Integer> projectionMapping = new ArrayList<>();


  public CarbonVectorizedRecordReader(QueryModel queryModel) {
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
        ByteBuffer buffer = reader
            .readByteBuffer(FileFactory.getUpdatedFilePath(splitPath),
                ((CarbonInputSplit) inputSplit).getLength() - 8,
                8);
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
      initBatch();
    } catch (QueryExecutionException e) {
      LOGGER.error(e);
      throw new InterruptedException(e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e);
      throw e;
    }
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }


  private boolean nextBatch() {
    carbonColumnarBatch.reset();
    if (iterator.hasNext()) {
      iterator.processNextBatch(carbonColumnarBatch);
      numBatched = carbonColumnarBatch.getActualSize();
      batchIdx = 0;
      return true;
    }
    return false;
  }

  private void initBatch() {
    if (carbonColumnarBatch == null) {
      List<ProjectionDimension> queryDimension = queryModel.getProjectionDimensions();
      List<ProjectionMeasure> queryMeasures = queryModel.getProjectionMeasures();
      StructField[] fields = new StructField[queryDimension.size() + queryMeasures.size()];
      for (ProjectionDimension dim : queryDimension) {
        fields[dim.getOrdinal()] =
            new StructField(dim.getColumnName(), dim.getDimension().getDataType());
      }
      for (ProjectionMeasure msr : queryMeasures) {
        DataType dataType = msr.getMeasure().getDataType();
        if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.SHORT
            || dataType == DataTypes.INT || dataType == DataTypes.LONG
            || dataType == DataTypes.FLOAT || dataType == DataTypes.BYTE
            || dataType == DataTypes.BINARY) {
          fields[msr.getOrdinal()] =
              new StructField(msr.getColumnName(), msr.getMeasure().getDataType());
        } else if (DataTypes.isDecimal(dataType)) {
          fields[msr.getOrdinal()] = new StructField(msr.getColumnName(),
              DataTypes.createDecimalType(msr.getMeasure().getPrecision(),
                  msr.getMeasure().getScale()));
        } else {
          fields[msr.getOrdinal()] = new StructField(msr.getColumnName(), DataTypes.DOUBLE);
        }
      }
      CarbonColumnVector[] vectors = new CarbonColumnVector[fields.length];

      Map<String, Integer> colmap = new HashMap<>();
      for (int i = 0; i < fields.length; i++) {
        vectors[i] = new CarbonColumnVectorImpl(
                CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT,
                fields[i].getDataType());
        if (colmap.containsKey(fields[i].getFieldName())) {
          int reusedIndex = colmap.get(fields[i].getFieldName());
          projectionMapping.add(reusedIndex);
        } else {
          colmap.put(fields[i].getFieldName(), i);
          projectionMapping.add(i);
        }
      }
      carbonColumnarBatch = new CarbonColumnarBatch(vectors,
          CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT,
          new boolean[] {});
    }
  }

  // if same col is given in projection many time then below logic is used to scan only once
  // Ex. project cols=C1,C2,C3,C2 , projectionMapping holds[0,1,2,1]
  // Row will be formed based on projectionMapping.
  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    rowCount += 1;
    Object[] row = new Object[projectionMapping.size()];
    for (int i = 0; i < projectionMapping.size(); i ++) {
      // if projectionMapping.get(i) <i it means row is fetched already
      if (projectionMapping.get(i) < i) {
        row[i] = row[projectionMapping.get(i)];
      } else {
        Object data = carbonColumnarBatch.columnVectors[projectionMapping.get(i)]
                .getData(batchIdx - 1);
        if (carbonColumnarBatch.columnVectors[i].getType() == DataTypes.STRING
                || carbonColumnarBatch.columnVectors[i].getType() == DataTypes.VARCHAR) {
          if (data == null) {
            row[i] = null;
          } else {
            row[i] = ByteUtil.toString((byte[]) data, 0, (((byte[]) data).length));
          }
        } else if (carbonColumnarBatch.columnVectors[i].getType() == DataTypes.BOOLEAN) {
          if (data == null) {
            row[i] = null;
          } else {
            row[i] = ByteUtil.toBoolean((byte) data);
          }
        } else {
          row[i] = carbonColumnarBatch.columnVectors[projectionMapping.get(i)]
                  .getData(batchIdx - 1);
        }
      }
    }
    return row;
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
