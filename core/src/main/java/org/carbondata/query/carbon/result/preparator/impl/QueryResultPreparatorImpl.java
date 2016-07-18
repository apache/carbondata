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
package org.carbondata.query.carbon.result.preparator.impl;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.CountAggregator;
import org.carbondata.query.aggregator.impl.DistinctCountAggregator;
import org.carbondata.query.aggregator.impl.DistinctStringCountAggregator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Below class will be used to get the result by converting to actual data
 * Actual data conversion can be converting the surrogate key to actual data
 *
 * @TODO there are many things in class which is very confusing, need to check
 * why it was handled like that and how we can handle that in a better
 * way.Need to revisit this class. IF aggregation is push down to spark
 * layer and if we can process the data in byte array format then this
 * class wont be useful so in future we can delete this class.
 * @TODO need to expose one interface which will return the result based on required type
 * for example its implementation case return converted result or directly result with out
 * converting to actual value
 */
public class QueryResultPreparatorImpl extends
    AbstractQueryResultPreparator<Map<ByteArrayWrapper, MeasureAggregator[]>, MeasureAggregator> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryResultPreparatorImpl.class.getName());

  public QueryResultPreparatorImpl(QueryExecutorProperties executerProperties,
      QueryModel queryModel) {
    super(executerProperties, queryModel);
  }

  @Override public BatchResult prepareQueryResult(
      Result<Map<ByteArrayWrapper, MeasureAggregator[]>, MeasureAggregator> scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      return new BatchResult();
    }
    QueryStatistic statistic = new QueryStatistic();
    List<QueryDimension> queryDimension = queryModel.getQueryDimension();
    int dimensionCount = queryDimension.size();
    int totalNumberOfColumn = dimensionCount + queryExecuterProperties.measureAggregators.length;
    Object[][] resultData = new Object[scannedResult.size()][totalNumberOfColumn];
    if (!queryExecuterProperties.isFunctionQuery && totalNumberOfColumn == 0
        && scannedResult.size() > 0) {
      return getEmptyChunkResult(scannedResult.size());
    }
    int currentRow = 0;
    long[] surrogateResult = null;
    int noDictionaryColumnIndex = 0;
    int complexTypeColumnIndex = 0;
    ByteArrayWrapper key = null;
    MeasureAggregator[] value = null;
    while (scannedResult.hasNext()) {
      key = scannedResult.getKey();
      value = scannedResult.getValue();
      surrogateResult = queryExecuterProperties.keyStructureInfo.getKeyGenerator()
          .getKeyArray(key.getDictionaryKey(),
              queryExecuterProperties.keyStructureInfo.getMaskedBytes());
      for (int i = 0; i < dimensionCount; i++) {
        if (!CarbonUtil
            .hasEncoding(queryDimension.get(i).getDimension().getEncoder(), Encoding.DICTIONARY)) {
          resultData[currentRow][i] = DataTypeUtil.getDataBasedOnDataType(
              new String(key.getNoDictionaryKeyByIndex(noDictionaryColumnIndex++),
                  Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)),
              queryDimension.get(i).getDimension().getDataType());
        } else if (CarbonUtil.hasDataType(queryDimension.get(i).getDimension().getDataType(),
            new DataType[] { DataType.ARRAY, DataType.STRUCT, DataType.MAP })) {
          resultData[currentRow][i] = queryExecuterProperties.complexDimensionInfoMap
              .get(queryDimension.get(i).getDimension().getOrdinal())
              .getDataBasedOnDataTypeFromSurrogates(
                  ByteBuffer.wrap(key.getComplexTypeByIndex(complexTypeColumnIndex++)));
        } else {
          resultData[currentRow][i] =
              (int) surrogateResult[queryDimension.get(i).getDimension().getKeyOrdinal()];
        }
      }

      // @TODO need to check why it was handled like this
      if (queryExecuterProperties.isFunctionQuery) {
        if (value[0].toString().contains("Long")) {
          Long sizeOfListL = value[0].getLongValue();
          return getEmptyChunkResult(sizeOfListL.intValue());
        } else if (value[0].toString().contains("Decimal")) {
          BigDecimal sizeOfListD = value[0].getBigDecimalValue();
          return getEmptyChunkResult(sizeOfListD.intValue());
        } else {
          Double sizeOfList = value[0].getDoubleValue();
          return getEmptyChunkResult(sizeOfList.intValue());
        }

      }
      for (int i = 0; i < queryExecuterProperties.measureAggregators.length; i++) {
        resultData[currentRow][dimensionCount + i] = value[i];
      }
      currentRow++;
      noDictionaryColumnIndex = 0;
      complexTypeColumnIndex = 0;
    }
    if (resultData.length > 0) {
      resultData = encodeToRows(resultData);
    }
    BatchResult result = getResult(queryModel, resultData);
    statistic.addStatistics("Time take to prepare query result of size " + resultData[0].length,
        System.currentTimeMillis());
    queryModel.getStatisticsRecorder().recordStatistics(statistic);
    return result;
  }

  private BatchResult getResult(QueryModel queryModel, Object[][] convertedResult) {
    Object[][] rows = new Object[convertedResult[0].length][];
    List<QueryDimension> queryDimensions = queryModel.getQueryDimension();
    int dimensionCount = queryDimensions.size();
    int msrCount = queryExecuterProperties.measureAggregators.length;
    int rowSize = convertedResult[0].length;
    Object[] row = null;
    for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
      row = new Object[dimensionCount + msrCount];
      fillDimensionData(convertedResult, queryDimensions, dimensionCount, row, rowIndex);
      MeasureAggregator[] msrAgg =
          new MeasureAggregator[queryExecuterProperties.measureAggregators.length];

      fillMeasureValueForAggGroupByQuery(queryModel, convertedResult, dimensionCount, rowIndex,
          msrAgg);
      fillDimensionAggValue(queryModel, convertedResult, dimensionCount, rowIndex, msrAgg);

      if (!queryModel.isDetailQuery()) {
        for (int i = 0; i < queryModel.getQueryMeasures().size(); i++) {
          row[queryModel.getQueryMeasures().get(i).getQueryOrder()] =
              msrAgg[queryExecuterProperties.measureStartIndex + i].get();
        }
        int index = 0;
        for (int i = 0; i < queryModel.getDimAggregationInfo().size(); i++) {
          DimensionAggregatorInfo dimensionAggregatorInfo =
              queryModel.getDimAggregationInfo().get(i);
          for (int j = 0; j < dimensionAggregatorInfo.getOrderList().size(); j++) {
            row[dimensionAggregatorInfo.getOrderList().get(j)] = msrAgg[index++].get();
          }
        }
        for (int i = 0; i < queryModel.getExpressions().size(); i++) {
          row[queryModel.getExpressions().get(i).getQueryOrder()] =
              ((MeasureAggregator) convertedResult[dimensionCount
                  + queryExecuterProperties.aggExpressionStartIndex + i][rowIndex]).get();
        }
      } else {
        QueryMeasure msr = null;
        for (int i = 0; i < queryModel.getQueryMeasures().size(); i++) {
          msr = queryModel.getQueryMeasures().get(i);
          if (msrAgg[queryExecuterProperties.measureStartIndex + i].isFirstTime()) {
            row[msr.getQueryOrder()] = null;
          } else {
            Object msrVal;
            switch (msr.getMeasure().getDataType()) {
              case INT:
              case LONG:
                msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getLongValue();
                break;
              case DECIMAL:
                msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getBigDecimalValue();
                break;
              default:
                msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getDoubleValue();
            }
            row[msr.getQueryOrder()] =
                DataTypeUtil.getMeasureDataBasedOnDataType(msrVal, msr.getMeasure().getDataType());
          }
        }
      }
      rows[rowIndex] = row;
    }
    LOGGER.info(
        "###########################################------ Total Number of records" + rowSize);
    BatchResult chunkResult = new BatchResult();
    chunkResult.setRows(rows);
    return chunkResult;
  }

  private void fillDimensionAggValue(QueryModel queryModel, Object[][] surrogateResult,
      int dimensionCount, int columnIndex, MeasureAggregator[] v) {
    Iterator<DimensionAggregatorInfo> dimAggInfoIterator =
        queryModel.getDimAggregationInfo().iterator();
    DimensionAggregatorInfo dimensionAggregatorInfo = null;
    List<String> partitionColumns = queryModel.getParitionColumns();
    int rowIndex = -1;
    int index = 0;
    while (dimAggInfoIterator.hasNext()) {
      dimensionAggregatorInfo = dimAggInfoIterator.next();
      for (int j = 0; j < dimensionAggregatorInfo.getAggList().size(); j++) {
        ++rowIndex;
        if (!dimensionAggregatorInfo.getAggList().get(j)
            .equals(CarbonCommonConstants.DISTINCT_COUNT)) {
          v[index++] =
              ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex]);
        } else if (partitionColumns.size() == 1 && partitionColumns
            .contains(dimensionAggregatorInfo.getColumnName()) && dimensionAggregatorInfo
            .getAggList().get(j).equals(CarbonCommonConstants.DISTINCT_COUNT)) {
          double value =
              ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex])
                  .getDoubleValue();

          MeasureAggregator countAggregator = new CountAggregator();
          countAggregator.setNewValue(value);
          v[index++] = countAggregator;
        } else {
          if (surrogateResult[dimensionCount
              + rowIndex][columnIndex] instanceof DistinctCountAggregator) {

            Iterator<Integer> iterator =
                ((DistinctCountAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex])
                    .getBitMap().iterator();

            MeasureAggregator distinctCountAggregatorObjct = new DistinctStringCountAggregator();
            while (iterator.hasNext()) {
              String member = queryExecuterProperties.columnToDictionayMapping
                  .get(dimensionAggregatorInfo.getDim().getColumnId())
                  .getDictionaryValueForKey(iterator.next());
              if (!member.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                distinctCountAggregatorObjct.agg(member);
              }
            }
            v[index++] = distinctCountAggregatorObjct;
          } else {
            v[index++] =
                ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex]);
          }
        }
      }
    }
  }

}
