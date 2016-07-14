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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
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
public class DetailQueryResultPreparatorImpl
    extends AbstractQueryResultPreparator<List<ListBasedResultWrapper>, Object> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DetailQueryResultPreparatorImpl.class.getName());

  public DetailQueryResultPreparatorImpl(QueryExecutorProperties executerProperties,
      QueryModel queryModel) {
    super(executerProperties, queryModel);
  }

  @Override public BatchResult prepareQueryResult(
      Result<List<ListBasedResultWrapper>, Object> scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      return new BatchResult();
    }
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
    Object[] value = null;
    while (scannedResult.hasNext()) {
      key = scannedResult.getKey();
      value = scannedResult.getValue();
      if (key != null) {
        surrogateResult = queryExecuterProperties.keyStructureInfo.getKeyGenerator()
            .getKeyArray(key.getDictionaryKey(),
                queryExecuterProperties.keyStructureInfo.getMaskedBytes());
        for (int i = 0; i < dimensionCount; i++) {
          if (!CarbonUtil.hasEncoding(queryDimension.get(i).getDimension().getEncoder(),
              Encoding.DICTIONARY)) {
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
      }
      if (value != null) {
        System.arraycopy(value, 0, resultData[currentRow], dimensionCount,
            queryExecuterProperties.measureAggregators.length);
      }
      currentRow++;
      noDictionaryColumnIndex = 0;
      complexTypeColumnIndex = 0;
    }
    if (resultData.length > 0) {
      resultData = encodeToRows(resultData);
    }
    return getResult(queryModel, resultData);
  }

  private BatchResult getResult(QueryModel queryModel, Object[][] convertedResult) {

    int rowSize = convertedResult[0].length;
    Object[][] rows = new Object[rowSize][];
    List<QueryDimension> queryDimensions = queryModel.getQueryDimension();
    int dimensionCount = queryDimensions.size();
    int msrCount = queryExecuterProperties.measureAggregators.length;
    Object[] row;
    for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
      row = new Object[dimensionCount + msrCount];
      fillDimensionData(convertedResult, queryDimensions, dimensionCount, row, rowIndex);

      QueryMeasure msr;
      for (int i = 0; i < queryModel.getQueryMeasures().size(); i++) {
        msr = queryModel.getQueryMeasures().get(i);
        row[msr.getQueryOrder()] = convertedResult[dimensionCount + i][rowIndex];
      }
      rows[rowIndex] = row;
    }
    LOGGER.info(
        "###########################################------ Total Number of records" + rowSize);
    BatchResult chunkResult = new BatchResult();
    chunkResult.setRows(rows);
    return chunkResult;
  }
}
