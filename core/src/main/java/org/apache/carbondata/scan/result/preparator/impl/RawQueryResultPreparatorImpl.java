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
package org.apache.carbondata.scan.result.preparator.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.scan.executor.impl.QueryExecutorProperties;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryMeasure;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.model.QuerySchemaInfo;
import org.apache.carbondata.scan.result.BatchRawResult;
import org.apache.carbondata.scan.result.BatchResult;
import org.apache.carbondata.scan.result.ListBasedResultWrapper;
import org.apache.carbondata.scan.result.Result;
import org.apache.carbondata.scan.wrappers.ByteArrayWrapper;

/**
 * It does not decode the dictionary.
 */
public class RawQueryResultPreparatorImpl
    extends AbstractQueryResultPreparator<List<ListBasedResultWrapper>, Object> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RawQueryResultPreparatorImpl.class.getName());

  private QuerySchemaInfo querySchemaInfo;

  public RawQueryResultPreparatorImpl(QueryExecutorProperties executerProperties,
                                      QueryModel queryModel) {
    super(executerProperties, queryModel);
    querySchemaInfo = new QuerySchemaInfo();
    querySchemaInfo.setKeyGenerator(queryExecuterProperties.keyStructureInfo.getKeyGenerator());
    querySchemaInfo.setMaskedByteIndexes(queryExecuterProperties.keyStructureInfo.getMaskedBytes());
    querySchemaInfo.setQueryDimensions(queryModel.getQueryDimension()
        .toArray(new QueryDimension[queryModel.getQueryDimension().size()]));
    querySchemaInfo.setQueryMeasures(queryModel.getQueryMeasures()
        .toArray(new QueryMeasure[queryModel.getQueryMeasures().size()]));
    int msrSize = queryExecuterProperties.measureDataTypes.length;
    int dimensionCount = queryModel.getQueryDimension().size();
    int[] queryOrder = new int[dimensionCount + msrSize];
    int[] queryReverseOrder = new int[dimensionCount + msrSize];
    for (int i = 0; i < dimensionCount; i++) {
      queryOrder[queryModel.getQueryDimension().get(i).getQueryOrder()] = i;
      queryReverseOrder[i] = queryModel.getQueryDimension().get(i).getQueryOrder();
    }
    for (int i = 0; i < msrSize; i++) {
      queryOrder[queryModel.getQueryMeasures().get(i).getQueryOrder()] = i + dimensionCount;
      queryReverseOrder[i + dimensionCount] = queryModel.getQueryMeasures().get(i).getQueryOrder();
    }
    querySchemaInfo.setQueryOrder(queryOrder);
    querySchemaInfo.setQueryReverseOrder(queryReverseOrder);
  }

  @Override
  public BatchResult prepareQueryResult(
      Result<List<ListBasedResultWrapper>, Object> scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      return new BatchRawResult();
    }
    QueryDimension[] queryDimensions = querySchemaInfo.getQueryDimensions();
    int msrSize = queryExecuterProperties.measureDataTypes.length;
    int dimSize = queryDimensions.length;
    int[] order = querySchemaInfo.getQueryReverseOrder();
    Object[][] resultData = new Object[scannedResult.size()][];
    Object[] value;
    Object[] row;
    int counter = 0;
    if (queryModel.isRawBytesDetailQuery()) {
      while (scannedResult.hasNext()) {
        value = scannedResult.getValue();
        row = new Object[msrSize + 1];
        row[0] = scannedResult.getKey();
        if (value != null) {
          assert (value.length == msrSize);
          System.arraycopy(value, 0, row, 1, msrSize);
        }
        resultData[counter] = row;
        counter++;
      }
    } else {
      while (scannedResult.hasNext()) {
        value = scannedResult.getValue();
        row = new Object[msrSize + dimSize];
        ByteArrayWrapper key = scannedResult.getKey();
        if (key != null) {
          long[] surrogateResult = querySchemaInfo.getKeyGenerator()
              .getKeyArray(key.getDictionaryKey(), querySchemaInfo.getMaskedByteIndexes());
          int noDictionaryColumnIndex = 0;
          int complexTypeColumnIndex = 0;
          for (int i = 0; i < dimSize; i++) {
            if (!queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
              row[order[i]] = DataTypeUtil.getDataBasedOnDataType(
                  new String(key.getNoDictionaryKeyByIndex(noDictionaryColumnIndex++)),
                  queryDimensions[i].getDimension().getDataType());
            } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
              DirectDictionaryGenerator directDictionaryGenerator =
                  DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
                      queryDimensions[i].getDimension().getDataType());
              if (directDictionaryGenerator != null) {
                row[order[i]] = directDictionaryGenerator.getValueFromSurrogate(
                    (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()]);
              }
            } else if (CarbonUtil
                .hasComplexDataType(queryDimensions[i].getDimension().getDataType())) {
              row[order[i]] = queryExecuterProperties.complexDimensionInfoMap
                  .get(queryDimensions[i].getDimension().getOrdinal())
                  .getDataBasedOnDataTypeFromSurrogates(
                      ByteBuffer.wrap(key.getComplexTypeByIndex(complexTypeColumnIndex++)));
            } else {
              row[order[i]] =
                  (int) surrogateResult[queryDimensions[i].getDimension().getKeyOrdinal()];
            }
          }
        }
        for (int i = 0; i < msrSize; i++) {
          row[order[i + queryDimensions.length]] = value[i];
        }
        resultData[counter] = row;
        counter++;
      }
    }

    LOGGER.info("###########################---- Total Number of records" + scannedResult.size());
    BatchRawResult result = new BatchRawResult();
    result.setRows(resultData);
    return result;
  }

}

