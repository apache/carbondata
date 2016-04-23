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
package org.carbondata.query.carbon.executor.impl;

import java.util.Arrays;
import java.util.Iterator;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.BlockIndexStore;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.query.aggregator.util.MeasureAggregatorFactory;
import org.carbondata.query.carbon.executor.QueryExecutor;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;
import org.carbondata.query.carbon.executor.util.QueryUtil;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryModel;

/**
 * This class provides a skeletal implementation of the {@link QueryExecutor}
 * interface to minimize the effort required to implement this interface. This
 * will be used to prepare all the properties required for query execution
 */
public abstract class AbstractQueryExecutor implements QueryExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractQueryExecutor.class.getName());
  /**
   * holder for query properties which will be used to execute the query
   */
  protected QueryExecutorProperties queryProperties;

  public AbstractQueryExecutor() {
    queryProperties = new QueryExecutorProperties();
  }

  /**
   * Below method will be used to fill the executor properties based on query
   * model it will parse the query model and get the detail and fill it in
   * query properties
   *
   * @param queryModel
   */
  protected void initQuery(QueryModel queryModel) throws QueryExecutionException {
    /**
     * setting the table unique name
     */
    queryProperties.tableUniqueName =
        queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getDatabaseName() + '_'
            + queryModel.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableName();

    // get the table blocks
    try {
      queryProperties.dataBlocks = BlockIndexStore.getInstance()
          .loadAndGetBlocks(queryModel.getTableBlockInfos(),
              queryModel.getAbsoluteTableIdentifier());
    } catch (IndexBuilderException e) {
      throw new QueryExecutionException(e);
    }
    //
    // // updating the restructuring infos for the query
    queryProperties.keyStructureInfo = getKeyStructureInfo(queryModel,
        queryProperties.dataBlocks.get(queryProperties.dataBlocks.size() - 1).getSegmentProperties()
            .getDimensionKeyGenerator());

    // calculating the total number of aggeragted columns
    int aggTypeCount = queryModel.getQueryMeasures().size() + queryModel.getExpressions().size();

    // as in one dimension multiple aggregator can be selected , so we need
    // to select all the aggregator function
    Iterator<DimensionAggregatorInfo> iterator = queryModel.getDimAggregationInfo().iterator();
    while (iterator.hasNext()) {
      aggTypeCount += iterator.next().getAggList().size();
    }
    int currentIndex = 0;
    String[] aggTypes = new String[aggTypeCount];
    DataType[] dataTypes = new DataType[aggTypeCount];

    for (CarbonMeasure carbonMeasure : queryModel.getQueryMeasures()) {
      // adding the data type and aggregation type of all the measure this
      // can be used
      // to select the aggregator
      aggTypes[currentIndex] = carbonMeasure.getAggregateFunction();
      dataTypes[currentIndex] = carbonMeasure.getDataType();
    }

    if (queryModel.isDetailQuery()) {
      Arrays.fill(aggTypes, CarbonCommonConstants.DUMMY);
    }
    if (!queryModel.isDetailQuery()) {
      // adding query dimension selected in aggregation info
      for (DimensionAggregatorInfo dimensionAggregationInfo : queryModel.getDimAggregationInfo()) {
        for (int i = 0; i < dimensionAggregationInfo.getAggList().size(); i++) {
          aggTypes[currentIndex] = dimensionAggregationInfo.getAggList().get(i);
          dataTypes[currentIndex] = dimensionAggregationInfo.getDim().getDataType();
        }
      }
      // filling the query expression data type and its aggregator
      // in this case both will be custom
      for (int i = 0; i < queryModel.getExpressions().size(); i++) {
        aggTypes[currentIndex] = CarbonCommonConstants.CUSTOM;
        dataTypes[currentIndex] = DataType.STRING;
      }
    }
    queryProperties.measureAggregators = MeasureAggregatorFactory
        .getMeassureAggregator(aggTypes, dataTypes, queryModel.getExpressions());
    // as aggregation will be executed in following order
    // 1.aggregate dimension expression
    // 2. expression
    // 3. query measure
    // so calculating the index of the expression start index
    // and measure column start index
    queryProperties.aggExpressionStartIndex =
        aggTypes.length - queryModel.getExpressions().size() - queryModel.getQueryMeasures().size();
    queryProperties.measureStartIndex = aggTypes.length - queryModel.getQueryMeasures().size();

    // dictionary column unique column id to dictionary mapping
    // which will be used to get column actual data

    queryProperties.columnToDictionayMapping = QueryUtil
        .getDimensionDictionaryDetail(queryModel.getQueryDimension(),
            queryModel.getDimAggregationInfo(), queryModel.getExpressions(),
            queryModel.getAbsoluteTableIdentifier());

  }

  /**
   * Below method will be used to get the key structure info for the uqery
   *
   * @param queryModel   query model
   * @param keyGenerator
   * @return key structure info
   */
  private KeyStructureInfo getKeyStructureInfo(QueryModel queryModel, KeyGenerator keyGenerator) {
    // getting the masked byte range for dictionary column
    int[] maskByteRanges =
        QueryUtil.getMaskedByteRange(queryModel.getQueryDimension(), keyGenerator);

    // getting the masked bytes for query dimension dictionary column
    int[] maskedBytes = QueryUtil.getMaksedByte(keyGenerator.getKeySizeInBytes(), maskByteRanges);

    // max key for the dictionary dimension present in the query
    byte[] maxKey = null;
    try {
      // getting the max key which will be used to masked and get the
      // masked key
      maxKey = QueryUtil.getMaxKeyBasedOnDimensions(queryModel.getQueryDimension(), keyGenerator);
    } catch (KeyGenException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e, "problem while getting the max key");
    }

    KeyStructureInfo restructureInfos = new KeyStructureInfo();
    restructureInfos.setKeyGenerator(keyGenerator);
    restructureInfos.setMaskByteRanges(maskByteRanges);
    restructureInfos.setMaskedBytes(maskedBytes);
    restructureInfos.setMaxKey(maxKey);
    return restructureInfos;
  }

}
