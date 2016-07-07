package org.carbondata.query.carbon.result.preparator.impl;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.model.QuerySchemaInfo;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;

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
    int msrSize = queryExecuterProperties.measureAggregators.length;
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

  @Override public BatchResult prepareQueryResult(
      Result<List<ListBasedResultWrapper>, Object> scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      BatchRawResult batchRawResult = new BatchRawResult();
      batchRawResult.setQuerySchemaInfo(querySchemaInfo);
      return batchRawResult;
    }
    int msrSize = queryExecuterProperties.measureAggregators.length;
    Object[][] resultData = new Object[scannedResult.size()][];
    Object[] value;
    Object[] row;
    int counter = 0;
    while (scannedResult.hasNext()) {
      value = scannedResult.getValue();
      row = new Object[msrSize + 1];
      row[0] = scannedResult.getKey();
      if(value != null) {
        System.arraycopy(value, 0, row, 1, msrSize);
      }
      resultData[counter] = row;
      counter ++;
    }
    LOGGER.info("###########################---- Total Number of records" + scannedResult.size());
    BatchRawResult result = new BatchRawResult();
    result.setRows(resultData);
    result.setQuerySchemaInfo(querySchemaInfo);
    return result;
  }

}

