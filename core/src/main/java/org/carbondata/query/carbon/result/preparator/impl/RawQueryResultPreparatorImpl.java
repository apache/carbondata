package org.carbondata.query.carbon.result.preparator.impl;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.model.QuerySchemaInfo;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * It does not decode the dictionary.
 */
public class RawQueryResultPreparatorImpl extends AbstractQueryResultPreparator<BatchRawResult> {

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

  @Override public BatchRawResult prepareQueryResult(Result scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      BatchRawResult batchRawResult = new BatchRawResult(new Object[0][0]);
      batchRawResult.setQuerySchemaInfo(querySchemaInfo);
      return batchRawResult;
    }
    int msrSize = queryExecuterProperties.measureAggregators.length;
    int totalNumberOfColumn = msrSize + 1;
    Object[][] resultData = new Object[scannedResult.size()][totalNumberOfColumn];
    int currentRow = 0;
    ByteArrayWrapper key = null;
    MeasureAggregator[] value = null;
    while (scannedResult.hasNext()) {
      key = scannedResult.getKey();
      value = scannedResult.getValue();
      resultData[currentRow][0] = key;
      for (int i = 0; i < msrSize; i++) {
        resultData[currentRow][1 + i] = value[i];
      }
      currentRow++;
    }

    if (resultData.length > 0) {
      resultData = encodeToRows(resultData);
    }
    BatchRawResult result = getResult(queryModel, resultData);
    result.setQuerySchemaInfo(querySchemaInfo);
    return result;
  }


  private BatchRawResult getResult(QueryModel queryModel, Object[][] convertedResult) {

    int msrCount = queryExecuterProperties.measureAggregators.length;
    Object[][] resultDataA = new Object[1 + msrCount][convertedResult[0].length];

    for (int columnIndex = 0; columnIndex < resultDataA[0].length; columnIndex++) {
      resultDataA[0][columnIndex] = convertedResult[0][columnIndex];
      MeasureAggregator[] msrAgg =
          new MeasureAggregator[queryExecuterProperties.measureAggregators.length];

      fillMeasureValueForAggGroupByQuery(queryModel, convertedResult, 1, columnIndex, msrAgg);

      QueryMeasure msr = null;
      for (int i = 0; i < queryModel.getQueryMeasures().size(); i++) {
        msr = queryModel.getQueryMeasures().get(i);
        if (msrAgg[queryExecuterProperties.measureStartIndex + i].isFirstTime()) {
          resultDataA[i + 1][columnIndex] = null;
        } else {
          Object msrVal;
          switch (msr.getMeasure().getDataType()) {
            case LONG:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getLongValue();
              break;
            case DECIMAL:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getBigDecimalValue();
              break;
            default:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getDoubleValue();
          }
          resultDataA[i + 1][columnIndex] = DataTypeUtil
              .getMeasureDataBasedOnDataType(msrVal,
                  msr.getMeasure().getDataType());
        }
      }
    }
    LOGGER.info("###########################################------ Total Number of records"
            + resultDataA[0].length);
    return new BatchRawResult(resultDataA);
  }
}

