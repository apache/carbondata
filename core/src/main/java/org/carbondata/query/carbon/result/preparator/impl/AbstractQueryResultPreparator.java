package org.carbondata.query.carbon.result.preparator.impl;

import java.util.List;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.preparator.QueryResultPreparator;
import org.carbondata.query.carbon.util.DataTypeUtil;

public abstract class AbstractQueryResultPreparator<K, V> implements QueryResultPreparator<K, V> {

  /**
   * query properties
   */
  protected QueryExecutorProperties queryExecuterProperties;

  /**
   * query model
   */
  protected QueryModel queryModel;

  public AbstractQueryResultPreparator(QueryExecutorProperties executerProperties,
      QueryModel queryModel) {
    this.queryExecuterProperties = executerProperties;
    this.queryModel = queryModel;
  }

  protected void fillMeasureValueForAggGroupByQuery(QueryModel queryModel,
      Object[][] surrogateResult, int dimensionCount, int columnIndex, MeasureAggregator[] v) {
    int msrCount = queryModel.getQueryMeasures().size();
    for (int i = 0; i < msrCount; i++) {
      v[queryExecuterProperties.measureStartIndex + i] =
          ((MeasureAggregator) surrogateResult[dimensionCount
              + queryExecuterProperties.measureStartIndex + i][columnIndex]);
    }
  }

  protected void fillDimensionData(Object[][] convertedResult, List<QueryDimension> queryDimensions,
      int dimensionCount, Object[] row, int rowIndex) {
    QueryDimension queryDimension;
    for (int i = 0; i < dimensionCount; i++) {
      queryDimension = queryDimensions.get(i);
      if (!CarbonUtil.hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.DICTIONARY)
          || CarbonUtil.hasDataType(queryDimension.getDimension().getDataType(),
          new DataType[] { DataType.ARRAY, DataType.STRUCT, DataType.MAP })) {
        row[queryDimension.getQueryOrder()] = convertedResult[i][rowIndex];
      } else if (CarbonUtil
          .hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(queryDimension.getDimension().getDataType());
        row[queryDimension.getQueryOrder()] =
            directDictionaryGenerator.getValueFromSurrogate((Integer) convertedResult[i][rowIndex]);
      } else {
        if (queryExecuterProperties.sortDimIndexes[i] == 1) {
          row[queryDimension.getQueryOrder()] = DataTypeUtil.getDataBasedOnDataType(
              queryExecuterProperties.columnToDictionayMapping
                  .get(queryDimension.getDimension().getColumnId())
                  .getDictionaryValueFromSortedIndex((Integer) convertedResult[i][rowIndex]),
              queryDimension.getDimension().getDataType());
        } else {
          row[queryDimension.getQueryOrder()] = DataTypeUtil.getDataBasedOnDataType(
              queryExecuterProperties.columnToDictionayMapping
                  .get(queryDimension.getDimension().getColumnId())
                  .getDictionaryValueForKey((Integer) convertedResult[i][rowIndex]),
              queryDimension.getDimension().getDataType());
        }
      }
    }
  }

  protected Object[][] encodeToRows(Object[][] data) {
    if (data.length == 0) {
      return data;
    }
    Object[][] rData = new Object[data[0].length][data.length];
    int len = data.length;
    for (int i = 0; i < rData.length; i++) {
      for (int j = 0; j < len; j++) {
        rData[i][j] = data[j][i];
      }
    }
    return rData;
  }

  protected BatchResult getEmptyChunkResult(int size) {
    Object[][] row = new Object[size][1];
    BatchResult chunkResult = new BatchResult();
    chunkResult.setRows(row);
    return chunkResult;
  }

}
