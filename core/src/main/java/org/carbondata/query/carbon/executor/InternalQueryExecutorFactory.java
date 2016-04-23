package org.carbondata.query.carbon.executor;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.query.carbon.executor.impl.AggregationQueryExecutor;
import org.carbondata.query.carbon.executor.impl.CountStartExecutor;
import org.carbondata.query.carbon.executor.impl.DetailQueryExecutor;
import org.carbondata.query.carbon.executor.impl.DetailWithOrderQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Factory class to get the query executor based on query type
 */
public class InternalQueryExecutorFactory {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InternalQueryExecutorFactory.class.getName());

  /**
   * Method to get the query executor instance
   *
   * @param queryModel query model
   * @return list of data block which is available for query
   */
  public static InternalQueryExecutor getQueryExecutor(QueryModel queryModel,
      List<AbstractIndex> dataBlock) {

    // if all the other query property like query dimension dimension
    //aggregation expression are empty and is count start query is true
    //with one measure on which counter will be executed
    // then its a counter start query
    if (queryModel.isCountStarQuery() && null == queryModel.getFilterEvaluatorTree()
        && queryModel.getQueryDimension().size() < 1 && queryModel.getQueryDimension().size() < 2
        && queryModel.getDimAggregationInfo().size() < 1
        && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Count(*) query: ");
      return new CountStartExecutor(dataBlock);
    }
    // if all the query property is empty then is a function query like count(1)
    // in that case we need to return empty record of size number of records
    //present in the carbon data file
    else if (null == queryModel.getFilterEvaluatorTree()
        && queryModel.getQueryDimension().size() == 0 && queryModel.getQueryMeasures().size() == 0
        && queryModel.getDimAggregationInfo().size() == 0
        && queryModel.getExpressions().size() == 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Function query: ");
      return new CountStartExecutor(dataBlock);
    }
    // if not a detail query then it is a aggregation query
    else if (!queryModel.isDetailQuery()) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Aggergation query: ");
      return new AggregationQueryExecutor();
    }
    // to handle detail with order by query
    else if (queryModel.isDetailQuery() && queryModel.getSortDimension().size() > 0) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Detail with order by query: ");
      return new DetailWithOrderQueryExecutor();
    } else {
      // detail query
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Detail query: ");
      return new DetailQueryExecutor();
    }
  }
}
