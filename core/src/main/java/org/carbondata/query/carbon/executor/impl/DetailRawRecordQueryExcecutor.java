package org.carbondata.query.carbon.executor.impl;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.internal.impl.InternalDetailQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.iterator.DetailRawQueryResultIterator;

/**
 * Executor for raw records, it does not parse to actual data
 */
public class DetailRawRecordQueryExcecutor extends AbstractQueryExecutor<BatchRawResult> {

  @Override public CarbonIterator<BatchRawResult> execute(QueryModel queryModel)
      throws QueryExecutionException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    InternalQueryExecutor queryExecutor = new InternalDetailQueryExecutor(queryModel);
    return new DetailRawQueryResultIterator(blockExecutionInfoList, queryProperties, queryModel,
        queryExecutor);
  }
}
