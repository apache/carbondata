package org.carbondata.query.carbon.executor.impl;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.iterator.DetailQueryResultIterator;
import org.carbondata.query.carbon.result.preparator.impl.RawQueryResultPreparatorImpl;

/**
 * Executor for raw records, it does not parse to actual data
 */
public class DetailRawRecordQueryExecutor extends AbstractQueryExecutor<BatchResult> {

  @Override public CarbonIterator<BatchResult> execute(QueryModel queryModel)
      throws QueryExecutionException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    return new DetailQueryResultIterator(blockExecutionInfoList, queryModel,
        new RawQueryResultPreparatorImpl(queryProperties, queryModel));
  }
}
