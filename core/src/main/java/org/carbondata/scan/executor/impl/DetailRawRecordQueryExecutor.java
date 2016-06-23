package org.carbondata.scan.executor.impl;

import java.util.List;

import org.carbondata.common.CarbonIterator;
import org.carbondata.scan.executor.exception.QueryExecutionException;
import org.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.carbondata.scan.model.QueryModel;
import org.carbondata.scan.result.BatchResult;
import org.carbondata.scan.result.iterator.DetailQueryResultIterator;
import org.carbondata.scan.result.preparator.impl.RawQueryResultPreparatorImpl;

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
