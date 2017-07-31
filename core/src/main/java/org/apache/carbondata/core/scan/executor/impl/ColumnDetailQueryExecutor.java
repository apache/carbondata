package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ColumnBasedResultIterator;

public class ColumnDetailQueryExecutor extends AbstractQueryExecutor {

  @Override public CarbonIterator execute(QueryModel queryModel)
      throws QueryExecutionException, IOException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    this.queryIterator = new ColumnBasedResultIterator(blockExecutionInfoList, queryModel,
        queryProperties.executorService);
    return this.queryIterator;
  }
}