package org.apache.carbondata.presto.scan.executor.impl;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.AbstractQueryExecutor;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.presto.scan.result.ColumnBasedResultIterator;

public class ColumnDetailQueryExecutor extends AbstractQueryExecutor implements QueryExecutor {

    @Override
    public CarbonIterator execute(QueryModel queryModel)
        throws QueryExecutionException, IOException {
        List blockExecutionInfoList = getBlockExecutionInfos(queryModel);
        this.queryIterator = new ColumnBasedResultIterator(blockExecutionInfoList, queryModel,
            queryProperties.executorService);
        return this.queryIterator;
    }
}