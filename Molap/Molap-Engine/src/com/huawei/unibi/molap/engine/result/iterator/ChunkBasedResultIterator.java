package com.huawei.unibi.molap.engine.result.iterator;

import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.impl.QueryExecuterProperties;
import com.huawei.unibi.molap.engine.executer.impl.QueryResultPreparator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.ChunkResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ChunkBasedResultIterator implements MolapIterator<ChunkResult>
{
    private MolapIterator<QueryResult> queryResultIterator;
    
    private QueryResultPreparator queryResultPreparator;
    
    public ChunkBasedResultIterator(MolapIterator<QueryResult> queryResultIterator, QueryExecuterProperties executerProperties, MolapQueryExecutorModel queryModel)
    {
        this.queryResultIterator=queryResultIterator;
        this.queryResultPreparator=new QueryResultPreparator(executerProperties,queryModel);
        
    }
    @Override
    public boolean hasNext()
    {
        return queryResultIterator.hasNext();
    }

    @Override
    public ChunkResult next()
    {
        return queryResultPreparator.prepareQueryOutputResult(queryResultIterator.next());
    }

}
