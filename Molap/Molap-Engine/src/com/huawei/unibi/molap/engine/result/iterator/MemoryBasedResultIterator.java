package com.huawei.unibi.molap.engine.result.iterator;

import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class MemoryBasedResultIterator implements MolapIterator<QueryResult>
{
    private QueryResult result;
    
    private boolean hasNext=true;
    
    public MemoryBasedResultIterator(QueryResult result)
    {
        this.result=result;
    }
    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public QueryResult next()
    {
        hasNext=false;
        return result;
    }

}
