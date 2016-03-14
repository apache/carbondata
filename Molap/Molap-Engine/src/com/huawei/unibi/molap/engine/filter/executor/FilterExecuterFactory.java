package com.huawei.unibi.molap.engine.filter.executor;

import com.huawei.unibi.molap.engine.filter.executor.impl.MergeSortBasedNonUniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.NonUniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.NonUniqueBlockNotEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.UniqueBlockEquals;
import com.huawei.unibi.molap.engine.filter.executor.impl.UniqueBlockNotEquals;

public final class FilterExecuterFactory
{
    private FilterExecuterFactory()
    {
        
    }
    
    public static FilterExecutor getFilterExecuter(FilterExcutorType filterExcuterType)
    {
        switch(filterExcuterType)
        {
            case UNIQUE_EQUALS:
                return new UniqueBlockEquals();
            case UNIQUE_NOT_EQUALS:
                return new UniqueBlockNotEquals();
            case EQUALS:
                return new NonUniqueBlockEquals();
            case NOT_EQUALS:
                return new NonUniqueBlockNotEquals();
            case MERGESORT_EQUALS:
                return new MergeSortBasedNonUniqueBlockEquals();
            case MERGESORT_NOT_EQUALS:
                return new MergeSortBasedNonUniqueBlockEquals();
            default:
                return null;
        }
    }
}
