package com.huawei.unibi.molap.engine.columnar.aggregator;

import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.result.Result;

public interface ColumnarScannedResultAggregator
{
    int aggregateData(AbstractColumnarScanResult keyValue);
    
    Result getResult(RestructureHolder restructureHolder);
    
}
