package com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor;

import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;

public interface DataStoreBlockProcessor
{
    AbstractColumnarScanResult getScannedData(BlockDataHolder blockDataHolder);
    
}
