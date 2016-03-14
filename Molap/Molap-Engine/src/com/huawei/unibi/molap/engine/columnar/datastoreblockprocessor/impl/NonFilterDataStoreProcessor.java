package com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.impl;

import com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.NonFilterScanResult;

public class NonFilterDataStoreProcessor extends AbstractColumnarDataStoreProcessor
{

    public NonFilterDataStoreProcessor(ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo)
    {
        super(columnarDataStoreBlockInfo);
        this.keyValue = new NonFilterScanResult(this.columnarDataStoreBlockInfo.getKeySize(),columnarDataStoreBlockInfo.getDimensionIndexes());
    }
    
}
