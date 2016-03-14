package com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.impl;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import com.huawei.unibi.molap.engine.columnar.datastoreblockprocessor.DataStoreBlockProcessor;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.evaluators.BlockDataHolder;

public abstract class AbstractColumnarDataStoreProcessor implements DataStoreBlockProcessor
{
    protected AbstractColumnarScanResult keyValue;
    
    protected ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo;
    
    public AbstractColumnarDataStoreProcessor(ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo)
    {
        this.columnarDataStoreBlockInfo=columnarDataStoreBlockInfo;
    }
    
    protected void fillKeyValue(BlockDataHolder blockDataHolder)
    {
        keyValue.reset();
        keyValue.setMeasureBlock(blockDataHolder.getLeafDataBlock().getNodeMsrDataWrapper(columnarDataStoreBlockInfo.getAllSelectedMeasures(),
                columnarDataStoreBlockInfo.getFileHolder()).getValues());
        keyValue.setNumberOfRows(blockDataHolder.getLeafDataBlock().getnKeys());
        ColumnarKeyStoreDataHolder[] columnarKeyStore = blockDataHolder.getLeafDataBlock().getColumnarKeyStore(columnarDataStoreBlockInfo.getFileHolder(),
                columnarDataStoreBlockInfo.getAllSelectedDimensions(), new boolean[columnarDataStoreBlockInfo.getAllSelectedDimensions().length]);
        ColumnarKeyStoreDataHolder[] temp = new  ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo.getTotalNumberOfDimension()];
        for(int i = 0;i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length;i++)
        {
            temp[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]]=columnarKeyStore[i];
        }
        keyValue.setKeyBlock(temp);
    }
    
    @Override
    public AbstractColumnarScanResult getScannedData(BlockDataHolder blockDataHolder)
    {
        fillKeyValue(blockDataHolder);
        return keyValue;
    }
}
