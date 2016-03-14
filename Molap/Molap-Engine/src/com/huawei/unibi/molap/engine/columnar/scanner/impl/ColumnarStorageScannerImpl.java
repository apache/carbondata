package com.huawei.unibi.molap.engine.columnar.scanner.impl;

import com.huawei.unibi.molap.engine.columnar.aggregator.impl.DataAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.ListBasedResultAggregatorImpl;
import com.huawei.unibi.molap.engine.columnar.scanner.AbstractColumnarStorageScanner;
import com.huawei.unibi.molap.engine.schema.metadata.ColumnarStorageScannerInfo;

public class ColumnarStorageScannerImpl extends AbstractColumnarStorageScanner
{
    private long counter;
    
    private int limit;
    
    public ColumnarStorageScannerImpl(ColumnarStorageScannerInfo columnarStorageScannerInfo)
    {
        super(columnarStorageScannerInfo);
        
        this.columnarAggaregator = new ListBasedResultAggregatorImpl(
                columnarStorageScannerInfo.getColumnarAggregatorInfo(), new DataAggregator(
                        columnarStorageScannerInfo.isAutoAggregateTableRequest(),
                        columnarStorageScannerInfo.getColumnarAggregatorInfo()));
        limit=columnarStorageScannerInfo.getColumnarAggregatorInfo().getLimit();
    }
    
    @Override
    public void scanStore()
    {
        while(leafIterator.hasNext())
        {
            blockDataHolder.setLeafDataBlock(leafIterator.next());
            addToQueryStats(blockDataHolder);
            blockDataHolder.reset();
            counter+=this.columnarAggaregator.aggregateData(blockProcessor.getScannedData(blockDataHolder));
            finish();
            if(limit != -1 && counter >= limit)
            {
                break;
            }
        }
    }
}
