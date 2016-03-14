package com.huawei.unibi.molap.engine.columnar.scanner.impl;

import com.huawei.unibi.molap.engine.columnar.aggregator.impl.DataAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.MapBasedResultAggregatorImpl;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.columnar.scanner.AbstractColumnarStorageScanner;
import com.huawei.unibi.molap.engine.schema.metadata.ColumnarStorageScannerInfo;

public class ColumnarStorageAggregatedScannerImpl extends AbstractColumnarStorageScanner
{

    public ColumnarStorageAggregatedScannerImpl(ColumnarStorageScannerInfo columnarStorageScannerInfo)
    {
        super(columnarStorageScannerInfo);
        this.columnarAggaregator = new MapBasedResultAggregatorImpl(
                columnarStorageScannerInfo.getColumnarAggregatorInfo(), new DataAggregator(
                        columnarStorageScannerInfo.isAutoAggregateTableRequest(),
                        columnarStorageScannerInfo.getColumnarAggregatorInfo()));
    }

    @Override
    public void scanStore()
    {
        while(leafIterator.hasNext())
        {
            blockDataHolder.setLeafDataBlock(leafIterator.next());
            addToQueryStats(blockDataHolder);
            blockDataHolder.reset();
            AbstractColumnarScanResult unProcessData = blockProcessor.getScannedData(blockDataHolder);
            this.columnarAggaregator.aggregateData(unProcessData);
        }
        finish();
    }
}
