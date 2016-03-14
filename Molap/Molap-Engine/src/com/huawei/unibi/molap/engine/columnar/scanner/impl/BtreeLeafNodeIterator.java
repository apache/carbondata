package com.huawei.unibi.molap.engine.columnar.scanner.impl;

import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class BtreeLeafNodeIterator implements MolapIterator<DataStoreBlock>
{
    private int blockCounter;

    private boolean hasNext = true;

    private long totalNumberOfBlocksToScan;
    
    /**
     * data store block
     */
    protected DataStoreBlock datablock;
    
    public BtreeLeafNodeIterator(DataStoreBlock datablock, long totalNumberOfBlocksToScan)
    {
        this.datablock=datablock;
        this.totalNumberOfBlocksToScan = totalNumberOfBlocksToScan;
    }
    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public DataStoreBlock next()
    {
        DataStoreBlock datablockTemp=datablock;
        datablock = datablock.getNext();
        blockCounter++;
        if(null == datablock || blockCounter >= this.totalNumberOfBlocksToScan)
        {
            hasNext = false;
        }
        return datablockTemp;
    }
}
