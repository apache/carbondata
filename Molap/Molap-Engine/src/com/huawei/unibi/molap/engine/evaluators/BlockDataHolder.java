package com.huawei.unibi.molap.engine.evaluators;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;

public class BlockDataHolder
{
    private MolapReadDataHolder[] measureBlocks;
    
    private ColumnarKeyStoreDataHolder[] columnarKeyStore;
    
    private DataStoreBlock leafDataBlock;
    
    private FileHolder fileHolder;
    
    public BlockDataHolder(int dimColumnCount, int msrColumnCount)
    {
        this.measureBlocks=new MolapReadDataHolder[msrColumnCount];
        this.columnarKeyStore=new ColumnarKeyStoreDataHolder[dimColumnCount];
    }
    
    public MolapReadDataHolder[] getMeasureBlocks()
    {
        return measureBlocks;
    }

    public void setMeasureBlocks(final MolapReadDataHolder[] measureBlocks)
    {
        this.measureBlocks = measureBlocks;
    }

    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore()
    {
        return columnarKeyStore;
    }

    public void setColumnarKeyStore(final ColumnarKeyStoreDataHolder[] columnarKeyStore)
    {
        this.columnarKeyStore = columnarKeyStore;
    }

    public DataStoreBlock getLeafDataBlock()
    {
        return leafDataBlock;
    }

    public void setLeafDataBlock(final DataStoreBlock dataBlock)
    {
        this.leafDataBlock = dataBlock;
    }

    public FileHolder getFileHolder()
    {
        return fileHolder;
    }

    public void setFileHolder(final FileHolder fileHolder)
    {
        this.fileHolder = fileHolder;
    }
    
    public void reset()
    {
        for(int i = 0;i < measureBlocks.length;i++)
        {
            this.measureBlocks[i]=null;
        }
        for(int i = 0;i < columnarKeyStore.length;i++)
        {
            this.columnarKeyStore[i]= null;
        }
    }
}
