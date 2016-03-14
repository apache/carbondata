package com.huawei.unibi.molap.datastorage.store.impl;

import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;

/**
 */
public class CompressedDataMeasureDataWrapper implements MeasureDataWrapper
{

    
    private final MolapReadDataHolder[] values;

    /**
     * @param values
     * @param decimal
     * @param maxValue
     */
    public CompressedDataMeasureDataWrapper(final MolapReadDataHolder[] values)
    {
        this.values = values;
    }
    
    @Override
    public MolapReadDataHolder[] getValues()
    {
        // TODO Auto-generated method stub
        return values;
    }


}
