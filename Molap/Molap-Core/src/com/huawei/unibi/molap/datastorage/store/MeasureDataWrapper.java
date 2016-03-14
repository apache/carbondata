package com.huawei.unibi.molap.datastorage.store;

import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;

/**
 * MeasureDataWrapper, interface.
 * @author S71955
 *
 */
public interface MeasureDataWrapper
{
     MolapReadDataHolder[] getValues();
    
}
