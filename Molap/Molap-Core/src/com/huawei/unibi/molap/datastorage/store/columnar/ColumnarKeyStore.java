package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.datastorage.store.FileHolder;

public interface ColumnarKeyStore
{
    /**
     * This method will be used to get the actual mdkeys array present in the
     * molap store, it will read and uncomnpress the key 
     * 
     * @param fileHolder
     * @return mdkey
     * 
     * 
     */
     ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(
            FileHolder fileHolder, int[] blockIndex,boolean[] needCompressedData);
    
    /**
     * This method will be used to get the actual mdkeys array present in the
     * molap store, it will read and uncomnpress the key 
     * 
     * @param fileHolder
     * @return mdkey
     * 
     * 
     */
     ColumnarKeyStoreDataHolder getUnCompressedKeyArray(
            FileHolder fileHolder, int blockIndex,boolean needCompressedData);
    
    
}
