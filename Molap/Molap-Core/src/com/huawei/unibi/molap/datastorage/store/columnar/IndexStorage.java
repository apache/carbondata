package com.huawei.unibi.molap.datastorage.store.columnar;

public interface IndexStorage<T>
{
     boolean isAlreadySorted();
    
     T getDataAfterComp();
    
     T getIndexMap();
    
     byte[][] getKeyBlock();
    
     T getDataIndexMap();
    
     int getTotalSize();
    
}
