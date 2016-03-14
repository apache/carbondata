package com.huawei.unibi.molap.engine.result;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public interface Result<K,V>
{
    void addScannedResult(K key,V value);
    
    boolean hasNext();
    
    ByteArrayWrapper getKey();

    MeasureAggregator[] getValue();
    
    void merge(Result<K,V> otherResult);
    
    K getKeys();
    
    V getValues();
    
    int size();
}
