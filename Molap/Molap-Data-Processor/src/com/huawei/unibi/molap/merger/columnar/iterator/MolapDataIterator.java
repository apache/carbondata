package com.huawei.unibi.molap.merger.columnar.iterator;

public interface MolapDataIterator<E>
{
    boolean hasNext();
    
    void fetchNextData();
    
    E getNextData();
}
