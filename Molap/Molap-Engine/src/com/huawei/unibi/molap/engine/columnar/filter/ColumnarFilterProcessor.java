package com.huawei.unibi.molap.engine.columnar.filter;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;


public interface ColumnarFilterProcessor
{
    void getFilteredIndexes(int keyBlockIndex, int numerOfRows, BitSet bitSet,ColumnarKeyStoreDataHolder... keyBlockArray);
    
    BitSet getFilteredIndexes(int keyBlockIndex, int numerOfRows,ColumnarKeyStoreDataHolder... keyBlockArray);
}
