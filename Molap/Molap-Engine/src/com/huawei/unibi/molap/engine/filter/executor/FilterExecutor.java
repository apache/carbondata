package com.huawei.unibi.molap.engine.filter.executor;

import java.util.BitSet;

import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;

public interface FilterExecutor
{
    BitSet getFilteredIndexes(ColumnarKeyStoreDataHolder keyBlockArray, int numerOfRows, byte[][] filterValues);
}
