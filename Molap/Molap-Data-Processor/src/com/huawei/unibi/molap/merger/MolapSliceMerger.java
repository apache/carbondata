package com.huawei.unibi.molap.merger;

import com.huawei.unibi.molap.merger.exeception.SliceMergerException;

public interface MolapSliceMerger
{
    boolean fullMerge(int currentRestructNumber) throws SliceMergerException;
}
