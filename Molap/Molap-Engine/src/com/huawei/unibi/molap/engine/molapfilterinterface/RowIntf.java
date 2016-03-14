package com.huawei.unibi.molap.engine.molapfilterinterface;


public interface RowIntf
{
    Object getVal(int index);
    Object[] getValues();
    int size();
    void setValues(Object[] setValues);
    
}
