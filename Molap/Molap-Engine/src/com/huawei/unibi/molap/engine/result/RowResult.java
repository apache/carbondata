package com.huawei.unibi.molap.engine.result;

import com.huawei.unibi.molap.engine.scanner.impl.MolapKey;
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue;

public class RowResult
{
    private MolapKey key;
    
    private MolapValue value;

    public MolapKey getKey()
    {
        return key;
    }

    public void setKey(MolapKey key)
    {
        this.key = key;
    }

    public MolapValue getValue()
    {
        return value;
    }

    public void setValue(MolapValue value)
    {
        this.value = value;
    }
}
