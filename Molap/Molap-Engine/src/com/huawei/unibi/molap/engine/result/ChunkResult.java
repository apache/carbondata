package com.huawei.unibi.molap.engine.result;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey;
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ChunkResult implements MolapIterator<RowResult>
{
    private List<MolapKey> keys;
    private List<MolapValue> values;
    
    private int counter;
    
    public ChunkResult()
    {
        keys = new ArrayList<MolapKey>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        values = new ArrayList<MolapValue>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    public List<MolapKey> getKeys()
    {
        return keys;
    }

    public void setKeys(List<MolapKey> keys)
    {
        this.keys = keys;
    }

    public List<MolapValue> getValues()
    {
        return values;
    }

    public void setValues(List<MolapValue> values)
    {
        this.values = values;
    }

    @Override
    public boolean hasNext()
    {
        return counter<keys.size();
    }

    @Override
    public RowResult next()
    {
        RowResult rowResult = new RowResult();
        rowResult.setKey(keys.get(counter));
        rowResult.setValue(values.get(counter));
        counter++;
        return rowResult;
    }
}
