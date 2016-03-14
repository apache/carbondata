/**
 * 
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import java.io.Serializable;
import java.util.Arrays;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class MolapValue implements Serializable,Comparable<MolapValue>
{

    /**
     * 
     */
    private static final long serialVersionUID = 8034398963696130423L;
    
    private MeasureAggregator[] values;
    
    private int topNIndex;

    public MolapValue(MeasureAggregator[] values)
    {
        this.values = values;
    }

    /**
     * @return the values
     */
    public MeasureAggregator[] getValues()
    {
        return values;
    }
    
    public MolapValue merge(MolapValue another)
    {
        for(int i = 0;i < values.length;i++)
        {
            values[i].merge(another.values[i]);
        }
        return this;
    }
    
    public void setTopNIndex(int index)
    {
        this.topNIndex = index;
    }
    
    public void addGroup(MolapKey key,MolapValue value)
    {
        
    }
    
    public MolapValue mergeKeyVal(MolapValue another)
    {
        return another;
    }
    
    @Override
    public String toString()
    {
        return Arrays.toString(values);
    }

    @Override
    public int compareTo(MolapValue o)
    {
        return values[topNIndex].compareTo(o.values[topNIndex]);
    }
    

}
