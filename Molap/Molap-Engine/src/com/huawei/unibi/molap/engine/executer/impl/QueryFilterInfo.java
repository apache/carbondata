package com.huawei.unibi.molap.engine.executer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.engine.schema.metadata.DimColumnFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

public class QueryFilterInfo
{
    
    private Map<Dimension,List<DimColumnFilterInfo>> dimensionFilter;
    
    private Map<Measure, List<Double>> measureFilter;
    
    public QueryFilterInfo()
    {
        dimensionFilter  = new HashMap<Dimension, List<DimColumnFilterInfo>>(20);
        measureFilter  = new HashMap<Measure, List<Double>>(20);
    }

    public Map<Dimension, List<DimColumnFilterInfo>> getDimensionFilter()
    {
        return dimensionFilter;
    }

    public void addDimensionFilter(Dimension dim, DimColumnFilterInfo filterValues)
    {
        List<DimColumnFilterInfo> currentFilterValues = dimensionFilter.get(dim);
        if(null==currentFilterValues)
        {
            currentFilterValues = new ArrayList<DimColumnFilterInfo>(20);
            currentFilterValues.add(filterValues);
            dimensionFilter.put(dim,currentFilterValues);
        }
        else
        {
            currentFilterValues.add(filterValues);
        }
    }
    
    public void addMeasureFilter(Measure msr, List<Double> filterValues)
    {
        List<Double> list = measureFilter.get(msr);
        if(null==list)
        {
            measureFilter.put(msr,filterValues);
        }
        else
        {
            list.addAll(filterValues);
        }
    }

    public Map<Measure, List<Double>> getMeasureFilter()
    {
        return measureFilter;
    }

    
}
