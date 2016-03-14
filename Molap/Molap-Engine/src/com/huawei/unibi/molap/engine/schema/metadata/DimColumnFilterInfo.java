package com.huawei.unibi.molap.engine.schema.metadata;

import java.util.List;


public class DimColumnFilterInfo
{
    private boolean isIncludeFilter;
    
    private List<Integer> filterList;

    public boolean isIncludeFilter()
    {
        return isIncludeFilter;
    }

    public void setIncludeFilter(boolean isIncludeFilter)
    {
        this.isIncludeFilter = isIncludeFilter;
    }

    public List<Integer> getFilterList()
    {
        return filterList;
    }

    public void setFilterList(List<Integer> filterList)
    {
        this.filterList = filterList;
    }
}
