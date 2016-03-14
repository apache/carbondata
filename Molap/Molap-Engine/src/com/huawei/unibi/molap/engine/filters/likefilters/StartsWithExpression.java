package com.huawei.unibi.molap.engine.filters.likefilters;

import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class StartsWithExpression extends MolapFilterInfo implements FilterLikeExpressionIntf
{



    /**
     * 
     */
    private static final long serialVersionUID = 3813284333559000811L;
    private LikeExpression likeContainsExpression;


    @Override
    public LikeExpression getLikeExpression()
    {
        // TODO Auto-generated method stub
        return likeContainsExpression;
    }
    
    @Override
    public void processLikeExpressionFilters(List<String> listFilterExpression, List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void setLikeExpression(LikeExpression expressionName)
    {
        likeContainsExpression=expressionName;

    }

 



}
