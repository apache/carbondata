package com.huawei.unibi.molap.engine.filters.likefilters;

import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class EndsWithExpression extends MolapFilterInfo implements FilterLikeExpressionIntf
{


 
    /**
     * 
     */
    private static final long serialVersionUID = -2334245511608022225L;
    private LikeExpression likeContainsExpression;

    @Override
    public LikeExpression getLikeExpression()
    {
        return likeContainsExpression;
    }

    
    

    @Override
    public void setLikeExpression(LikeExpression exprName)
    {
        likeContainsExpression=exprName; 

    }




    @Override
    public void processLikeExpressionFilters(List<String> listFilterExpression, List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        // TODO Auto-generated method stub
        
    }

}
