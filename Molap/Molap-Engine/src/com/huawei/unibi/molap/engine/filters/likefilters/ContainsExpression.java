package com.huawei.unibi.molap.engine.filters.likefilters;

import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

public class ContainsExpression extends MolapFilterInfo implements FilterLikeExpressionIntf
{
    
    /**
     * 
     */
    private static final long serialVersionUID = -1463266055265811771L;
    private LikeExpression likeContainsExpr;
    
    @Override
    public LikeExpression getLikeExpression()
    {
        // TODO Auto-generated method stub 
        return likeContainsExpr;
    }
    
    @Override
    public void setLikeExpression(LikeExpression expressionName)
    {
        likeContainsExpr=expressionName;

    }


    @Override
    public void processLikeExpressionFilters(List<String> listFilterExpression, List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo, boolean hasNameColumn,
            Locale locale)
    {
        // TODO Auto-generated method stub
        
    }

}
