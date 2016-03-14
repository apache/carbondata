package com.huawei.unibi.molap.engine.filters.likefilters;

import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.filters.metadata.ContentMatchFilterInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.filter.MolapFilterInfo;

public interface FilterLikeExpressionIntf
{

    void setLikeExpression(LikeExpression expression);

     LikeExpression getLikeExpression();

    void processLikeExpressionFilters(List<String> listFilterExpression, List<InMemoryCube> slices,
            Entry<Dimension, MolapFilterInfo> entry, ContentMatchFilterInfo matchFilterInfo,boolean hasNameColumn, Locale locale);

}
