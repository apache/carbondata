/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.aggregator;

import java.util.List;

import com.huawei.unibi.molap.engine.expression.ColumnExpression;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowIntf;

/**
 * 
 * @author K00900207
 * 
 */
public interface CustomMeasureAggregator extends MeasureAggregator
{
    /**
     * Aggregate method with generic row interface where RowIntf holds value for
     * each column given in MeasureAggregator@getColumns()
     */
    void agg(RowIntf row);

    /**
     * 
     * @return List of columns required for the aggregator
     * 
     */
    List<ColumnExpression> getColumns();

}
