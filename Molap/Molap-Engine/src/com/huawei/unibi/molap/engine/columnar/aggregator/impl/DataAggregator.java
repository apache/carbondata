package com.huawei.unibi.molap.engine.columnar.aggregator.impl;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.dimension.DimensionDataAggreagtor;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure.AggregateTableAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure.FactTableAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure.MeasureDataAggregator;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class DataAggregator
{
    private MeasureDataAggregator msrAggregator;

    private DimensionDataAggreagtor dimensionDataAggreagtor;
    
    private ExpressionAggregator expressionAggregator;

    public DataAggregator(boolean isAggTable, ColumnarAggregatorInfo columnarAggregatorInfo)
    {
        if(!isAggTable)
        {
            msrAggregator = new FactTableAggregator(columnarAggregatorInfo);
        }
        else
        {
            msrAggregator = new AggregateTableAggregator(columnarAggregatorInfo);
        }

        dimensionDataAggreagtor = new DimensionDataAggreagtor(columnarAggregatorInfo);
        expressionAggregator = new ExpressionAggregator(columnarAggregatorInfo);
    }

    public void aggregateData(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData, ByteArrayWrapper dimensionsRowWrapper)
    {
        dimensionDataAggreagtor.aggregateDimension(keyValue, currentMsrRowData,dimensionsRowWrapper);
        expressionAggregator.aggregateExpression(keyValue, currentMsrRowData);
        msrAggregator.aggregateMeasure(keyValue, currentMsrRowData);
    }
}
