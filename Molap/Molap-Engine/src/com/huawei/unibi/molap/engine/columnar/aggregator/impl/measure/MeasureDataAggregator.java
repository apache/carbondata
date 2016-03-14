package com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;

public abstract class MeasureDataAggregator
{
    /**
     * columnarScannerVo
     */
    protected ColumnarAggregatorInfo columnaraggreagtorInfo;
    
    protected int noOfMeasuresInQuery;

    protected double[] uniqueValues;
    
    protected int[] measureOrdinal;
    
    public MeasureDataAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo)
    {
        this.columnaraggreagtorInfo=columnaraggreagtorInfo;
        this.noOfMeasuresInQuery = columnaraggreagtorInfo.getMeasureOrdinal().length;
        this.measureOrdinal=columnaraggreagtorInfo.getMeasureOrdinal();
        this.uniqueValues=columnaraggreagtorInfo.getUniqueValue();
    }
    
    /**
     * aggregateMsrs
     * 
     * @param available
     * @param currentMsrRowData
     */
    public abstract void aggregateMeasure(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData);
}
