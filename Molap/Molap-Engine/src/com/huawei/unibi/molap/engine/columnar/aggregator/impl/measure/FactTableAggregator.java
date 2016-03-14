package com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;

public class FactTableAggregator extends MeasureDataAggregator
{
    private boolean[] isMeasureExists;
    
    private double[] measureDefaultValue;
    
    public FactTableAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo)
    {
        super(columnaraggreagtorInfo);   
        this.isMeasureExists=columnaraggreagtorInfo.getIsMeasureExistis();
        this.measureDefaultValue=columnaraggreagtorInfo.getMsrDefaultValue();
        
    }
    
    /**
     * aggregateMsrs
     * 
     * @param available
     * @param currentMsrRowData
     */
    public void aggregateMeasure(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData)
    {
        for(int i = 0;i < noOfMeasuresInQuery;i++)
        {
            double value = isMeasureExists[i]?keyValue.getNormalMeasureValue(measureOrdinal[i]):measureDefaultValue[i];
            if(isMeasureExists[i] && uniqueValues[measureOrdinal[i]] != value)
            {
                currentMsrRowData[columnaraggreagtorInfo.getMeasureStartIndex()+i].agg(value, null, 0, 0);
            }
            else if(!isMeasureExists[i])
            {
                currentMsrRowData[columnaraggreagtorInfo.getMeasureStartIndex()+i].agg(measureDefaultValue[i], null, 0, 0);
            }
        }
    }
}
