package com.huawei.unibi.molap.engine.columnar.aggregator.impl.measure;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.util.MolapUtil;

public class AggregateTableAggregator extends FactTableAggregator
{
    private char[] type;
    public AggregateTableAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo)
    {
        super(columnaraggreagtorInfo);
        type=new char[columnaraggreagtorInfo.getAggType().length];
        for(int i = 0;i < type.length;i++)
        {
            type[i]=MolapUtil.getType(columnaraggreagtorInfo.getAggType()[i]);
        }
    }
    
    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    public void aggregateMeasure(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData)
    {
        byte[] byteValue= null;
        double doubleValue= 0;
        for(int i = 0;i < noOfMeasuresInQuery;i++)
        {
            if(type[i]==MolapCommonConstants.SUM_COUNT_VALUE_MEASURE)
            {
                doubleValue = keyValue.getNormalMeasureValue(measureOrdinal[i]);
                if(uniqueValues[measureOrdinal[i]] != doubleValue)
                {
                    currentMsrRowData[columnaraggreagtorInfo.getMeasureStartIndex() + i].agg(doubleValue, null, 0, 0);
                }
            }
            else
            {
                byteValue = keyValue.getCustomMeasureValue(measureOrdinal[i]);
                currentMsrRowData[columnaraggreagtorInfo.getMeasureStartIndex() + i].merge(byteValue);
            }
        }
    }

}
