package com.huawei.unibi.molap.engine.columnar.aggregator.impl;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.CustomMeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.molapfilterinterface.RowImpl;
import com.huawei.unibi.molap.engine.util.DataTypeConverter;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * To handle aggregation for expressions in the query   
 * 
 * @author K00900841 
 *
 */
public class ExpressionAggregator 
{
    
    private ColumnarAggregatorInfo columnaraggreagtorInfo;
    
    public ExpressionAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo)
    {
        this.columnaraggreagtorInfo=columnaraggreagtorInfo;
    }
    
    public void aggregateExpression(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData)
    {
        int startIndex=this.columnaraggreagtorInfo.getExpressionStartIndex();
        RowImpl rowImpl = null;
        for(int i = 0;i < columnaraggreagtorInfo.getCustomExpressions().size();i++)
        {
            List<Dimension> referredColumns = columnaraggreagtorInfo.getCustomExpressions().get(i).getReferredColumns();
            Object[] row = new Object[referredColumns.size()];
            for(int j = 0;j < referredColumns.size();j++)
            {
                Dimension dimension = referredColumns.get(j);
                if(dimension instanceof Measure)
                {
                    row[j]=keyValue.getNormalMeasureValue(dimension.getOrdinal());
                }
                else if(dimension.isHighCardinalityDim())
                {
                   byte[] directSurrogate= keyValue.getHighCardinalityDimDataForAgg(dimension);
                   row[j]=DataTypeConverter.getDataBasedOnDataType(new String(directSurrogate),
                           dimension.getDataType());
                }
                else
                {
                    int dimSurrogate = keyValue.getDimDataForAgg(dimension.getOrdinal());
                    if(dimSurrogate==1)
                    {
                        row[j]=null;
                    }
                    else
                    {
                    String member = QueryExecutorUtility
                    .getMemberBySurrogateKey(dimension, dimSurrogate,
                            columnaraggreagtorInfo.getSlices(), columnaraggreagtorInfo.getCurrentSliceIndex()).toString();
                    row[j]=DataTypeConverter.getDataBasedOnDataType(member,
                            dimension.getDataType());
                    }
                }
            }
            CustomMeasureAggregator agg = (CustomMeasureAggregator)currentMsrRowData[startIndex+i];
            rowImpl = new RowImpl();
            rowImpl.setValues(row);
            agg.agg(rowImpl);
        }
        
    }
}
