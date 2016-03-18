/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
