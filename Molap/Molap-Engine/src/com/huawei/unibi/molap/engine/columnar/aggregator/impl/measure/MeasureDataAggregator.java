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

    protected Object[] uniqueValues;
    
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
