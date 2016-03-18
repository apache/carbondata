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
