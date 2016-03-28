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

package org.carbondata.query.columnar.aggregator.impl.measure;

import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.columnar.aggregator.ColumnarAggregatorInfo;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;

public class FactTableAggregator extends MeasureDataAggregator {
    private boolean[] isMeasureExists;

    private Object[] measureDefaultValue;

    public FactTableAggregator(ColumnarAggregatorInfo columnaraggreagtorInfo) {
        super(columnaraggreagtorInfo);
        this.isMeasureExists = columnaraggreagtorInfo.getIsMeasureExistis();
        this.measureDefaultValue = columnaraggreagtorInfo.getMsrDefaultValue();

    }

    /**
     * aggregateMsrs
     *
     * @param currentMsrRowData
     */
    public void aggregateMeasure(AbstractColumnarScanResult keyValue,
            MeasureAggregator[] currentMsrRowData) {
        Object value = null;
        SqlStatement.Type dataType = null;
        for (int i = 0; i < noOfMeasuresInQuery; i++) {
            if (isMeasureExists[i]) {
                int index = columnaraggreagtorInfo.getMeasureOrdinalMap().get(measureOrdinal[i]);
                dataType = this.columnaraggreagtorInfo.getDataTypes()[index];
                switch (dataType) {
                case LONG:
                    value = keyValue.getLongValue(measureOrdinal[i]);
                    break;
                case DECIMAL:
                    value = keyValue.getBigDecimalValue(measureOrdinal[i]);
                    break;
                default:
                    value = keyValue.getDoubleValue(measureOrdinal[i]);
                }
            } else {
                value = measureDefaultValue[i];
            }
            if (!uniqueValues[measureOrdinal[i]].equals(value)) {
                currentMsrRowData[columnaraggreagtorInfo.getMeasureStartIndex() + i].agg(value);
            }
        }
    }
}
