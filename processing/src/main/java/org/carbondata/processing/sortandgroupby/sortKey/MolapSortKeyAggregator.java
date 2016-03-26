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

package org.carbondata.processing.sortandgroupby.sortKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.processing.groupby.exception.MolapGroupByException;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.util.AggUtil;

public class MolapSortKeyAggregator {

    private int keyIndex;

    private String[] aggType;

    private String[] aggClassName;

    private boolean isFirst = true;

    private byte[] prvKey;

    private KeyGenerator factKeyGenerator;

    private List<Object[]> result = new ArrayList<Object[]>(20);

    /**
     * max value for each measure
     */
    private char[] type;

    /**
     * aggregators
     */
    private MeasureAggregator[] aggregators;

    /**
     * isNotNullValue
     */
    private boolean[] isNotNullValue;

    private int resultSize;

    private Object[] mergedMinValue;

    /**
     * constructer.
     *
     * @param aggType
     * @param aggClassName
     * @param factKeyGenerator
     * @param type
     */
    public MolapSortKeyAggregator(String[] aggType, String[] aggClassName,
            KeyGenerator factKeyGenerator, char[] type, Object[] mergedMinValue) {
        this.keyIndex = aggType.length;
        this.aggType = aggType;
        this.aggClassName = aggClassName;
        this.factKeyGenerator = factKeyGenerator;
        resultSize = aggType.length + 1;
        this.type = type;
        this.mergedMinValue = mergedMinValue;
    }

    /**
     * getAggregatedData.
     *
     * @param rows
     * @return
     * @throws MolapGroupByException
     * @Description : getAggregatedData
     */
    public Object[][] getAggregatedData(Object[][] rows) throws MolapGroupByException {
        for (int i = 0; i < rows.length; i++) {
            add(rows[i]);
        }

        result.add(prepareResult());
        return result.toArray(new Object[result.size()][]);
    }

    /**
     * This method will be used to add new row it will check if new row and
     * previous row key is same then it will merger the measure values, else it
     * return the previous row
     *
     * @param row new row
     * @return previous row
     * @throws MolapGroupByException
     */
    private void add(Object[] row) throws MolapGroupByException {
        if (isFirst) {

            isFirst = false;
            initialiseAggegators();
            addNewRow(row);

            return;
        }
        if (MolapDataProcessorUtil.compare(prvKey, (byte[]) row[this.keyIndex]) == 0) {
            updateMeasureValue(row);
        } else {
            result.add(prepareResult());
            initialiseAggegators();
            addNewRow(row);
        }
    }

    private Object[] prepareResult() {
        Object[] out = new Object[resultSize];
        for (int i = 0; i < aggregators.length; i++) {
            if (type[i] != 'c') {
                if (isNotNullValue[i]) {
                    switch (type[i]) {
                    case 'l':

                        out[i] = aggregators[i].getLongValue();
                        break;
                    case 'b':

                        out[i] = aggregators[i].getBigDecimalValue();
                        break;
                    default:

                        out[i] = aggregators[i].getDoubleValue();
                    }
                } else {
                    out[i] = null;
                }
            } else {
                out[i] = aggregators[i].getByteArray();
            }
        }

        out[out.length - 1] = prvKey;
        return out;
    }

    private void initialiseAggegators() {
        aggregators = AggUtil.getAggregators(Arrays.asList(this.aggType),
                Arrays.asList(this.aggClassName), false, factKeyGenerator, null, mergedMinValue,
                this.type);
        isNotNullValue = new boolean[this.aggType.length];
        for (int i = 0; i < aggType.length; i++) {
            if (aggType[i].equals(MolapCommonConstants.DISTINCT_COUNT)) {
                isNotNullValue[i] = true;
            }

        }
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     *
     * @param row row
     */
    private void updateMeasureValue(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                double value = (Double) row[i];
                aggregators[i].agg(value);
            }
        }

    }

    /**
     * Below method will be used to add new row
     *
     * @param row
     */
    private void addNewRow(Object[] row) {
        for (int i = 0; i < aggregators.length; i++) {
            if (null != row[i]) {
                this.isNotNullValue[i] = true;
                double value = (Double) row[i];
                aggregators[i].agg(value);
            }
        }
        prvKey = (byte[]) row[this.keyIndex];

    }

}
