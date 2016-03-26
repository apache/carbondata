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

package org.carbondata.query.executer.impl.comparator;

import java.math.BigDecimal;
import java.util.Comparator;

import org.carbondata.query.executer.Tuple;

/**
 * Class Description : Comparator responsible for based on measures
 * Version 1.0
 */
public class MeasureComparatorTuple implements Comparator<Tuple> {
    /**
     * msrIndex
     */
    private int msrIndex;

    /**
     * sortOrder
     */
    private int sortOrder;

    /**
     * MeasureComparator Constructor
     *
     * @param msrIndex
     * @param sortOrder
     */
    public MeasureComparatorTuple(int msrIndex, int sortOrder) {
        this.msrIndex = msrIndex;
        this.sortOrder = sortOrder;
    }

    /**
     * This method will be used to compare two byte array
     *
     * @param o1
     * @param o2
     */
    @Override public int compare(Tuple t1, Tuple t2) {
        int cmp = 0;
        if (this.msrIndex < 0 || this.msrIndex >= t1.getMeasures().length) {
            return cmp;
        }

        if (t1.getMeasures()[this.msrIndex].toString().contains("Long")) {
            long msrValue1L = t1.getMeasures()[this.msrIndex].getLongValue();
            long msrValue2L = t2.getMeasures()[this.msrIndex].getLongValue();
            if (msrValue1L < msrValue2L) {
                cmp = -1;
            } else if (msrValue1L > msrValue2L) {
                cmp = 1;
            }
        } else if (t1.getMeasures()[this.msrIndex].toString().contains("Decimal")) {
            BigDecimal msrValue1B = t1.getMeasures()[this.msrIndex].getBigDecimalValue();
            BigDecimal msrValue2B = t2.getMeasures()[this.msrIndex].getBigDecimalValue();
            cmp = msrValue1B.compareTo(msrValue2B);
        } else {
            double msrValue1D = t1.getMeasures()[this.msrIndex].getDoubleValue();
            double msrValue2D = t2.getMeasures()[this.msrIndex].getDoubleValue();
            if (msrValue1D < msrValue2D) {
                cmp = -1;
            } else if (msrValue1D > msrValue2D) {
                cmp = 1;
            }
        }
        if (this.sortOrder == 1 || this.sortOrder == 3) {
            cmp *= -1;
        }
        return cmp;
    }

}
