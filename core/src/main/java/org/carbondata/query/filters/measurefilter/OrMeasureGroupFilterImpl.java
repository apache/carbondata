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

package org.carbondata.query.filters.measurefilter;

import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * It is "OR" measure group, so any one condition is true then it is ok.
 */
public class OrMeasureGroupFilterImpl implements MeasureFilter, MeasureGroupFilter {

    private MeasureFilter[][] measureFilters;

    private int[] msrFilterIndices;

    public OrMeasureGroupFilterImpl(MeasureFilter[][] measureFilters) {
        msrFilterIndices = MeasureFilterUtil.getMsrFilterIndexes(measureFilters);
        this.measureFilters = measureFilters;
    }

    @Override public boolean filter(MeasureAggregator[] msrValue) {
        for (int i = 0; i < msrFilterIndices.length; i++) {
            if (measureFilters[msrFilterIndices[i]][0].filter(msrValue)) {
                return true;
            }
        }
        return false;
    }

    @Override public boolean filter(double[] msrValue, int msrStartIndex) {
        for (int i = 0; i < msrFilterIndices.length; i++) {
            if (measureFilters[msrFilterIndices[i]][0].filter(msrValue, msrStartIndex)) {
                return true;
            }
        }
        return false;
    }

    @Override public boolean isMsrFilterEnabled() {
        return msrFilterIndices.length > 0;
    }

}
