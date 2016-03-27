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

package org.carbondata.query.executer.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.schema.metadata.DimColumnFilterInfo;

public class QueryFilterInfo {

    private Map<Dimension, List<DimColumnFilterInfo>> dimensionFilter;

    private Map<Measure, List<Double>> measureFilter;

    public QueryFilterInfo() {
        dimensionFilter = new HashMap<Dimension, List<DimColumnFilterInfo>>(20);
        measureFilter = new HashMap<Measure, List<Double>>(20);
    }

    public Map<Dimension, List<DimColumnFilterInfo>> getDimensionFilter() {
        return dimensionFilter;
    }

    public void addDimensionFilter(Dimension dim, DimColumnFilterInfo filterValues) {
        List<DimColumnFilterInfo> currentFilterValues = dimensionFilter.get(dim);
        if (null == currentFilterValues) {
            currentFilterValues = new ArrayList<DimColumnFilterInfo>(20);
            currentFilterValues.add(filterValues);
            dimensionFilter.put(dim, currentFilterValues);
        } else {
            currentFilterValues.add(filterValues);
        }
    }

    public void addMeasureFilter(Measure msr, List<Double> filterValues) {
        List<Double> list = measureFilter.get(msr);
        if (null == list) {
            measureFilter.put(msr, filterValues);
        } else {
            list.addAll(filterValues);
        }
    }

    public Map<Measure, List<Double>> getMeasureFilter() {
        return measureFilter;
    }

}
