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

package org.carbondata.query.queryinterface.query.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.query.queryinterface.query.CarbonQuery;

/**
 * It is Axis class, it can be row,column or slice axis.It contains all information of query depends on levels and measures added in query.
 */
public class Axis implements Serializable {
    private static final long serialVersionUID = -574689684553603640L;

    private List<CarbonLevelHolder> dims = new ArrayList<CarbonLevelHolder>(10);

    /**
     * Add query details to this axis.
     *
     * @param level
     * @param sortType
     * @param msrFilters
     * @param dimLevelFilter
     */
    public void add(CarbonLevel level, CarbonQuery.SortType sortType,
            List<CarbonMeasureFilter> msrFilters, CarbonDimensionLevelFilter dimLevelFilter) {
        CarbonLevelHolder holder = new CarbonLevelHolder(level, sortType);
        holder.setMsrFilters(msrFilters);
        holder.setDimLevelFilter(dimLevelFilter);
        dims.add(holder);
    }

    /**
     * Get dims
     *
     * @return the dims
     */
    public List<CarbonLevelHolder> getDims() {
        return dims;
    }
}
