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

package org.carbondata.query.filters;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.filters.metadata.InMemFilterModel;

/**
 * Filter implementation to scan the data store based on exclude filter applied in the dimensions.
 */
public class IncludeOrKeyFilterImpl extends KeyFilterImpl {

    private int[] dimensionOffsetIncludeOr;

    private byte[][][] includeFiltersOr;

    private int[][] includeRangesOr;

    private byte[][] maxKeyIncludeOr;

    private int[] lengthsOr;

    private byte[][] tempsOr = null;

    public IncludeOrKeyFilterImpl(InMemFilterModel filterModel, KeyGenerator keyGenerator,
            long[] maxKey) {
        this.filterModel = filterModel;
        this.keyGenerator = keyGenerator;
        dimensionOffsetIncludeOr = filterModel.getColIncludeDimOffsetOr();
        includeFiltersOr = filterModel.getIncludeFilterOr();
        includeRangesOr = filterModel.getMaskedByteRangesIncludeOr();
        maxKeyIncludeOr = filterModel.getMaxKeyIncludeOr();

        dimensionOffset = filterModel.getColIncludeDimOffset();
        includeFilters = filterModel.getFilter();
        maxKeyBytes = filterModel.getMaxKey();
        ranges = filterModel.getMaskedByteRanges();
        temps = new byte[dimensionOffset.length][];
        lengths = new int[dimensionOffset.length];
        createTempByteAndRangeLengths(temps, lengths, dimensionOffset, ranges);

        tempsOr = new byte[dimensionOffsetIncludeOr.length][];
        lengthsOr = new int[dimensionOffsetIncludeOr.length];
        createTempByteAndRangeLengths(tempsOr, lengthsOr, dimensionOffsetIncludeOr,
                includeRangesOr);

    }

    /**
     * @see KeyFilterImpl#filterKey(KeyValue)
     */
    @Override public boolean filterKey(KeyValue key) {
        //
        if (!super.filterKey(key)) {

            byte[][] notAllowedValuesForDim;
            for (int i = 0; i < dimensionOffsetIncludeOr.length; i++) {
                //
                notAllowedValuesForDim = includeFiltersOr[dimensionOffsetIncludeOr[i]];
                int searchresult =
                        binarySearch(notAllowedValuesForDim, key.backKeyArray, key.keyOffset,
                                maxKeyIncludeOr[dimensionOffsetIncludeOr[i]],
                                includeRangesOr[dimensionOffsetIncludeOr[i]], tempsOr[i],
                                lengthsOr[i]);
                if (searchresult < 0) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }
}
