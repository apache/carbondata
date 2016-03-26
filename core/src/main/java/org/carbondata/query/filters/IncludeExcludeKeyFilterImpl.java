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
public class IncludeExcludeKeyFilterImpl extends KeyFilterImpl {

    private int[] dimensionOffsetExclude;

    private byte[][][] excludeFilters;

    private int[][] excludeRanges;

    private byte[][] maxKeyExclude;

    private int[] lengthsExc;

    private byte[][] tempsExc = null;

    public IncludeExcludeKeyFilterImpl(InMemFilterModel filterModel, KeyGenerator keyGenerator,
            long[] maxKey) {
        this.filterModel = filterModel;
        this.keyGenerator = keyGenerator;
        dimensionOffsetExclude = filterModel.getColExcludeDimOffset();
        excludeFilters = filterModel.getExcludeFilter();
        excludeRanges = filterModel.getMaskedByteRangesExclude();
        maxKeyExclude = filterModel.getMaxKeyExclude();

        dimensionOffset = filterModel.getColIncludeDimOffset();
        includeFilters = filterModel.getFilter();
        maxKeyBytes = filterModel.getMaxKey();
        ranges = filterModel.getMaskedByteRanges();
        temps = new byte[dimensionOffset.length][];
        lengths = new int[dimensionOffset.length];
        createTempByteAndRangeLengths(temps, lengths, dimensionOffset, ranges);

        tempsExc = new byte[dimensionOffsetExclude.length][];
        lengthsExc = new int[dimensionOffsetExclude.length];
        createTempByteAndRangeLengths(tempsExc, lengthsExc, dimensionOffsetExclude, excludeRanges);
        //        this.optimizer = new IncludeExcludeScanOptimizerImpl(maxKey, filterModel.getIncludePredicateKeys(),
        //                filterModel.getExcludePredicateKeys(), keyGenerator,filterModel.getGroups());
    }

    /**
     * @see KeyFilterImpl#filterKey(KeyValue)
     */
    @Override public boolean filterKey(KeyValue key) {
        //
        if (super.filterKey(key)) {

            byte[][] notAllowedValuesForDim;
            for (int i = 0; i < dimensionOffsetExclude.length; i++) {
                //
                notAllowedValuesForDim = excludeFilters[dimensionOffsetExclude[i]];
                int searchresult =
                        binarySearch(notAllowedValuesForDim, key.backKeyArray, key.keyOffset,
                                maxKeyExclude[dimensionOffsetExclude[i]],
                                excludeRanges[dimensionOffsetExclude[i]], tempsExc[i],
                                lengthsExc[i]);
                if (searchresult >= 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * @see KeyFilterImpl#filterKey(KeyValue)
     */
    @Override public boolean filterKey(byte[] key) {
        //
        if (super.filterKey(key)) {

            byte[][] notAllowedValuesForDim;
            int[] searchResult = new int[dimensionOffsetExclude.length];
            for (int i = 0; i < dimensionOffsetExclude.length; i++) {
                //
                notAllowedValuesForDim = excludeFilters[dimensionOffsetExclude[i]];
                searchResult[i] = binarySearch(notAllowedValuesForDim, key, 0,
                        maxKeyExclude[dimensionOffsetExclude[i]],
                        excludeRanges[dimensionOffsetExclude[i]], tempsExc[i], lengthsExc[i]);

            }
            for (int i = 0; i < searchResult.length; i++) {
                if (searchResult[i] < 0) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }
}
