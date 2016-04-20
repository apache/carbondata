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
package org.carbondata.integration.spark.load;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;

/**
 * The class prepares the column sort info ie sortIndex
 * and inverted sort index info
 */
public class CarbonDictionarySortInfoPreparator {

    /**
     * Carbon store path
     */
    private String carbonStorePath;

    /**
     * carbon table identifier instance to identify the databaseName & tableName
     */
    private CarbonTableIdentifier carbonTableIdentifier;

    public CarbonDictionarySortInfoPreparator(String carbonStorePath,
            CarbonTableIdentifier carbonTableIdentifier) {
        this.carbonStorePath = carbonStorePath;
        this.carbonTableIdentifier = carbonTableIdentifier;
    }

    /**
     * The method returns the column Sort Info
     *
     * @param columnName column name
     */
    public CarbonDictionarySortInfo getDictionarySortInfo(String columnName) {
        CacheProvider cacheProviderInstance = CacheProvider.getInstance();
        Cache reverseDictionaryCache =
                cacheProviderInstance.createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath);

        DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier =
                new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnName);
        Dictionary dictionary =
                (Dictionary) reverseDictionaryCache.get(dictionaryColumnUniqueIdentifier);
        //TODO null check to be handled on dictionary
        CarbonDictionarySortModel[] dictionarySortModels =
                prepareDictionarySortModels(dictionary.getDictionaryChunks());
        return createColumnSortInfo(dictionarySortModels);
    }

    /**
     * The method prepares the sort_index and sort_index_inverted data
     *
     * @param dictionarySortModels
     */
    private CarbonDictionarySortInfo createColumnSortInfo(
            CarbonDictionarySortModel[] dictionarySortModels) {

        //Sort index after members are sorted
        int[] sortIndex;
        //inverted sort index to get the member
        int[] sortIndexInverted;

        Arrays.sort(dictionarySortModels);
        sortIndex = new int[dictionarySortModels.length];
        sortIndexInverted = new int[dictionarySortModels.length];

        for (int i = 0; i < dictionarySortModels.length; i++) {
            CarbonDictionarySortModel dictionarySortModel = dictionarySortModels[i];
            sortIndex[i] = dictionarySortModel.getKey();
            // the array index starts from 0 therefore -1 is done to avoid wastage
            // of 0th index in array and surrogate key starts from 1 there 1 is added to i
            // which is a counter starting from 0
            sortIndexInverted[dictionarySortModel.getKey() - 1] = i + 1;
        }
        dictionarySortModels = null;
        List<Integer> sortIndexList = convertToList(sortIndex);
        List<Integer> sortIndexInvertedList = convertToList(sortIndexInverted);
        return new CarbonDictionarySortInfo(sortIndexList, sortIndexInvertedList);
    }

    /**
     * The method converts the int[] to List<Integer>
     *
     * @param data
     * @return
     */
    private List<Integer> convertToList(int[] data) {
        Integer[] wrapperType = ArrayUtils.toObject(data);
        return Arrays.asList(wrapperType);
    }

    /**
     * The method returns the array of CarbonDictionarySortModel
     *
     * @param dictionaryChunksWrapper The wrapper wraps the list<list<bye[]>> and provide the
     *                                iterator to retrieve the chunks members.
     * @return CarbonDictionarySortModel[] CarbonDictionarySortModel[] the model
     * CarbonDictionarySortModel contains the  member's surrogate and
     * its byte value
     */
    private CarbonDictionarySortModel[] prepareDictionarySortModels(
            DictionaryChunksWrapper dictionaryChunksWrapper) {

        CarbonDictionarySortModel[] dictionarySortModels =
                new CarbonDictionarySortModel[dictionaryChunksWrapper.getSize()];
        int surrogate = 1;
        while (dictionaryChunksWrapper.hasNext()) {
            CarbonDictionarySortModel dictionarySortModel =
                    new CarbonDictionarySortModel(surrogate, dictionaryChunksWrapper.next());
            dictionarySortModels[surrogate - 1] = dictionarySortModel;
            surrogate++;
        }
        return dictionarySortModels;
    }
}
