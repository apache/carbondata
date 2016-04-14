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

package org.carbondata.core.cache.dictionary;

import java.util.List;

/**
 * class that implements methods specific for dictionary data look up
 */
public class ColumnDictionaryInfo extends AbstractColumnDictionaryInfo {

    /**
     * This method will find and return the surrogate key for a given dictionary value
     * Applicable scenario:
     * 1. Incremental data load : Dictionary will not be generated for existing values. For
     * that values have to be looked up in the existing dictionary cache.
     * 2. Filter scenarios where from value surrogate key has to be found.
     *
     * @param value dictionary value
     * @return if found returns key else 0
     */
    @Override public int getSurrogateKey(String value) {
        return 0;
    }

    /**
     * This method will find and return the surrogate key for a given dictionary value
     * Applicable scenario:
     * 1. Incremental data load : Dictionary will not be generated for existing values. For
     * that values have to be looked up in the existing dictionary cache.
     * 2. Filter scenarios where from value surrogate key has to be found.
     *
     * @param value dictionary value as byte array
     * @return if found returns key else 0
     */
    @Override public int getSurrogateKey(byte[] value) {
        return 0;
    }

    /**
     * This method will find and return the sort index for a given dictionary id.
     * Applicable scenarios:
     * 1. Used in case of order by queries when data sorting is required
     *
     * @param surrogateKey a unique ID for a dictionary value
     * @return if found returns key else 0
     */
    @Override public int getSortedIndex(int surrogateKey) {
        return 0;
    }

    /**
     * This method will find and return the dictionary value from sorted index.
     * Applicable scenarios:
     * 1. Query final result preparation in case of order by queries:
     * While convert the final result which will
     * be surrogate key back to original dictionary values this method will be used
     *
     * @param sortedIndex sort index of dictionary value
     * @return value if found else null
     */
    @Override public String getDictionaryValueFromSortedIndex(int sortedIndex) {
        return null;
    }

    /**
     * This method will add a new dictionary chunk to existing list of dictionary chunks
     *
     * @param dictionaryChunk
     */
    @Override public void addDictionaryChunk(List<byte[]> dictionaryChunk) {
        dictionaryChunks.add(dictionaryChunk);
    }
}
