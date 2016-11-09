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

package org.apache.carbondata.core.datastorage.store.columnar;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UnBlockIndexerTest {

    @Test
    public void testUncompressIndex() {
        int[] indexData = {0, 1};
        int[] indexMap = {};

        int[] result = UnBlockIndexer.uncompressIndex(indexData, indexMap);
        assertThat(result, is(equalTo(indexData)));
    }

    @Test
    public void testUncompressIndexWithIndexMap() {
        int[] indexData = {0, 9, 1, 8};
        int[] indexMap = {2};

        int[] result = UnBlockIndexer.uncompressIndex(indexData, indexMap);
        assertThat(result, is(equalTo(new int[]{0, 9, 1, 2, 3, 4, 5, 6, 7, 8})));
    }

    @Test
    public void testUncompressData() {
        byte[] data = {2};
        int[] index = {0, 2};
        int keyLen = 1;

        byte[] result = UnBlockIndexer.uncompressData(data, index, keyLen);

        assertThat(result, is(equalTo(new byte[]{2, 2})));
    }

    @Test
    public void testUncompressDataWithIndexZero() {
        byte[] data = {2};
        int[] index = {};
        int keyLen = 1;

        byte[] result = UnBlockIndexer.uncompressData(data, index, keyLen);

        assertThat(result, is(equalTo(data)));
    }
}
