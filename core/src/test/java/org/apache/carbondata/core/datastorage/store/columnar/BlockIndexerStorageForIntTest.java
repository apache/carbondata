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

import org.apache.carbondata.core.datastorage.store.columnar.BlockIndexerStorageForInt;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockIndexerStorageForIntTest {

    @Test
    public void testBlockIndexerStorageForIntConstructor() {
        byte[][] data = {{85, -81, -31, 42}, {85, -79, 50, -86}};
        BlockIndexerStorageForInt storageForInt = new BlockIndexerStorageForInt(data, false, false, true);

        assertThat(storageForInt.isAlreadySorted(), is(equalTo(false)));
        assertThat(storageForInt.getDataAfterComp(), is(equalTo(new int[]{0, 1})));
        assertThat(storageForInt.getIndexMap(), is(equalTo(new int[]{})));
        assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));
        assertThat(storageForInt.getDataIndexMap(), is(nullValue()));
        assertThat(storageForInt.getTotalSize(), is(equalTo(0)));
    }

    @Test
    public void testBlockIndexerStorageForIntConstructorCompressData() {
        byte[][] data = {{2}, {3}};
        BlockIndexerStorageForInt storageForInt = new BlockIndexerStorageForInt(data, true, false, true);

        assertThat(storageForInt.isAlreadySorted(), is(equalTo(false)));
        assertThat(storageForInt.getDataAfterComp(), is(equalTo(new int[]{0, 1})));
        assertThat(storageForInt.getIndexMap(), is(equalTo(new int[]{})));
        assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));
        assertThat(storageForInt.getDataIndexMap(), is(equalTo(new int[]{})));
        assertThat(storageForInt.getTotalSize(), is(equalTo(2)));
        assertThat(storageForInt.getMax(), is(equalTo(new byte[]{3})));
        assertThat(storageForInt.getMin(), is(equalTo(new byte[]{2})));
    }

    @Test
    public void testBlockIndexerStorageForIntConstructorNoDictionary() {
        byte[][] data = {{0, 8, 65, 83, 68, 54, 57, 54, 52, 51}, {0, 8, 65, 83, 68, 52, 50, 56, 57, 50}};
        BlockIndexerStorageForInt storageForInt = new BlockIndexerStorageForInt(data, false, true, true);

        assertThat(storageForInt.isAlreadySorted(), is(equalTo(false)));
        assertThat(storageForInt.getDataAfterComp(), is(equalTo(new int[]{1, 0})));
        assertThat(storageForInt.getIndexMap(), is(equalTo(new int[]{})));
        assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));
        assertThat(storageForInt.getDataIndexMap(), is(nullValue()));
        assertThat(storageForInt.getTotalSize(), is(equalTo(0)));
    }
}
