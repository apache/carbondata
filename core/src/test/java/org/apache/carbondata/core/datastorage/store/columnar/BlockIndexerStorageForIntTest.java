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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockIndexerStorageForIntTest {

  @Test public void testBlockIndexerStorageForIntConstructor() {
    byte[][] data = { { 85, -81, -31, 42 }, { 85, -79, 50, -86 } };
    BlockIndexerStorageForInt storageForInt =
        new BlockIndexerStorageForInt(data, false, false, true);

    boolean expectedAlreadyStored = false;
    assertThat(storageForInt.isAlreadySorted(), is(equalTo(expectedAlreadyStored)));

    int[] expectedDataAfterComp = { 0, 1 };
    assertThat(storageForInt.getDataAfterComp(), is(equalTo(expectedDataAfterComp)));

    int[] expectedIndexMap = {};
    assertThat(storageForInt.getIndexMap(), is(equalTo(expectedIndexMap)));

    assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));

    assertThat(storageForInt.getDataIndexMap(), is(nullValue()));

    int expectedTotalSize = 0;
    assertThat(storageForInt.getTotalSize(), is(equalTo(expectedTotalSize)));
  }

  @Test public void testBlockIndexerStorageForIntConstructorCompressData() {
    byte[][] data = { { 2 }, { 3 } };
    BlockIndexerStorageForInt storageForInt =
        new BlockIndexerStorageForInt(data, true, false, true);

    boolean expectedAlreadyStored = false;
    assertThat(storageForInt.isAlreadySorted(), is(equalTo(expectedAlreadyStored)));

    int[] expectedDataAfterComp = { 0, 1 };
    assertThat(storageForInt.getDataAfterComp(), is(equalTo(expectedDataAfterComp)));

    int[] expectedIndexMap = {};
    assertThat(storageForInt.getIndexMap(), is(equalTo(expectedIndexMap)));

    assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));

    int[] expectedDataIndexMap = {};
    assertThat(storageForInt.getDataIndexMap(), is(equalTo(expectedDataIndexMap)));

    int expectedTotalSize = 2;
    assertThat(storageForInt.getTotalSize(), is(equalTo(expectedTotalSize)));

    byte[] expectedMax = {3};
    assertThat(storageForInt.getMax(), is(equalTo(expectedMax)));

    byte[] expectedMin = {2};
    assertThat(storageForInt.getMin(), is(equalTo(expectedMin)));
  }

  @Test public void testBlockIndexerStorageForIntConstructorNoDictionary() {
    byte[][] data =
        { { 0, 8, 65, 83, 68, 54, 57, 54, 52, 51 }, { 0, 8, 65, 83, 68, 52, 50, 56, 57, 50 } };
    BlockIndexerStorageForInt storageForInt =
        new BlockIndexerStorageForInt(data, false, true, true);

    boolean expectedAlreadyStored = false;
    assertThat(storageForInt.isAlreadySorted(), is(equalTo(expectedAlreadyStored)));

    int[] expectedDataAfterComp = { 1, 0 };
    assertThat(storageForInt.getDataAfterComp(), is(equalTo(expectedDataAfterComp)));

    int[] expectedIndexMap = {};
    assertThat(storageForInt.getIndexMap(), is(equalTo(expectedIndexMap)));

    assertThat(storageForInt.getKeyBlock(), is(equalTo(data)));

    assertThat(storageForInt.getDataIndexMap(), is(nullValue()));

    int expectedTotalSize = 0;
    assertThat(storageForInt.getTotalSize(), is(equalTo(expectedTotalSize)));
  }
}
