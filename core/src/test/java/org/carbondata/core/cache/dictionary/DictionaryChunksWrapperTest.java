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

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for dictionary chuck wrapper
 */
public class DictionaryChunksWrapperTest {

    private List<List<byte[]>> dictionaryChuncks;
    private DictionaryChunksWrapper dictionaryChunksWrapper;
    private List<byte[]> expectedData;

    /**
     * init resources
     *
     * @throws Exception
     */
    @Before public void setUp() throws Exception {
        dictionaryChuncks = prepareData();
        expectedData = prepareExpectedData();
        dictionaryChunksWrapper = new DictionaryChunksWrapper(dictionaryChuncks);
    }

    /**
     * The method returns the list<List<byte[]>>
     *
     * @return
     */
    private List<List<byte[]>> prepareData() {
        List<List<byte[]>> dictionaryChunks = new ArrayList<>();
        List<byte[]> chunks = new ArrayList<>();
        chunks.add("d".getBytes());
        chunks.add("b".getBytes());
        chunks.add("c".getBytes());
        chunks.add("a".getBytes());
        dictionaryChunks.add(chunks);
        return dictionaryChunks;
    }

    private List<byte[]> prepareExpectedData() {
        List<byte[]> chunks = new ArrayList<>();
        chunks.add("d".getBytes());
        chunks.add("b".getBytes());
        chunks.add("c".getBytes());
        chunks.add("a".getBytes());
        return chunks;
    }

    /**
     * release resources
     *
     * @throws Exception
     */
    @After public void tearDown() throws Exception {
        dictionaryChunksWrapper = null;
        expectedData = null;
        dictionaryChuncks = null;
    }

    /**
     * The test the next method
     *
     * @throws Exception
     */
    @Test public void testNext() throws Exception {
        List<byte[]> actual = new ArrayList<>();
        while (dictionaryChunksWrapper.hasNext()) {
            actual.add(dictionaryChunksWrapper.next());
        }
        Assert.assertEquals(expectedData.size(), actual.size());
        for (int i = 0; i < expectedData.size(); i++) {
            Assert.assertArrayEquals(actual.get(i), expectedData.get(i));
        }
    }

    /**
     * The method validate the size
     *
     * @throws Exception
     */
    @Test public void getSize() throws Exception {
        int size = dictionaryChunksWrapper.getSize();
        Assert.assertEquals("", 4, size);
    }
}