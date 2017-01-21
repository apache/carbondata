/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.cache.dictionary;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ColumnReverseDictionaryInfoTest {

  private static ColumnReverseDictionaryInfo columnReverseDictionaryInfo;

  @BeforeClass public static void setUp() {
    columnReverseDictionaryInfo = new ColumnReverseDictionaryInfo();
    columnReverseDictionaryInfo.addDictionaryChunk(Arrays.asList("a".getBytes()));
    columnReverseDictionaryInfo.addDictionaryChunk(Arrays.asList("b".getBytes()));
  }

  @Test public void testToGetSurrogateKey() {
    int key1 = columnReverseDictionaryInfo.getSurrogateKey("a".getBytes());
    int key2 = columnReverseDictionaryInfo.getSurrogateKey("b".getBytes());
    int[] surrogateKey = { key1, key2 };
    int[] expectedKeys = { 1, 2 };
    assertThat(surrogateKey, is(equalTo(expectedKeys)));
  }

  @Test public void testToGetSurrogateKeyForInvalidKey() {
    int key = columnReverseDictionaryInfo.getSurrogateKey("c".getBytes());
    int expectedKey = -1;
    assertThat(key, is(equalTo(expectedKey)));
  }
}
