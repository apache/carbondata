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

package org.apache.carbondata.core.localdictionary;

import org.apache.carbondata.core.localdictionary.dictionaryholder.DictionaryStore;
import org.apache.carbondata.core.localdictionary.dictionaryholder.MapBasedDictionaryStore;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;

import org.junit.Assert;
import org.junit.Test;

public class TestDictionaryStore {

  @Test
  public void testDictionaryStoreWithinThreshold() {
    DictionaryStore dictionaryStore = new MapBasedDictionaryStore(10);
    for (int i = 0; i < 10; i++) {
      try {
        dictionaryStore.putIfAbsent((i+"").getBytes());
        Assert.assertTrue(true);
      } catch (DictionaryThresholdReachedException e) {
        Assert.assertTrue(false);
        break;
      }
    }
  }

  @Test
  public void testDictionaryStoreWithMoreThanThreshold() {
    DictionaryStore dictionaryStore = new MapBasedDictionaryStore(10);
    boolean isException = false;
    for (int i = 0; i < 15; i++) {
      try {
        dictionaryStore.putIfAbsent((i+"").getBytes());
      } catch (DictionaryThresholdReachedException e) {
        isException = true;
        break;
      }
    }
    Assert.assertTrue(isException);
    Assert.assertTrue(dictionaryStore.isThresholdReached());
  }
}
