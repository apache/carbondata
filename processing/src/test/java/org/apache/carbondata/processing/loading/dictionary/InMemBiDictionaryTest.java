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

package org.apache.carbondata.processing.loading.dictionary;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.junit.Assert;
import org.junit.Test;

public class InMemBiDictionaryTest {

  /**
   * test pre-created dictionary
   */
  @Test public void testPreCreated() throws Exception {
    Map<Integer, String> map = new HashMap<>();
    map.put(1, "amy");
    map.put(2, "bob");
    BiDictionary<Integer, String> dict = new InMemBiDictionary<>(map);
    Assert.assertEquals(1, dict.getKey("amy").intValue());
    Assert.assertEquals(2, dict.getKey("bob").intValue());
    Assert.assertEquals("amy", dict.getValue(1));
    Assert.assertEquals("bob", dict.getValue(2));
    Assert.assertEquals(2, dict.size());
    try {
      dict.getOrGenerateKey("cat");
      Assert.fail("add dictionary successfully");
    } catch (Exception e) {
      // test pass
    }
  }

  /**
   * test generating dictionary on the fly
   */
  @Test public void testGenerateDict() throws Exception {
    BiDictionary<Integer, String> dict = new InMemBiDictionary<>(
        new DictionaryGenerator<Integer, String>() {
          int sequence = 1;
          @Override
          public Integer generateKey(String value) throws DictionaryGenerationException {
            return sequence++;
          }
        });
    Assert.assertEquals(1, dict.getOrGenerateKey("amy").intValue());
    Assert.assertEquals(2, dict.getOrGenerateKey("bob").intValue());
    Assert.assertEquals(1, dict.getKey("amy").intValue());
    Assert.assertEquals(2, dict.getKey("bob").intValue());
    Assert.assertEquals("amy", dict.getValue(1));
    Assert.assertEquals("bob", dict.getValue(2));
    Assert.assertEquals(2, dict.size());
  }
}
