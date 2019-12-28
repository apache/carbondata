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

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper;

import net.jpountz.xxhash.XXHashFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class DictionaryByteArrayWrapperTest {

  static DictionaryByteArrayWrapper dictionaryByteArrayWrapper;
  static DictionaryByteArrayWrapper dictionaryByteArrayWrapper1;

  @BeforeClass public static void setup() {
    byte[] data = "Rahul".getBytes();
    dictionaryByteArrayWrapper = new DictionaryByteArrayWrapper(data);
    dictionaryByteArrayWrapper1 =
        new DictionaryByteArrayWrapper(data, XXHashFactory.fastestInstance().hash32());
  }

  @Test public void equalsTestWithSameObject() {
    Boolean res = dictionaryByteArrayWrapper.equals(dictionaryByteArrayWrapper);
    Assert.assertTrue(res);
  }

  @Test public void equalsTestWithString() {
    Boolean res = dictionaryByteArrayWrapper.equals("Rahul");
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithXxHash32() {
    Boolean res = dictionaryByteArrayWrapper1.equals("Rahul");
    Assert.assertTrue(!res);
  }

  @Test public void equalsTestWithDictionaryByteArrayWrapper() {
    Boolean res =
        dictionaryByteArrayWrapper.equals(new DictionaryByteArrayWrapper("Rahul".getBytes()));
    Assert.assertTrue(res);
  }

  @Test public void equalsTestWithDifferentLength() {
    Boolean res =
        dictionaryByteArrayWrapper.equals(new DictionaryByteArrayWrapper("Rahul ".getBytes()));
    Assert.assertTrue (!res);
  }

  @Test public void hashCodeTest() {
    int res = dictionaryByteArrayWrapper.hashCode();
    int expectedResult = -967077647;
    assertEquals(res, expectedResult);
  }

}
