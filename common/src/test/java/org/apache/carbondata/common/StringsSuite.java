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

package org.apache.carbondata.common;

import org.junit.Assert;
import org.junit.Test;

public class StringsSuite {

  @Test(expected = NullPointerException.class)
  public void testMkStringNullString() {
    Strings.mkString(null, ",");
  }

  @Test(expected = NullPointerException.class)
  public void testMkStringNullDelimiter() {
    Strings.mkString(new String[]{"abc"}, null);
  }
  
  @Test
  public void testMkString() {
    String[] strings = new String[]{};
    String output = Strings.mkString(strings, ",");
    Assert.assertTrue(output.length() == 0);

    strings = new String[]{"abc"};
    output = Strings.mkString(strings, ",");
    Assert.assertEquals("abc", output);

    strings = new String[]{"abc", "def"};
    output = Strings.mkString(strings, ",");
    Assert.assertEquals("abc,def", output);

    strings = new String[]{"abc", "def", "ghj"};
    output = Strings.mkString(strings, ",");
    Assert.assertEquals("abc,def,ghj", output);
  }
}
