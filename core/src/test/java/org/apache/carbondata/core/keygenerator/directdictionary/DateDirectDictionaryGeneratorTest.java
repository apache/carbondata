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

package org.apache.carbondata.core.keygenerator.directdictionary;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test case for the DateDirectDictionaryGenerator
 */
public class DateDirectDictionaryGeneratorTest {
  private DirectDictionaryGenerator dictionaryGenerator = null;

  @Before public void setUp() throws Exception {
    TimeStampDirectDictionaryGenerator generator = new TimeStampDirectDictionaryGenerator();
    dictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(DataTypes.DATE,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
  }

  /**
   * The generated surrogateKey must be greater than 1
   *
   * @throws Exception
   */
  @Test public void lowerBoundaryValueTest() {
    int surrogateKey = dictionaryGenerator.generateDirectSurrogateKey("0001-01-01");
    Assert.assertTrue(surrogateKey > 1);
  }
  /**
   * The generated surrogateKey must be greater than 1
   *
   * @throws Exception
   */
  @Test public void lowerBoundaryInvalidValueTest() {
    int surrogateKey = dictionaryGenerator.generateDirectSurrogateKey("0001-01-00");
    Assert.assertTrue(surrogateKey == 1);
  }

  /**
   * The generated surrogateKey must be greater than 1
   *
   * @throws Exception
   */
  @Test public void upperBoundaryValueTest() {
    int surrogateKey = dictionaryGenerator.generateDirectSurrogateKey("9999-12-31");
    Assert.assertTrue(surrogateKey > 1);
  }

  /**
   * The generated surrogateKey must be greater than 1
   *
   * @throws Exception
   */
  @Test public void upperBoundaryInvalidValueTest() {
    int surrogateKey = dictionaryGenerator.generateDirectSurrogateKey("10000-12-31");
    Assert.assertTrue(surrogateKey == 1);
  }

  @After public void tearDown() throws Exception {
    dictionaryGenerator = null;
  }
}
