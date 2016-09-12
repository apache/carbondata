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
package org.apache.carbondata.core.keygenerator.directdictionary.timestamp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test case for the TimeStampDirectDictionaryGenerator
 */
public class TimeStampDirectDictionaryGeneratorTest {
  private String memberString = "2015-10-20 12:30:01";
  private int surrogateKey = -1;

  @Before public void setUp() throws Exception {
    TimeStampDirectDictionaryGenerator generator = TimeStampDirectDictionaryGenerator.instance;
    surrogateKey = generator.generateDirectSurrogateKey("2015-10-20 12:30:01");
  }

  /**
   * The invalid input date format should return -1, if proper format then should return the  ve integer value
   *
   * @throws Exception
   */
  @Test public void generateDirectSurrogateKey() throws Exception {
    TimeStampDirectDictionaryGenerator generator = TimeStampDirectDictionaryGenerator.instance;
    // default timestamp format is "yyyy-MM-dd HH:mm:ss" and the data being passed
    // in "dd/MM/yyyy" so the parsing should fail and method should return 1.
    int key = generator.generateDirectSurrogateKey("20/12/2014");
    Assert.assertEquals(1, key);
    key = generator.generateDirectSurrogateKey("2015-10-20 12:30:01");
    Assert.assertEquals(surrogateKey, key);

  }

  /**
   * The memberString should be retrieved from the actual surrogate key
   *
   * @throws Exception
   */
  @Test public void getValueFromSurrogate() throws Exception {
    TimeStampDirectDictionaryGenerator generator = TimeStampDirectDictionaryGenerator.instance;
    long valueFromSurrogate = (long) generator.getValueFromSurrogate(surrogateKey);
    Date date = new Date(valueFromSurrogate / 1000);
    SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    timeParser.setLenient(false);
    String actualValue = timeParser.format(date);
    Assert.assertEquals(memberString, actualValue);
  }

  /**
   * The memberString should be retrieved from the actual surrogate key
   *
   * @throws Exception
   */
  @Test public void lowerBoundaryValueTest() throws Exception {
    TimeStampDirectDictionaryGenerator generator = TimeStampDirectDictionaryGenerator.instance;
    long valueFromSurrogate = (long) generator.getValueFromSurrogate(2);
    Date date = new Date(valueFromSurrogate / 1000);
    SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    timeParser.setLenient(false);
    timeParser.setTimeZone(TimeZone.getTimeZone("IST"));
    String actualValue = timeParser.format(date);
    Assert.assertEquals("1970-01-01 05:30:00", actualValue);
  }

  /**
   * The memberString should be retrieved from the actual surrogate key
   *
   * @throws Exception
   */
  @Test public void upperBoundaryValueTest() throws Exception {
    TimeStampDirectDictionaryGenerator generator = TimeStampDirectDictionaryGenerator.instance;
    int surrogateKey = generator.generateDirectSurrogateKey("2038-01-01 05:30:00");
    long valueFromSurrogate = (long) generator.getValueFromSurrogate(surrogateKey);
    Date date = new Date(valueFromSurrogate / 1000);
    SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    timeParser.setLenient(false);
    String actualValue = timeParser.format(date);
    Assert.assertEquals("2038-01-01 05:30:00", actualValue);
  }
}
