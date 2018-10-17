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

package org.apache.carbondata.core.load;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;

public class LoadMetadataDetailsUnitTest {

  private LoadMetadataDetails loadMetadataDetails;
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LoadMetadataDetailsUnitTest.class.getName());

  @Before public void setup() {
    loadMetadataDetails = new LoadMetadataDetails();
  }

  /**
   * This method will test Hashcode which will return 31 if we don't set loadName.
   *
   * @throws Exception
   */

  @Test public void testHashCodeLoadNameNull() throws Exception {
    int expected_result = 31;
    int data = loadMetadataDetails.hashCode();
    assertEquals(expected_result, data);
  }

  @Test public void testHashCodeValueInLoadName() throws Exception {
    loadMetadataDetails.setLoadName("test");
    int data = loadMetadataDetails.hashCode();
    assertNotSame(31, data);
  }

  @Test public void testEqualsObjectIsNotLoadMetadataDetails() throws Exception {
    Object obj = new Object();
    boolean result = loadMetadataDetails.equals(obj);
    assertEquals(false, result);
  }

  @Test public void testEqualsObjectIsNull() throws Exception {
    boolean result = loadMetadataDetails.equals(new Object());
    assertEquals(false, result);
  }

  @Test public void testEqualsObjectIsLoadMetadataDetailsWithoutLoadName() throws Exception {
    LoadMetadataDetails obj = new LoadMetadataDetails();
    boolean result = loadMetadataDetails.equals(obj);
    assertEquals(true, result);
  }

  @Test public void testEqualsObjectIsLoadMetadataDetails() throws Exception {
    loadMetadataDetails.setLoadName("test");
    LoadMetadataDetails obj = new LoadMetadataDetails();
    boolean result = loadMetadataDetails.equals(obj);
    assertEquals(false, result);
  }

  @Test public void testEqualsObjectIsLoadMetadataDetailsLoadNameNull() throws Exception {
    LoadMetadataDetails obj = new LoadMetadataDetails();
    obj.setLoadName("test");
    boolean result = loadMetadataDetails.equals(obj);
    assertEquals(false, result);
  }

  @Test public void testEqualsObjectIsLoadMetadataDetailsLoadNameEqualsObjectLoadName()
      throws Exception {
    loadMetadataDetails.setLoadName("test");
    LoadMetadataDetails obj = new LoadMetadataDetails();
    obj.setLoadName("test");
    boolean result = loadMetadataDetails.equals(obj);
    assertEquals(true, result);
  }

  @Test public void testGetTimeStampWithDate() throws Exception {
    String date = "01-01-2016 00:00:00:000";
    long longVal = loadMetadataDetails.getTimeStamp(date);
    loadMetadataDetails.setLoadStartTime(longVal);
    Long expected_result = getTime(date);
    Long result = loadMetadataDetails.getLoadStartTime();
    assertEquals(expected_result, result);
  }

  public static Long getTime(String date) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP_MILLIS);
    try {
      return simpleDateFormat.parse(date).getTime() * 1000;
    } catch (ParseException e) {
      LOGGER.error("Error while parsing " + date + " " + e.getMessage());
      return null;
    }
  }
}
