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

package org.apache.carbondata.core;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Method to test the carbon common constant configurations.
 */
public class CarbonPropertiesValidationTest extends TestCase {

  CarbonProperties carbonProperties;

  @Override public void setUp() throws Exception {
    carbonProperties = CarbonProperties.getInstance();
  }

  @Test public void testValidateLockType()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType = carbonProperties.getClass().getDeclaredMethod("validateLockType");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.LOCK_TYPE, "xyz");
    String valueBeforeValidation = carbonProperties.getProperty(CarbonCommonConstants.LOCK_TYPE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation = carbonProperties.getProperty(CarbonCommonConstants.LOCK_TYPE);
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableUnsafeSort()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableUnsafeSort");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableOffHeapSort()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableOffHeapSort");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "True");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("true"));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("false"));
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "xyz");
    assert (carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
        .equalsIgnoreCase("true"));
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCustomBlockDistribution()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCustomBlockDistribution");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue("false"
        .equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateEnableVectorReader()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateEnableVectorReader");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCarbonCSVReadBufferSizeByte()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCarbonCSVReadBufferSizeByte");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "xyz");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateCarbonCSVReadBufferSizeByteRange()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateCarbonCSVReadBufferSizeByte");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10485761");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10240");
    valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE, "10239");
    valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateHandoffSize() {
    assertEquals(CarbonCommonConstants.HANDOFF_SIZE_DEFAULT, carbonProperties.getHandoffSize());
    long newSize = 1024L * 1024 * 100;
    carbonProperties.addProperty(CarbonCommonConstants.HANDOFF_SIZE, "" + newSize);
    assertEquals(newSize, carbonProperties.getHandoffSize());
  }

  @Test public void testValidateTimeStampFormat()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType = carbonProperties.getClass()
        .getDeclaredMethod("validateTimeFormatKey", new Class[] { String.class, String.class });
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "agdgaJIASDG667");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT);
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        .equalsIgnoreCase(valueAfterValidation));
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        "yyyy-MM-dd hh:mm:ss");
    validateMethodType.invoke(carbonProperties, CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
    assertEquals("yyyy-MM-dd hh:mm:ss",
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT));
  }

  @Test public void testValidateSortFileWriteBufferSize()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateSortFileWriteBufferSize");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE, "test");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
        .equalsIgnoreCase(valueAfterValidation));
  }
  @Test public void testValidateSortIntermediateFilesLimit()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method validateMethodType =
        carbonProperties.getClass().getDeclaredMethod("validateSortIntermediateFilesLimit");
    validateMethodType.setAccessible(true);
    carbonProperties.addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "test");
    String valueBeforeValidation =
        carbonProperties.getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT);
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation =
        carbonProperties.getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT);
    assertTrue(valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE
        .equalsIgnoreCase(valueAfterValidation));
  }

  @Test public void testValidateDynamicSchedulerTimeOut() {
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "2");
    String valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "16");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT, "15");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT);
    assertTrue(valueAfterValidation
        .equals("15"));

  }
  @Test public void testValidateSchedulerMinRegisteredRatio() {
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "0.0");
    String valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "-0.1");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation
        .equals(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO_DEFAULT));
    carbonProperties
        .addProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO, "0.1");
    valueAfterValidation = carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO);
    assertTrue(valueAfterValidation.equals("0.1"));
  }

}
