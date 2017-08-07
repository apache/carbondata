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
import org.apache.carbondata.core.util.CarbonProperty;

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

  @Test public void testvalidateLockType()
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT.equalsIgnoreCase(valueAfterValidation));
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION_DEFAULT
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
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
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT.equalsIgnoreCase(valueAfterValidation));
  }
}
