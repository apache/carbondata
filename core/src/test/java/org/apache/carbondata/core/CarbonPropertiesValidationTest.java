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
import org.apache.carbondata.core.api.CarbonProperties;

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
    CarbonProperties.getInstance().addProperty("carbon.lock.type", "xyz");
    String valueBeforeValidation = CarbonProperties.LOCK_TYPE.getOrDefault();
    validateMethodType.invoke(carbonProperties);
    String valueAfterValidation = CarbonProperties.LOCK_TYPE.getOrDefault();
    assertTrue(!valueBeforeValidation.equals(valueAfterValidation));
    assertTrue(CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL.equalsIgnoreCase(valueAfterValidation));
  }

}
