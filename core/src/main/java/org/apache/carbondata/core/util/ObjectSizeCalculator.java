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

package org.apache.carbondata.core.util;

import java.lang.reflect.Method;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

/**
 * This wrapper class is created so that core doesnt have direct dependency on spark
 * TODO: Need to have carbon implementation if carbon needs to be used without spark
 */
public final class ObjectSizeCalculator {
  /**
   * Logger object for the class
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ObjectSizeCalculator.class.getName());

  /**
   * Class of spark to invoke
   */
  private static String className = "org.apache.spark.util.SizeEstimator";

  private static Method estimateMethod = null;

  private static boolean methodAccessible = true;

  /**
   * Invoke the spark's implementation of Object size computation
   * return the default value passed if function cannot be invoked
   * @param anObject
   * @param defValue
   * @return
   */
  public static long estimate(Object anObject, long defValue) {
    try {
      if (methodAccessible) {
        if (null == estimateMethod) {
          estimateMethod = Class.forName(className).getMethod("estimate", Object.class);
          estimateMethod.setAccessible(true);
        }
        return (Long) estimateMethod.invoke(null, anObject);
      } else {
        return defValue;
      }
    } catch (Throwable ex) {
      // throwable is being caught as external interface is being invoked through reflection
      // and runtime exceptions might get thrown
      LOGGER.error("Could not access method SizeEstimator:estimate.Returning default value", ex);
      methodAccessible = false;
      return defValue;
    }
  }
}
