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
package org.apache.carbondata.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Class to convert the java object to spark object
 */
public class SparkDataTypeAdapter {

  /**
   * singleton instance
   */
  private static final SparkDataTypeAdapter INSTANCE = new SparkDataTypeAdapter();

  /**
   * adapter to convert Strirg to utf8string
   */
  private Method utf8StringAdapter;

  private SparkDataTypeAdapter() {
    Class<?> utf8String = null;
    try {
      utf8String = Class.forName("org.apache.spark.unsafe.types.UTF8String");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    try {
      utf8StringAdapter = utf8String.getMethod("fromString", new Class[] { String.class });
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Below method will be used to get the adapter instance
   *
   * @return
   */
  public static SparkDataTypeAdapter getInstance() {
    return INSTANCE;
  }

  /**
   * Below method will be used to convert the java string to
   * utf8string
   *
   * @param data string data
   * @return utf8string data
   */
  public Object convertToSparkUTF8String(String data) {
    try {
      return utf8StringAdapter.invoke(null, data);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Below method will be used to get the java big decimal object to spark big decimal object
   *
   * @param data String data
   * @return
   */
  public Object getSparkBigDecimal(String data) {
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data);
    scala.math.BigDecimal scalaDecVal = new scala.math.BigDecimal(javaDecVal);
    Class sparkDecimalClass = null;
    try {
      sparkDecimalClass = Class.forName("org.apache.spark.sql.types.Decimal");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    Constructor sparkDecimalDefaultConstructor = null;
    try {
      sparkDecimalDefaultConstructor = sparkDecimalClass.getConstructor();
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
    Object newInstance = null;
    try {
      newInstance = sparkDecimalDefaultConstructor.newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }

    Method setMethod = null;
    try {
      setMethod =
          newInstance.getClass().getMethod("set", new Class[] { scala.math.BigDecimal.class });
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }

    try {
      return setMethod.invoke(newInstance, scalaDecVal);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}
