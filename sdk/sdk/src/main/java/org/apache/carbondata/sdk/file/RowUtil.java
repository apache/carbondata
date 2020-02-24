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

package org.apache.carbondata.sdk.file;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * This row class is used to transfer the row data from one step to other step
 */
public class RowUtil implements Serializable {

  public static String getString(Object[] data, int ordinal) {
    return (String) data[ordinal];
  }

  /**
   * get short type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return short data type data
   */
  public static short getShort(Object[] data, int ordinal) {
    return (short) data[ordinal];
  }

  /**
   * get int data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return int data type data
   */
  public static int getInt(Object[] data, int ordinal) {
    return (Integer) data[ordinal];
  }

  /**
   * get long data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return long data type data
   */
  public static long getLong(Object[] data, int ordinal) {
    return (long) data[ordinal];
  }

  /**
   * get array data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return array data type data
   */
  public static Object[] getArray(Object[] data, int ordinal) {
    Object object = data[ordinal];
    if (object instanceof Object[]) {
      return (Object[]) data[ordinal];
    } else {
      throw new IllegalArgumentException("It's not an array in ordinal of data.");
    }
  }

  /**
   * get double data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return double data type data
   */
  public static double getDouble(Object[] data, int ordinal) {
    return (double) data[ordinal];
  }

  /**
   * get boolean data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return boolean data type data
   */
  public static boolean getBoolean(Object[] data, int ordinal) {
    return (boolean) data[ordinal];
  }

  /**
   * get byte data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return byte data type data
   */
  public static Byte getByte(Object[] data, int ordinal) {
    return (Byte) data[ordinal];
  }

  /**
   * get float data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return float data type data
   */
  public static float getFloat(Object[] data, int ordinal) {
    return (float) data[ordinal];
  }

  /**
   * get varchar data type data by ordinal
   * This is for C++ SDK
   * JNI don't support varchar, so carbon convert decimal to string
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return string data type data
   */
  public static String getVarchar(Object[] data, int ordinal) {
    return (String) data[ordinal];
  }

  /**
   * get decimal data type data by ordinal
   * This is for C++ SDK
   * JNI don't support Decimal, so carbon convert decimal to string
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return string data type data
   */
  public static String getDecimal(Object[] data, int ordinal) {
    return ((BigDecimal) data[ordinal]).toString();
  }

  /**
   * get binary data type data by ordinal
   *
   * @param data carbon row data
   * @param ordinal the data index of Row
   * @return byte data type data
   */
  public static byte[] getBinary(Object[] data, int ordinal) {
    return (byte[]) data[ordinal];
  }

}
