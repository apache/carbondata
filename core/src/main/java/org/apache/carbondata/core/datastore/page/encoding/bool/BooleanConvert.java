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

package org.apache.carbondata.core.datastore.page.encoding.bool;

import java.util.Locale;

/**
 * convert tools for boolean data type
 */
public class BooleanConvert {

  public static final byte TRUE_VALUE = 1;
  public static final byte FALSE_VALUE = 0;
  /**
   * convert boolean to byte
   *
   * @param data data of boolean data type
   * @return byte type data by convert
   */
  public static byte boolean2Byte(boolean data) {
    return data ? TRUE_VALUE : FALSE_VALUE;
  }

  /**
   * convert byte to boolean
   *
   * @param data byte type data
   * @return boolean type data
   */
  public static boolean byte2Boolean(int data) {
    return data == TRUE_VALUE;
  }

  /**
   * parse boolean, true and false to boolean
   *
   * @param input string type data
   * @return Boolean type data
   */
  public static Boolean parseBoolean(String input) {
    String value = input.toLowerCase(Locale.getDefault());
    if (("false").equals(value)) {
      return Boolean.FALSE;
    } else if (("true").equals(value)) {
      return Boolean.TRUE;
    } else {
      throw new NumberFormatException("Not a valid Boolean type");
    }
  }
}
