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

package org.apache.carbondata.core.datastore.page;

/**
 * Utility methods to converts to primitive types only column page data decode.
 */
public class ColumnPageByteUtil {

  public static int toInt(byte[] bytes, int offset) {
    return (((int) bytes[offset + 3] & 0xff) << 24) + (((int) bytes[offset + 2] & 0xff) << 16) + (
        ((int) bytes[offset + 1] & 0xff) << 8) + ((int) bytes[offset] & 0xff);
  }

  public static short toShort(byte[] bytes, int offset) {
    return (short) ((((int) bytes[offset + 1] & 0xff) << 8) + ((int) bytes[offset] & 0xff));
  }

  public static double toDouble(byte[] bytes, int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset));
  }

  public static float toFloat(byte[] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset));
  }

  public static long toLong(byte[] bytes, int offset) {
    return ((((long) bytes[offset + 7]) << 56) | (((long) bytes[offset + 6] & 0xff) << 48) | (
        ((long) bytes[offset + 5] & 0xff) << 40) | (((long) bytes[offset + 4] & 0xff) << 32) | (
        ((long) bytes[offset + 3] & 0xff) << 24) | (((long) bytes[offset + 2] & 0xff) << 16) | (
        ((long) bytes[offset + 1] & 0xff) << 8) | (((long) bytes[offset] & 0xff)));
  }

}
