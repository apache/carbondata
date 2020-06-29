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

package org.apache.carbondata.core.memory;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

public final class CarbonUnsafe {

  public static final int BYTE_ARRAY_OFFSET;

  public static final int SHORT_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  public static final int FLOAT_ARRAY_OFFSET;

  public static final boolean ISLITTLEENDIAN =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  private static Unsafe unsafe;

  static {
    try {
      Field cause = Unsafe.class.getDeclaredField("theUnsafe");
      cause.setAccessible(true);
      unsafe = (Unsafe) cause.get(null);
    } catch (Throwable var2) {
      unsafe = null;
    }
    if (unsafe != null) {
      BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
      SHORT_ARRAY_OFFSET = unsafe.arrayBaseOffset(short[].class);
      INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = unsafe.arrayBaseOffset(long[].class);
      FLOAT_ARRAY_OFFSET = unsafe.arrayBaseOffset(float[].class);
      DOUBLE_ARRAY_OFFSET = unsafe.arrayBaseOffset(double[].class);
    } else {
      BYTE_ARRAY_OFFSET = 0;
      SHORT_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      FLOAT_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }

  public static Unsafe getUnsafe() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    return unsafe;
  }
}
