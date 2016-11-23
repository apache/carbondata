package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Created by root1 on 22/11/16.
 */
public final class CarbonUnsafe {

  public static final int BYTE_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static Unsafe unsafe;

  static {
    try {
      Field cause = Unsafe.class.getDeclaredField("theUnsafe");
      cause.setAccessible(true);
      unsafe = (Unsafe) cause.get((Object) null);
    } catch (Throwable var2) {
      unsafe = null;
    }
    if (unsafe != null) {
      BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
      LONG_ARRAY_OFFSET = unsafe.arrayBaseOffset(long[].class);
    } else {
      BYTE_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
    }
  }
}
