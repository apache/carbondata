package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Created by root1 on 22/11/16.
 */
public final class CarbonUnsafe {
  public static Unsafe unsafe;

  static {
    try {
      Field cause = Unsafe.class.getDeclaredField("theUnsafe");
      cause.setAccessible(true);
      unsafe = (Unsafe) cause.get((Object) null);
    } catch (Throwable var2) {
      unsafe = null;
    }
  }
}
