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

package com.huawei.unibi.molap.olap;

public final class Util {
  public static final String NL = System.getProperty("line.separator");

  private Util() {

  }

  /**
   * Creates an internal error with a given message.
   */
  public static RuntimeException newInternal(String message) {
    return new RuntimeException(message);
  }

  /**
   * Creates an internal error with a given message and cause.
   */
  public static RuntimeException newInternal(Throwable e, String message) {
    return new RuntimeException(message, e);
  }

  /**
   * Checks that a precondition (declared using the javadoc <code>@pre</code>
   * tag) is satisfied.
   *
   * @param b The value of executing the condition
   */
  public static void assertPrecondition(boolean b) {
    assertTrue(b);
  }

  /**
   * Throws an internal error if condition is not true. It would be called
   * <code>assert</code>, but that is a keyword as of JDK 1.4.
   */
  public static void assertTrue(boolean b) {
    if (!b) {
      newInternal("assert failed");
    }
  }

  /**
   * Returns true if two objects are equal, or are both null.
   *
   * @param s First object
   * @param t Second object
   * @return Whether objects are equal or both null
   */
  public static boolean equals(Object s, Object t) {
    if (s == t) {
      return true;
    }
    if (s == null || t == null) {
      return false;
    }
    return s.equals(t);
  }

  /**
   * Returns true if two strings are equal, or are both null.
   */
  public static boolean equals(String s, String t) {
    return equals((Object) s, (Object) t);
  }

  public static void assertPrecondition(boolean b, String condition) {
    assertTrue(b, condition);
  }

  public static void assertTrue(boolean b, String message) {
    if (!b) {
      newInternal("assert failed: " + message);
    }
  }

  /**
   * Creates a non-internal error. Currently implemented in terms of
   * internal errors, but later we will create resourced messages.
   */
  public static RuntimeException newError(String message) {
    return newInternal(message);
  }

  /**
   * Creates a non-internal error. Currently implemented in terms of
   * internal errors, but later we will create resourced messages.
   */
  public static RuntimeException newError(Throwable e, String message) {
    return newInternal(e, message);
  }

  /**
   * Converts a string into a double-quoted string.
   */
  public static String quoteForMdx(String val) {
    StringBuilder buf = new StringBuilder(val.length() + 20);
    quoteForMdx(buf, val);
    return buf.toString();
  }

  /**
   * Appends a double-quoted string to a string builder.
   */
  public static StringBuilder quoteForMdx(StringBuilder buf, String val) {
    buf.append("\"");
    String s0 = replace(val, "\"", "\"\"");
    buf.append(s0);
    buf.append("\"");
    return buf;
  }

  /**
   * Returns a string with every occurrence of a seek string replaced with
   * another.
   */
  public static String replace(String s, String find, String replace) {
    // let's be optimistic
    int found = s.indexOf(find);
    if (found == -1) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length() + 20);
    int start = 0;
    char[] chars = s.toCharArray();
    final int step = find.length();
    if (step == 0) {
      // Special case where find is "".
      sb.append(s);
      replace(sb, 0, find, replace);
    } else {
      for (; ; ) {
        sb.append(chars, start, found - start);
        if (found == s.length()) {
          break;
        }
        sb.append(replace);
        start = found + step;
        found = s.indexOf(find, start);
        if (found == -1) {
          found = s.length();
        }
      }
    }
    return sb.toString();
  }

  /**
   * Replaces all occurrences of a string in a buffer with another.
   *
   * @param buf     String buffer to act on
   * @param start   Ordinal within <code>find</code> to start searching
   * @param find    String to find
   * @param replace String to replace it with
   * @return The string buffer
   */
  public static StringBuilder replace(StringBuilder buf, int start, String find, String replace) {
    // Search and replace from the end towards the start, to avoid O(n ^ 2)
    // copying if the string occurs very commonly.
    int findLength = find.length();
    if (findLength == 0) {
      // Special case where the seek string is empty.
      for (int j = buf.length(); j >= 0; --j) {
        buf.insert(j, replace);
      }
      return buf;
    }
    int k = buf.length();
    while (k > 0) {
      int i = buf.lastIndexOf(find, k);
      if (i < start) {
        break;
      }
      buf.replace(i, i + find.length(), replace);
      // Step back far enough to ensure that the beginning of the section
      // we just replaced does not cause a match.
      k = i - findLength;
    }
    return buf;
  }
}
