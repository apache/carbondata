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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.log4j.Logger;

/**
 * It provides methods to convert object to Base64 string and vice versa.
 */
public class ObjectSerializationUtil {

  private static final Set<String> SAFE_JAVA_CLASSES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "java.lang.Boolean", "java.lang.Byte", "java.lang.Character", "java.lang.Double",
          "java.lang.Enum", "java.lang.Float", "java.lang.Integer", "java.lang.Long",
          "java.lang.Number", "java.lang.Object", "java.lang.Short", "java.lang.String",
          "java.math.BigDecimal", "java.math.BigInteger", "java.sql.Date",
          "java.sql.Timestamp", "java.util.ArrayList", "java.util.Date", "java.util.HashMap",
          "java.util.HashSet", "java.util.LinkedHashMap", "java.util.LinkedHashSet",
          "java.util.LinkedList", "java.util.TreeMap", "java.util.TreeSet", "java.util.UUID")));

  private static final Logger LOG =
      LogServiceFactory.getLogService(ObjectSerializationUtil.class.getName());

  /**
   * Convert object to Base64 String
   *
   * @param obj Object to be serialized
   * @return serialized string
   * @throws IOException
   */
  public static String convertObjectToString(Object obj) throws IOException {
    ByteArrayOutputStream baos = null;
    GZIPOutputStream gos = null;
    ObjectOutputStream oos = null;

    try {
      baos = new ByteArrayOutputStream();
      gos = new GZIPOutputStream(baos);
      oos = new ObjectOutputStream(gos);
      oos.writeObject(obj);
    } finally {
      try {
        if (oos != null) {
          oos.close();
        }
        if (gos != null) {
          gos.close();
        }
        if (baos != null) {
          baos.close();
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return CarbonUtil.encodeToString(baos.toByteArray());
  }

  /**
   * Converts Base64 string to object.
   *
   * @param objectString serialized object in string format
   * @return Object after convert string to object
   * @throws IOException
   */
  public static Object convertStringToObject(String objectString) throws IOException {
    if (objectString == null) {
      return null;
    }

    byte[] bytes = CarbonUtil.decodeStringToBytes(objectString);

    ByteArrayInputStream bais = null;
    GZIPInputStream gis = null;
    ObjectInputStream ois = null;

    try {
      bais = new ByteArrayInputStream(bytes);
      gis = new GZIPInputStream(bais);
      ois = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), gis);
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read object", e);
    } finally {
      try {
        if (ois != null) {
          ois.close();
        }
        if (gis != null) {
          gis.close();
        }
        if (bais != null) {
          bais.close();
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Converts a Base64 string to an object while restricting every class in the serialized graph.
   * This method must be used when the serialized value came from an untrusted source such as RPC.
   *
   * @param objectString serialized object in string format
   * @param expectedClass required type of the root object
   * @param allowedClassPrefixes package prefixes allowed in the object graph
   * @return the deserialized object
   * @throws IOException if the stream contains a disallowed class or has an unexpected root type
   */
  public static <T> T convertStringToObject(String objectString, Class<T> expectedClass,
      String... allowedClassPrefixes) throws IOException {
    if (objectString == null) {
      return null;
    }

    byte[] bytes = CarbonUtil.decodeStringToBytes(objectString);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         GZIPInputStream gis = new GZIPInputStream(bais);
         ObjectInputStream ois = new FilteringObjectInputStream(
             Thread.currentThread().getContextClassLoader(), gis, allowedClassPrefixes)) {
      Object object = ois.readObject();
      if (!expectedClass.isInstance(object)) {
        throw new InvalidClassException("Unexpected serialized type",
            object == null ? "null" : object.getClass().getName());
      }
      return expectedClass.cast(object);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read object", e);
    }
  }

  private static final class FilteringObjectInputStream extends ClassLoaderObjectInputStream {

    private final String[] allowedClassPrefixes;

    private FilteringObjectInputStream(ClassLoader classLoader, GZIPInputStream inputStream,
        String[] allowedClassPrefixes) throws IOException {
      super(classLoader, inputStream);
      this.allowedClassPrefixes = allowedClassPrefixes.clone();
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass descriptor)
        throws IOException, ClassNotFoundException {
      String className = getComponentClassName(descriptor.getName());
      if (!isAllowed(className)) {
        throw new InvalidClassException("Deserialization of class is not allowed", className);
      }
      return super.resolveClass(descriptor);
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces) throws IOException {
      // Proxy handlers are a common deserialization gadget entry point and are not needed here.
      throw new InvalidClassException("Deserialization of proxy classes is not allowed",
          Proxy.class.getName());
    }

    private boolean isAllowed(String className) {
      if (isPrimitive(className) || SAFE_JAVA_CLASSES.contains(className)) {
        return true;
      }
      for (String prefix : allowedClassPrefixes) {
        if (className.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }

    private static String getComponentClassName(String className) {
      while (className.startsWith("[")) {
        className = className.substring(1);
      }
      if (className.startsWith("L") && className.endsWith(";")) {
        return className.substring(1, className.length() - 1);
      }
      return className;
    }

    private static boolean isPrimitive(String className) {
      return className.length() == 1 && "ZBCSIJFD".contains(className);
    }
  }

}
