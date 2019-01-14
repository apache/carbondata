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
package org.apache.spark.sql;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

import org.apache.spark.sql.execution.vectorized.ColumnVector;

/**
 * This class uses the java reflection to create parquet dictionary class as CDH distribution uses
 * twitter parquet instead of apache parquet.
 */
@InterfaceAudience.Internal
public class CarbonDictionaryReflectionUtil {

  private static final boolean isApacheParquet;

  static {
    boolean isApache = true;
    try {
      createClass("org.apache.parquet.column.Encoding");
    } catch (Exception e) {
      isApache = false;
    }
    isApacheParquet = isApache;
  }

  public static Object generateDictionary(CarbonDictionary dictionary) {
    Class binary = createClass(getQualifiedName("parquet.io.api.Binary"));
    Object binaries = Array.newInstance(binary, dictionary.getDictionarySize());
    try {
      for (int i = 0; i < dictionary.getDictionarySize(); i++) {
        Object binaryValue = invokeStaticMethod(binary, "fromReusedByteArray",
            new Object[] { dictionary.getDictionaryValue(i) }, new Class[] { byte[].class });
        Array.set(binaries, i, binaryValue);
      }
      ;
      Class bytesInputClass = createClass(getQualifiedName("parquet.bytes.BytesInput"));
      Object bytesInput = invokeStaticMethod(bytesInputClass, "from", new Object[] { new byte[0] },
          new Class[] { byte[].class });

      Class dictPageClass = createClass(getQualifiedName("parquet.column.page.DictionaryPage"));
      Class encodingClass = createClass(getQualifiedName("parquet.column.Encoding"));
      Object plainEncoding = invokeStaticMethod(encodingClass, "valueOf", new Object[] { "PLAIN" },
          new Class[] { String.class });

      Object dictPageObj =
          dictPageClass.getDeclaredConstructor(bytesInputClass, int.class, encodingClass)
              .newInstance(bytesInput, 0, plainEncoding);
      Class plainDict = createClass(getQualifiedName(
          "parquet.column.values.dictionary.PlainValuesDictionary$PlainBinaryDictionary"));
      Object plainDictionary =
          plainDict.getDeclaredConstructor(dictPageClass).newInstance(dictPageObj);
      Field field = plainDict.getDeclaredField("binaryDictionaryContent");
      field.setAccessible(true);
      field.set(plainDictionary, binaries);
      return plainDictionary;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Object invokeStaticMethod(Class className, String methodName, Object[] values,
      Class[] classes) throws Exception {
    Method method = className.getMethod(methodName, classes);
    return method.invoke(null, values);
  }

  private static Class createClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getQualifiedName(String className) {
    if (isApacheParquet) {
      return "org.apache." + className;
    } else {
      return className;
    }
  }

  public static void setDictionary(ColumnVector vector, Object dictionary) {
    try {
      Class<?> aClass = vector.getClass();
      while (!aClass.getSimpleName().equals("ColumnVector")) {
        aClass = aClass.getSuperclass();
      }
      Field field = aClass.getDeclaredField("dictionary");
      field.setAccessible(true);
      field.set(vector, dictionary);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
