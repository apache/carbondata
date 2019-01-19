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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

/**
 * It provides methods to convert object to Base64 string and vice versa.
 */
public class ObjectSerializationUtil {

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
        LOG.error(e);
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
      ois = new ObjectInputStream(gis);
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
        LOG.error(e);
      }
    }
  }

}
