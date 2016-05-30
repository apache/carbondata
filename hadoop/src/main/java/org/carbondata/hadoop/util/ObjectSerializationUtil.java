package org.carbondata.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * It provides methods to convert object to Base64 string and vice versa.
 */
public class ObjectSerializationUtil {

  /**
   * Convert object to Base64 String
   *
   * @param obj
   * @return
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
      oos.close();
      gos.close();
      baos.close();
    }

    return new String(Base64.encodeBase64(baos.toByteArray()), "UTF-8");
  }

  /**
   * Converts Base64 string to object.
   * @param objectString
   * @return
   * @throws IOException
   */
  public static Object convertStringToObject(String objectString) throws IOException {
    if (objectString == null) {
      return null;
    }

    byte[] bytes = Base64.decodeBase64(objectString.getBytes("UTF-8"));

    ByteArrayInputStream bais = null;
    GZIPInputStream gis = null;
    ObjectInputStream ois = null;

    try {
      bais = new ByteArrayInputStream(bytes);
      gis = new GZIPInputStream(bais);
      ois = new ObjectInputStream(gis);
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read object");
    } finally {
      ois.close();
      gis.close();
      bais.close();
    }
  }
}
