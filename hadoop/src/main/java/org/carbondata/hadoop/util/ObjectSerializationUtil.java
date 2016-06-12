package org.carbondata.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * It provides methods to convert object to Base64 string and vice versa.
 */
public class ObjectSerializationUtil {

  private static final Log LOG = LogFactory.getLog(ObjectSerializationUtil.class);

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

    return new String(Base64.encodeBase64(baos.toByteArray()), "UTF-8");
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
