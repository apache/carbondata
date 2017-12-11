package org.apache.carbondata.processing.loading;

import java.util.HashMap;
import java.util.Map;

// TODO: This is a hack for data loading, remove it
public class TempTablePath {
  private static final Map<String, String> tempMapping = new HashMap<>();

  public static void setTempTablePath(String key, String value) {
    tempMapping.put(key, value);
  }

  public static String getTempTablePath(String key) {
    return tempMapping.get(key);
  }
}
