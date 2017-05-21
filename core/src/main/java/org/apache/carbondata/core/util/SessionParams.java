package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root1 on 19/5/17.
 */
public class SessionParams implements Serializable {

  protected transient CarbonProperties properties;

  private Map<String, String> sProps;

  public SessionParams() {
    sProps = new HashMap<>();
    properties = CarbonProperties.getInstance();
  }

  public SessionParams(SessionParams sessionParams) {
    this();
    sProps.putAll(sessionParams.sProps);
  }

  /**
   * This method will be used to get the properties value
   *
   * @param key
   * @return properties value
   */
  public String getProperty(String key) {
    String s = sProps.get(key);
    if (key == null) {
      s = properties.getProperty(key);
    }
    return s;
  }

  /**
   * This method will be used to get the properties value if property is not
   * present then it will return tghe default value
   *
   * @param key
   * @return properties value
   */
  public String getProperty(String key, String defaultValue) {
    String value = sProps.get(key);
    if (key == null) {
      value = properties.getProperty(key, defaultValue);
    }
    return value;
  }

  /**
   * This method will be used to add a new property
   *
   * @param key
   * @return properties value
   */
  public SessionParams addProperty(String key, String value) {
    sProps.put(key, value);
    return this;
  }

  public void setProperties(Map<String, String> newProperties) {
    sProps.putAll(newProperties);
  }

}
