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

package org.apache.carbondata.store.api.conf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.store.util.StoreUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StoreConf implements Serializable, Writable {

  public static final String SELECT_PROJECTION = "carbon.select.projection";
  public static final String SELECT_FILTER = "carbon.select.filter";
  public static final String SELECT_LIMIT = "carbon.select.limit";

  public static final String SELECT_ID = "carbon.select.id";

  public static final String WORKER_HOST = "carbon.worker.host";
  public static final String WORKER_PORT = "carbon.worker.port";
  public static final String WORKER_CORE_NUM = "carbon.worker.core.num";
  public static final String MASTER_HOST = "carbon.master.host";
  public static final String MASTER_PORT = "carbon.master.port";

  public static final String STORE_TEMP_LOCATION = "carbon.store.temp.location";
  public static final String STORE_LOCATION = "carbon.store.location";
  public static final String STORE_NAME = "carbon.store.name";

  public static final String STORE_CONF_FILE = "carbon.store.confFile";

  private Map<String, String> conf = new HashMap<>();

  public StoreConf() {
    String storeConfFile = System.getProperty(STORE_CONF_FILE);
    if (storeConfFile != null) {
      load(storeConfFile);
    }
  }

  public StoreConf(String storeName, String storeLocation) {
    conf.put(STORE_NAME, storeName);
    conf.put(STORE_LOCATION, storeLocation);
  }

  public StoreConf(String confFilePath) {
    load(confFilePath);
  }

  public StoreConf conf(String key, String value) {
    conf.put(key, value);
    return this;
  }

  public StoreConf conf(String key, int value) {
    conf.put(key, "" + value);
    return this;
  }

  public void load(String filePath) {
    StoreUtil.loadProperties(filePath, this);
  }

  public void conf(StoreConf conf) {
    this.conf.putAll(conf.conf);
  }

  public Object conf(String key) {
    return conf.get(key);
  }

  public String[] projection() {
    return stringArrayValue(SELECT_PROJECTION);
  }

  public String filter() {
    return stringValue(SELECT_FILTER);
  }

  public int limit() {
    return intValue(SELECT_LIMIT);
  }

  public String masterHost() {
    return stringValue(MASTER_HOST);
  }

  public int masterPort() {
    return intValue(MASTER_PORT);
  }

  public String workerHost() {
    return stringValue(WORKER_HOST);
  }

  public int workerPort() {
    return intValue(WORKER_PORT);
  }

  public int workerCoreNum() {
    return intValue(WORKER_CORE_NUM);
  }

  public String storeLocation() {
    return stringValue(STORE_LOCATION);
  }

  public String[] storeTempLocation() {
    return stringArrayValue(STORE_TEMP_LOCATION);
  }

  public String selectId() {
    return stringValue(SELECT_ID);
  }

  public Configuration newHadoopConf() {
    Configuration hadoopConf = FileFactory.getConfiguration();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key != null && value != null && key.startsWith("carbon.hadoop.")) {
        hadoopConf.set(key.substring("carbon.hadoop.".length()), value);
      }
    }
    return hadoopConf;
  }

  private String stringValue(String key) {
    Object obj = conf.get(key);
    if (obj == null) {
      return null;
    }
    return obj.toString();
  }

  private int intValue(String key) {
    String value = conf.get(key);
    if (value == null) {
      return -1;
    }
    return Integer.parseInt(value);
  }

  private String[] stringArrayValue(String key) {
    String value = conf.get(key);
    if (value == null) {
      return null;
    }
    return value.split(",", -1);
  }

  @Override public void write(DataOutput out) throws IOException {
    Set<Map.Entry<String, String>> entries = conf.entrySet();
    WritableUtils.writeVInt(out, conf.size());
    for (Map.Entry<String, String> entry : entries) {
      WritableUtils.writeString(out, entry.getKey());
      WritableUtils.writeString(out, entry.getValue());
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    if (conf == null) {
      conf = new HashMap<>();
    }

    int size = WritableUtils.readVInt(in);
    String key, value;
    for (int i = 0; i < size; i++) {
      key = WritableUtils.readString(in);
      value = WritableUtils.readString(in);
      conf.put(key, value);
    }
  }
}
