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

package org.apache.carbondata.horizon.rest.controller;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.store.api.conf.StoreConf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class Horizon {

  private static ConfigurableApplicationContext context;

  public static void main(String[] args) {
    String storeConfFile = System.getProperty(StoreConf.STORE_CONF_FILE);
    if (storeConfFile == null) {
      storeConfFile = getStoreConfFile();
    }
    start(storeConfFile);
  }

  static String getStoreConfFile() {
    try {
      return new File(".").getCanonicalPath() + "/store/conf/store.conf";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void start(String storeConfFile) {
    start(Horizon.class, storeConfFile);
  }

  static <T> void start(final Class<T> classTag, String storeConfFile) {
    System.setProperty("carbonstore.conf.file", storeConfFile);
    new Thread() {
      public void run() {
        context = SpringApplication.run(classTag);
      }
    }.start();
  }

  public static void stop() {
    SpringApplication.exit(context);
  }
}
