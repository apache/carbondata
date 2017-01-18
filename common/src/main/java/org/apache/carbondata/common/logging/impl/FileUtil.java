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

package org.apache.carbondata.common.logging.impl;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Provides file Utility
 */
public final class FileUtil {

  public static final String CARBON_PROPERTIES_FILE_PATH = "../../../conf/carbon.properties";
  private static final Logger LOG = Logger.getLogger(FileUtil.class.getName());
  private static Properties carbonProperties;

  private FileUtil() {

  }

  public static Properties getCarbonProperties() {
    if (null == carbonProperties) {
      loadProperties();
    }

    return carbonProperties;
  }

  /**
   * closes the stream
   *
   * @param stream stream to be closed.
   */
  public static void close(Closeable stream) {
    if (null != stream) {
      try {
        stream.close();
      } catch (IOException e) {
        LOG.error("Exception while closing the Log stream");
      }
    }
  }

  private static void loadProperties() {
    String property = System.getProperty("carbon.properties.filepath");
    if (null == property) {
      property = CARBON_PROPERTIES_FILE_PATH;
    }
    File file = new File(property);

    FileInputStream fis = null;
    try {
      if (file.exists()) {
        fis = new FileInputStream(file);

        carbonProperties = new Properties();
        carbonProperties.load(fis);
      }
    } catch (FileNotFoundException e) {
      LOG.error("Could not find carbon properties file in the path " + property);
    } catch (IOException e) {
      LOG.error("Error while reading carbon properties file in the path " + property);
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOG.error("Error while closing the file stream for carbon.properties");
        }
      }
    }
  }
}
