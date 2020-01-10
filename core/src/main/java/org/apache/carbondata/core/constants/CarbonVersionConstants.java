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

package org.apache.carbondata.core.constants;

import java.io.InputStream;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

public final class CarbonVersionConstants {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonVersionConstants.class.getName());
  /**
   * the file name of CarbonData version info
   */
  private static final String CARBONDATA_VERSION_INFO_FILE =
      "carbondata-version-info.properties";
  /**
   * current CarbonData version
   */
  public static final String CARBONDATA_VERSION;
  /**
   * which branch current version build from
   */
  public static final String CARBONDATA_BRANCH;
  /**
   * the latest commit revision which current branch point to
   */
  public static final String CARBONDATA_REVISION;
  /**
   * the date of building current version
   */
  public static final String CARBONDATA_BUILD_DATE;

  static {
    // create input stream for CARBONDATA_VERSION_INFO_FILE
    InputStream resourceStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(CARBONDATA_VERSION_INFO_FILE);
    Properties props = new Properties();
    try {
      // read CARBONDATA_VERSION_INFO_FILE into props
      props.load(resourceStream);
    } catch (Exception e) {
      LOGGER.error("Error loading properties from " + CARBONDATA_VERSION_INFO_FILE, e);
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close();
        } catch (Exception e) {
          LOGGER.error("Error closing CarbonData build info resource stream", e);
        }
      }
    }
    // set the values
    CARBONDATA_VERSION = props.getProperty("version");
    CARBONDATA_BRANCH = props.getProperty("branch");
    CARBONDATA_REVISION = props.getProperty("revision");
    CARBONDATA_BUILD_DATE = props.getProperty("date");
  }
}
