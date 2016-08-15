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

package org.apache.carbondata.processing.store;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * This class contains attributes of file which are required to
 * construct file name like taskId, factTimeStamp
 */
public class CarbonDataFileAttributes {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataFileAttributes.class.getName());
  /**
   * task Id which is unique for each spark task
   */
  private int taskId;

  /**
   * load start time
   */
  private String factTimeStamp;

  /**
   * @param taskId
   * @param factTimeStamp
   */
  public CarbonDataFileAttributes(int taskId, String factTimeStamp) {
    this.taskId = taskId;
    this.factTimeStamp = factTimeStamp;
  }

  /**
   * @return
   */
  public int getTaskId() {
    return taskId;
  }

  /**
   * @return fact time stamp which is load start time
   */
  public String getFactTimeStamp() {
    return convertTimeStampToString(factTimeStamp);
  }

  /**
   * This method will convert a given timestamp to long value and then to string back
   *
   * @param factTimeStamp
   * @return
   */
  private String convertTimeStampToString(String factTimeStamp) {
    SimpleDateFormat parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    Date dateToStr = null;
    try {
      dateToStr = parser.parse(factTimeStamp);
      return Long.toString(dateToStr.getTime());
    } catch (ParseException e) {
      LOGGER.error("Cannot convert" + factTimeStamp
          + " to Time/Long type value" + e.getMessage());
      return null;
    }
  }
}
