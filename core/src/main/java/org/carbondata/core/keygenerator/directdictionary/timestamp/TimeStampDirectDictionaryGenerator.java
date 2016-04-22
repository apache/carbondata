/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.keygenerator.directdictionary.timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.util.CarbonEngineLogEvent;
import static org.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_DAY;
import static org.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_HOUR;
import static org.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_MIN;
import static org.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_SEC;

import org.apache.spark.sql.columnar.TIMESTAMP;


/**
 * The class provides the method to generate dictionary key and getting the actual value from
 * the dictionaryKey for direct dictionary column for TIMESTAMP type.
 */
public class TimeStampDirectDictionaryGenerator implements DirectDictionaryGenerator {

  /**
   * Logger instance
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TimeStampDirectDictionaryGenerator.class.getName());

  /**
   * The value of 1 unit of the SECOND, MINUTE, HOUR, or DAY in millis.
   */
  public static long granularityFactor;
  /**
   * The date timestamp to be considered as start date for calculating the timestamp
   * java counts the number of milliseconds from  start of "January 1, 1970", this property is
   * customized the start of position. for example "January 1, 2000"
   */
  public static long cutOffTimeStamp;

  /**
   * initialization block for granularityFactor and cutOffTimeStamp
   */
  static {
    String cutOffTimeStampString = CarbonProperties.getInstance()
        .getProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP);
    String timeGranularity = CarbonProperties.getInstance()
        .getProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY, TIME_GRAN_SEC);
    granularityFactor = 1000;
    switch (timeGranularity) {
      case TIME_GRAN_SEC:
        granularityFactor = TimeStampGranularityTypeValue.MILLIS_SECONDS.getValue();
        break;
      case TIME_GRAN_MIN:
        granularityFactor = TimeStampGranularityTypeValue.MILLIS_MINUTE.getValue();
        break;
      case TIME_GRAN_HOUR:
        granularityFactor = TimeStampGranularityTypeValue.MILLIS_HOUR.getValue();
        break;
      case TIME_GRAN_DAY:
        granularityFactor = TimeStampGranularityTypeValue.MILLIS_DAY.getValue();
        break;
      default:
        granularityFactor = 1000;
    }
    if (null == cutOffTimeStampString) {
      cutOffTimeStamp = -1;
    } else {
      try {
        SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        Date dateToStr = timeParser.parse(cutOffTimeStampString);
        cutOffTimeStamp = dateToStr.getTime();
      } catch (ParseException e) {
        LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
            "Cannot convert" + TIMESTAMP.toString() + " to Time/Long type value" + e.getMessage());
        cutOffTimeStamp = -1;
      }
    }
  }

  /**
   * The method take member String as input and converts
   * and returns the dictionary key
   *
   * @param memberStr date format string
   * @return dictionary value
   */
  @Override public int generateDirectSurrogateKey(String memberStr) {
    SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
    Date dateToStr = null;
    try {
      dateToStr = timeParser.parse(memberStr);
    } catch (ParseException e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
          "Cannot convert" + TIMESTAMP.toString() + " to Time/Long type value" + e.getMessage());
    }
    if (null == dateToStr) {
      return 1;
    } else {
      if (cutOffTimeStamp >= 0) {
        return (int) ((dateToStr.getTime() - cutOffTimeStamp) / granularityFactor);
      } else {
        return (int) (dateToStr.getTime() / granularityFactor);
      }
    }
  }

  /**
   * The method take dictionary key as input and returns the
   *
   * @param key
   * @return member value/actual value Date
   */
  @Override public Object getValueFromSurrogate(int key) {
    long timeStamp = 0;
    if (cutOffTimeStamp >= 0) {
      timeStamp = (key * granularityFactor + cutOffTimeStamp);
    } else {
      timeStamp = key * granularityFactor;
    }
    return timeStamp * 1000L;
  }
}
