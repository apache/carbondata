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
package org.apache.carbondata.core.keygenerator.directdictionary.timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_DAY;
import static org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_HOUR;
import static org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_MIN;
import static org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants.TIME_GRAN_SEC;

/**
 * The class provides the method to generate dictionary key and getting the actual value from
 * the dictionaryKey for direct dictionary column for TIMESTAMP type.
 */
public class TimeStampDirectDictionaryGenerator implements DirectDictionaryGenerator {

  private ThreadLocal<SimpleDateFormat> simpleDateFormatLocal = new ThreadLocal<>();

  private String dateFormat;

  /**
   * The value of 1 unit of the SECOND, MINUTE, HOUR, or DAY in millis.
   */
  public static final long granularityFactor;
  /**
   * The date timestamp to be considered as start date for calculating the timestamp
   * java counts the number of milliseconds from  start of "January 1, 1970", this property is
   * customized the start of position. for example "January 1, 2000"
   */
  public static final long cutOffTimeStamp;
  /**
   * Logger instance
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TimeStampDirectDictionaryGenerator.class.getName());

  /*
   * initialization block for granularityFactor and cutOffTimeStamp
   */
  static {
    String cutOffTimeStampString = CarbonProperties.getInstance()
        .getProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP);
    String timeGranularity = CarbonProperties.getInstance()
        .getProperty(TimeStampGranularityConstants.CARBON_TIME_GRANULARITY, TIME_GRAN_SEC);
    long granularityFactorLocal = 1000;
    switch (timeGranularity) {
      case TIME_GRAN_SEC:
        granularityFactorLocal = TimeStampGranularityTypeValue.MILLIS_SECONDS.getValue();
        break;
      case TIME_GRAN_MIN:
        granularityFactorLocal = TimeStampGranularityTypeValue.MILLIS_MINUTE.getValue();
        break;
      case TIME_GRAN_HOUR:
        granularityFactorLocal = TimeStampGranularityTypeValue.MILLIS_HOUR.getValue();
        break;
      case TIME_GRAN_DAY:
        granularityFactorLocal = TimeStampGranularityTypeValue.MILLIS_DAY.getValue();
        break;
      default:
        granularityFactorLocal = 1000;
    }
    long cutOffTimeStampLocal;
    if (null == cutOffTimeStampString) {
      cutOffTimeStampLocal = 0;
    } else {
      try {
        SimpleDateFormat timeParser = new SimpleDateFormat(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        timeParser.setLenient(false);
        Date dateToStr = timeParser.parse(cutOffTimeStampString);
        cutOffTimeStampLocal = dateToStr.getTime();
      } catch (ParseException e) {
        LOGGER.warn("Cannot convert" + cutOffTimeStampString
            + " to Time/Long type value. Value considered for cutOffTimeStamp is -1." + e
            .getMessage());
        cutOffTimeStampLocal = 0;
      }
    }
    granularityFactor = granularityFactorLocal;
    cutOffTimeStamp = cutOffTimeStampLocal;
  }

  public TimeStampDirectDictionaryGenerator(String dateFormat) {
    this.dateFormat = dateFormat;
    initialize();
  }

  public TimeStampDirectDictionaryGenerator() {
    this(CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
  }

  /**
   * The method take member String as input and converts
   * and returns the dictionary key
   *
   * @param memberStr date format string
   * @return dictionary value
   */
  @Override public int generateDirectSurrogateKey(String memberStr) {
    if (null == memberStr || memberStr.trim().isEmpty() || memberStr
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    }
    return getDirectSurrogateForMember(memberStr);
  }

  /**
   * The method take member String as input and converts
   * and returns the dictionary key
   *
   * @param memberStr date format string
   * @return dictionary value
   */
  public int generateDirectSurrogateKey(String memberStr, String format) {
    if (null == format) {
      return generateDirectSurrogateKeyForNonTimestampType(memberStr);
    } else {
      if (null == memberStr || memberStr.trim().isEmpty() || memberStr
          .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
      }
      return getDirectSurrogateForMember(memberStr);
    }
  }

  private int getDirectSurrogateForMember(String memberStr) {
    Date dateToStr = null;
    try {
      SimpleDateFormat simpleDateFormat = simpleDateFormatLocal.get();
      if (null == simpleDateFormat) {
        initialize();
        simpleDateFormat = simpleDateFormatLocal.get();
      }
      dateToStr = simpleDateFormat.parse(memberStr);
    } catch (ParseException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Cannot convert value to Time/Long type value. Value considered as null." + e
            .getMessage());
      }
      dateToStr = null;
    }
    //adding +2 to reserve the first cuttOffDiff value for null or empty date
    if (null == dateToStr) {
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    } else {
      return generateKey(dateToStr.getTime());
    }
  }

  /**
   * The method take dictionary key as input and returns the
   *
   * @param key
   * @return member value/actual value Date
   */
  @Override public Object getValueFromSurrogate(int key) {
    if (key == CarbonCommonConstants.DIRECT_DICT_VALUE_NULL) {
      return null;
    }
    long timeStamp = ((key - 2) * granularityFactor + cutOffTimeStamp);
    return timeStamp * 1000L;
  }

  private int generateDirectSurrogateKeyForNonTimestampType(String memberStr) {
    long timeValue = -1;
    try {
      timeValue = Long.parseLong(memberStr) / 1000;
    } catch (NumberFormatException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Cannot convert " + memberStr + " Long type value. Value considered as null." + e
                .getMessage());
      }
    }
    if (timeValue == -1) {
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    } else {
      return generateKey(timeValue);
    }
  }

  public int generateKey(long timeValue) {
    long time = (timeValue - cutOffTimeStamp) / granularityFactor;
    int keyValue = -1;
    if (time >= (long) Integer.MIN_VALUE && time <= (long) Integer.MAX_VALUE) {
      keyValue = (int) time;
    }
    return keyValue < 0 ? CarbonCommonConstants.DIRECT_DICT_VALUE_NULL : keyValue + 2;
  }

  public void initialize() {
    if (simpleDateFormatLocal.get() == null) {
      simpleDateFormatLocal.set(new SimpleDateFormat(dateFormat));
      simpleDateFormatLocal.get().setLenient(false);
    }
  }

  @Override public DataType getReturnType() {
    return DataTypes.LONG;
  }

}