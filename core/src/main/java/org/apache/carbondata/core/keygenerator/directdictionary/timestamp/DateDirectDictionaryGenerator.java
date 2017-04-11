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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * The class provides the method to generate dictionary key and getting the actual value from
 * the dictionaryKey for direct dictionary column for TIMESTAMP type.
 */
public class DateDirectDictionaryGenerator implements DirectDictionaryGenerator {

  private static final int cutOffDate = Integer.MAX_VALUE >> 1;
  private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
  private static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;

  private ThreadLocal<SimpleDateFormat> simpleDateFormatLocal = new ThreadLocal<>();

  //Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private ThreadLocal<TimeZone> threadLocalLocalTimeZone = new ThreadLocal() {
    @Override protected TimeZone initialValue() {
      return Calendar.getInstance().getTimeZone();
    }
  };
  private String dateFormat;

  /**
   * Logger instance
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DateDirectDictionaryGenerator.class.getName());

  public DateDirectDictionaryGenerator(String dateFormat) {
    this.dateFormat = dateFormat;
    initialize();
  }

  public DateDirectDictionaryGenerator() {
    this(CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));
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
      return 1;
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
        return 1;
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
      LOGGER.debug(
          "Cannot convert " + memberStr + " to Time/Long type value. Value considered as null." + e
              .getMessage());
      dateToStr = null;
    }
    //adding +2 to reserve the first cuttOffDiff value for null or empty date
    if (null == dateToStr) {
      return 1;
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
    if (key == 1) {
      return null;
    }
    return key - cutOffDate;
  }

  private int generateDirectSurrogateKeyForNonTimestampType(String memberStr) {
    long timeValue = -1;
    try {
      timeValue = Long.valueOf(memberStr) / 1000;
    } catch (NumberFormatException e) {
      LOGGER.debug("Cannot convert " + memberStr + " Long type value. Value considered as null." + e
          .getMessage());
    }
    if (timeValue == -1) {
      return 1;
    } else {
      return generateKey(timeValue);
    }
  }

  private int generateKey(long timeValue) {
    long milli = timeValue + threadLocalLocalTimeZone.get().getOffset(timeValue);
    return (int) Math.floor((double) milli / MILLIS_PER_DAY) + cutOffDate;
  }

  public void initialize() {
    if (simpleDateFormatLocal.get() == null) {
      simpleDateFormatLocal.set(new SimpleDateFormat(dateFormat));
      simpleDateFormatLocal.get().setLenient(false);
    }
  }

  @Override public DataType getReturnType() {
    return DataType.INT;
  }
}