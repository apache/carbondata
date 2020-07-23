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
import java.util.TimeZone;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.apache.log4j.Logger;

/**
 * The class provides the method to generate dictionary key and getting the actual value from
 * the dictionaryKey for direct dictionary column for TIMESTAMP type.
 */
public class DateDirectDictionaryGenerator extends AbstractDirectDictionaryGenerator {

  public static final int cutOffDate = Integer.MAX_VALUE >> 1;
  private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
  public static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;
  /**
   * min value supported for date type column
   */
  public static final long MIN_VALUE;
  /**
   * MAx value supported for date type column
   */
  public static final long MAX_VALUE;
  /**
   * Logger instance
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DateDirectDictionaryGenerator.class.getName());

  static {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TimeZone.getTimeZone("GMT"));
    long minValue = 0;
    long maxValue = 0;
    try {
      minValue = df.parse("0001-01-01").getTime();
      maxValue = df.parse("9999-12-31").getTime();
    } catch (ParseException e) {
      // the Exception will not occur as constant value is being parsed
    }
    MIN_VALUE = minValue;
    MAX_VALUE = maxValue;
  }

  public DateDirectDictionaryGenerator(String dateFormat) {
    super(dateFormat);
  }

  /**
   * The method take dictionary key as input and returns the
   *
   * @param key
   * @return member value/actual value Date
   */
  @Override
  public Object getValueFromSurrogate(int key) {
    if (key == CarbonCommonConstants.DIRECT_DICT_VALUE_NULL) {
      return null;
    }
    return key - cutOffDate;
  }

  @Override
  public int generateKey(long timeValue) {
    if (timeValue < MIN_VALUE || timeValue > MAX_VALUE) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Value for date type column is not in valid range. Value considered as null.");
      }
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    }
    return (int) Math.floor((double) timeValue / MILLIS_PER_DAY) + cutOffDate;
  }

  @Override
  public void initialize() {
    if (simpleDateFormatLocal.get() == null) {
      simpleDateFormatLocal.set(new SimpleDateFormat(dateFormat));
      simpleDateFormatLocal.get().setLenient(false);
      simpleDateFormatLocal.get().setTimeZone(TimeZone.getTimeZone("GMT"));
    }
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.INT;
  }
}