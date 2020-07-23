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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;

import org.apache.log4j.Logger;

public abstract class AbstractDirectDictionaryGenerator implements DirectDictionaryGenerator {

  protected ThreadLocal<SimpleDateFormat> simpleDateFormatLocal = new ThreadLocal<>();

  protected String dateFormat;

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(AbstractDirectDictionaryGenerator.class.getName());

  public AbstractDirectDictionaryGenerator(String dateFormat) {
    this.dateFormat = dateFormat;
    initialize();
  }

  @Override
  public void initialize() {
    if (simpleDateFormatLocal.get() == null) {
      simpleDateFormatLocal.set(new SimpleDateFormat(dateFormat));
      simpleDateFormatLocal.get().setLenient(false);
    }
  }

  /**
   * The method take member String as input and converts
   * and returns the dictionary key
   *
   * @param memberStr date format string
   * @return dictionary value
   */
  @Override
  public int generateDirectSurrogateKey(String memberStr) {
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
    Date dateToStr;
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
    //adding +2 to reserve the first cutOffDiff value for null or empty date
    if (null == dateToStr) {
      return CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
    } else {
      return generateKey(dateToStr.getTime());
    }
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
}
