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
package org.carbondata.core.writer.sortindex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;

/**
 * Dictionary sort model class holds the member byte value and corresponding key value.
 */
public class CarbonDictionarySortModel implements Comparable<CarbonDictionarySortModel> {

  /**
   * Surrogate key
   */
  private int key;

  /**
   * member value in bytes
   */
  private String memberValue;

  /**
   * member dataType
   */
  private DataType dataType;

  /**
   * Constructor to init the dictionary sort model
   *
   * @param key
   * @param dataType
   * @param memberValue
   */
  public CarbonDictionarySortModel(int key, DataType dataType, String memberValue) {
    this.key = key;
    this.dataType = dataType;
    this.memberValue = memberValue;
  }

  /**
   * Compare
   */
  @Override public int compareTo(CarbonDictionarySortModel o) {
    switch (dataType) {
      case INT:
      case LONG:
      case DOUBLE:

        Double d1 = null;
        Double d2 = null;
        try {
          d1 = new Double(memberValue);
        } catch (NumberFormatException e) {
          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(o.memberValue)) {
            return -1;
          }
          return 1;
        }
        try {
          d2 = new Double(o.memberValue);
        } catch (NumberFormatException e) {
          return -1;
        }
        return d1.compareTo(d2);
      case DECIMAL:
        java.math.BigDecimal val1 = null;
        java.math.BigDecimal val2 = null;
        try {
          val1 = new java.math.BigDecimal(memberValue);
        } catch (NumberFormatException e) {
          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(o.memberValue)) {
            return -1;
          }
          return 1;
        }
        try {
          val2 = new java.math.BigDecimal(o.memberValue);
        } catch (NumberFormatException e) {
          return -1;
        }
        return val1.compareTo(val2);
      case TIMESTAMP:
        SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
        Date date1 = null;
        Date date2 = null;
        try {
          date1 = parser.parse(memberValue);
        } catch (ParseException e) {
          if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(o.memberValue)) {
            return -1;
          }
          return 1;
        }
        try {
          date2 = parser.parse(o.memberValue);
        } catch (ParseException e) {
          return -1;
        }
        return date1.compareTo(date2);
      case STRING:
      default:
        return this.memberValue.compareTo(o.memberValue);
    }
  }

  /**
   * @see Object#hashCode()
   */
  @Override public int hashCode() {
    int result = ((memberValue == null) ? 0 : memberValue.hashCode());
    return result;
  }

  /**
   * @see Object#equals(Object)
   */
  @Override public boolean equals(Object obj) {
    if (obj instanceof CarbonDictionarySortModel) {
      if (this == obj) {
        return true;
      }
      CarbonDictionarySortModel other = (CarbonDictionarySortModel) obj;
      if (memberValue == null) {
        if (other.memberValue != null) {
          return false;
        }
      } else if (!this.memberValue.equals(other.memberValue)) {
        return false;
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * return the surrogate of the member
   *
   * @return
   */
  public int getKey() {
    return key;
  }

  /**
   * Returns member buye
   *
   * @return
   */
  public String getMemberValue() {
    return memberValue;
  }

}
