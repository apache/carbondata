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
package org.carbondata.integration.spark.load;

import java.nio.charset.Charset;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.ByteUtil;

/**
 * Dictionary sort model class holds the member byte value and corresponding key value.
 */
public class CarbonDictionarySortModel implements Comparable<CarbonDictionarySortModel> {

  /**
   * Charset const
   */
  public static final Charset CHARSET_CONST =
      Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET);
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
      case DECIMAL:
        Double d1 = null;
        Double d2 = null;
        try {
          d1 = new Double(memberValue);
        } catch (NumberFormatException e) {
          return -1;
        }
        try {
          d2 = new Double(o.memberValue);
        } catch (NumberFormatException e) {
          return 1;
        }
        return d1.compareTo(d2);
      case STRING:
      default:
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(this.memberValue.getBytes(CHARSET_CONST),
            o.memberValue.getBytes(CHARSET_CONST));
    }
  }

  /**
   * @see Object#hashCode()
   */
  @Override public int hashCode() {
    int result = 1;
    result = result + ((memberValue == null) ? 0 : memberValue.hashCode());
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
      } else if (!ByteUtil.UnsafeComparer.INSTANCE.equals(this.memberValue.getBytes(CHARSET_CONST),
          other.memberValue.getBytes(CHARSET_CONST))) {
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
