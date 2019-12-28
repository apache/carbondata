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

package org.apache.carbondata.core.cache.dictionary;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * class that implements methods specific for dictionary data look up
 */
public class ColumnDictionaryInfo extends AbstractColumnDictionaryInfo {

  /**
   * index after members are sorted
   */
  private AtomicReference<List<Integer>> sortOrderReference =
      new AtomicReference<List<Integer>>(new ArrayList<Integer>());

  private DataType dataType;

  public ColumnDictionaryInfo(DataType dataType) {
    this.dataType = dataType;
  }

  /**
   * This method will apply binary search logic to find the surrogate key for the
   * given value
   *
   * @param byteValuesOfFilterMembers to be searched
   * @param surrogates
   * @return
   */
  public void getIncrementalSurrogateKeyFromDictionary(List<byte[]> byteValuesOfFilterMembers,
      List<Integer> surrogates) {
    List<Integer> sortedSurrogates = sortOrderReference.get();
    int low = 0;
    for (byte[] byteValueOfFilterMember : byteValuesOfFilterMembers) {
      String filterKey = new String(byteValueOfFilterMember,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterKey)) {
        surrogates.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY);
        continue;
      }
      int high = sortedSurrogates.size() - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int surrogateKey = sortedSurrogates.get(mid);
        int cmp =
            compareFilterValue(surrogateKey, sortedSurrogates, byteValueOfFilterMember, filterKey);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          surrogates.add(surrogateKey);
          if (this.getDataType() == DataTypes.DOUBLE) {
            int tmp_mid = mid - 1;
            int tmp_low = low > 0 ? low + 1 : 0;
            while (tmp_mid >= tmp_low) {
              surrogateKey = sortedSurrogates.get(tmp_mid);
              cmp = compareFilterValue(surrogateKey, sortedSurrogates, byteValueOfFilterMember,
                  filterKey);
              if (cmp == 0) {
                surrogates.add(surrogateKey);
              }
              tmp_mid--;
            }
          }
          low = mid;
          break;
        }
      }
    }
    //Default value has to be added
    if (surrogates.isEmpty()) {
      surrogates.add(0);
    }
  }

  private int compareFilterValue(int surrogateKey, List<Integer> sortedSurrogates,
      byte[] byteValueOfFilterMember, String filterKey) {
    byte[] dictionaryValue = getDictionaryBytesFromSurrogate(surrogateKey);
    int cmp = -1;
    //fortify fix
    if (null == dictionaryValue) {
      cmp = -1;
    } else if (this.getDataType() != DataTypes.STRING) {
      cmp = compareFilterKeyWithDictionaryKey(
          new String(dictionaryValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)),
          filterKey, this.getDataType());

    } else {
      cmp = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dictionaryValue, byteValueOfFilterMember);
    }
    return cmp;
  }

  private int compareFilterKeyWithDictionaryKey(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
      if (dataType == DataTypes.SHORT) {
        return Short.compare((Short.parseShort(dictionaryVal)), (Short.parseShort(memberVal)));
      } else if (dataType == DataTypes.INT) {
        return Integer.compare((Integer.parseInt(dictionaryVal)), (Integer.parseInt(memberVal)));
      } else if (dataType == DataTypes.DOUBLE) {
        return DataTypeUtil.compareDoubleWithNan(
            (Double.parseDouble(dictionaryVal)), (Double.parseDouble(memberVal)));
      } else if (dataType == DataTypes.LONG) {
        return Long.compare((Long.parseLong(dictionaryVal)), (Long.parseLong(memberVal)));
      } else if (DataTypes.isDecimal(dataType)) {
        java.math.BigDecimal javaDecValForDictVal = new java.math.BigDecimal(dictionaryVal);
        java.math.BigDecimal javaDecValForMemberVal = new java.math.BigDecimal(memberVal);
        return javaDecValForDictVal.compareTo(javaDecValForMemberVal);
      } else {
        return -1;
      }
    } catch (Exception e) {
      //In all data types excluding String data type the null member will be the highest
      //while doing search in dictioary when the member comparison happens with filter member
      //which is also null member, since the parsing fails in other data type except string
      //explicit comparison is required, is both are null member then system has to return 0.
      if (memberVal.equals(dictionaryVal)) {
        return 0;
      }
      return 1;
    }
  }

  public DataType getDataType() {
    return dataType;
  }

}
