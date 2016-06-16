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

package org.carbondata.core.cache.dictionary;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonProperties;

/**
 * class that implements methods specific for dictionary data look up
 */
public class ColumnDictionaryInfo extends AbstractColumnDictionaryInfo {

  /**
   * index after members are sorted
   */
  private AtomicReference<List<Integer>> sortOrderReference =
      new AtomicReference<List<Integer>>(new ArrayList<Integer>());

  /**
   * inverted index to retrieve the member
   */
  private AtomicReference<List<Integer>> sortReverseOrderReference =
      new AtomicReference<List<Integer>>(new ArrayList<Integer>());

  private DataType dataType;

  public ColumnDictionaryInfo(DataType dataType) {
    this.dataType = dataType;
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value as byte array
   * @return if found returns key else 0
   */
  @Override public int getSurrogateKey(byte[] value) {
    return getSurrogateKeyFromDictionaryValue(value);
  }

  /**
   * This method will find and return the sort index for a given dictionary id.
   * Applicable scenarios:
   * 1. Used in case of order by queries when data sorting is required
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return if found returns key else 0
   */
  @Override public int getSortedIndex(int surrogateKey) {
    if (surrogateKey > sortReverseOrderReference.get().size()
        || surrogateKey < MINIMUM_SURROGATE_KEY) {
      return -1;
    }
    // decrement surrogate key as surrogate key basically means the index in array list
    // because surrogate key starts from 1 and index of list from 0, so it needs to be
    // decremented by 1
    return sortReverseOrderReference.get().get(surrogateKey - 1);
  }

  /**
   * This method will find and return the dictionary value from sorted index.
   * Applicable scenarios:
   * 1. Query final result preparation in case of order by queries:
   * While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param sortedIndex sort index of dictionary value
   * @return value if found else null
   */
  @Override public String getDictionaryValueFromSortedIndex(int sortedIndex) {
    if (sortedIndex > sortReverseOrderReference.get().size()
        || sortedIndex < MINIMUM_SURROGATE_KEY) {
      return null;
    }
    // decrement surrogate key as surrogate key basically means the index in array list
    // because surrogate key starts from 1, sort index will start form 1 and index
    // of list from 0, so it needs to be decremented by 1
    int surrogateKey = sortOrderReference.get().get(sortedIndex - 1);
    return getDictionaryValueForKey(surrogateKey);
  }

  /**
   * This method will add a new dictionary chunk to existing list of dictionary chunks
   *
   * @param dictionaryChunk
   */
  @Override public void addDictionaryChunk(List<byte[]> dictionaryChunk) {
    dictionaryChunks.add(dictionaryChunk);
  }

  /**
   * This method will set the sort order index of a dictionary column.
   * Sort order index if the index of dictionary values after they are sorted.
   *
   * @param sortOrderIndex
   */
  @Override public void setSortOrderIndex(List<Integer> sortOrderIndex) {
    sortOrderReference.set(sortOrderIndex);
  }

  /**
   * This method will set the sort reverse index of a dictionary column.
   * Sort reverse index is the index of dictionary values before they are sorted.
   *
   * @param sortReverseOrderIndex
   */
  @Override public void setSortReverseOrderIndex(List<Integer> sortReverseOrderIndex) {
    sortReverseOrderReference.set(sortReverseOrderIndex);
  }

  /**
   * This method will apply binary search logic to find the surrogate key for the
   * given value
   *
   * @param key to be searched
   * @return
   */
  private int getSurrogateKeyFromDictionaryValue(byte[] key) {
    String filterKey = new String(key, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    int low = 0;
    List<Integer> sortedSurrogates = sortOrderReference.get();
    int high = sortedSurrogates.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int surrogateKey = sortedSurrogates.get(mid);
      byte[] dictionaryValue = getDictionaryBytesFromSurrogate(surrogateKey);
      int cmp = -1;
      if (this.getDataType() != DataType.STRING) {
        cmp = compareFilterKeyWithDictionaryKey(new String(dictionaryValue), filterKey,
            this.getDataType());

      } else {
        cmp = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dictionaryValue, key);
      }
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return surrogateKey; // key found
      }
    }
    return 0;
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
      String filterKey = new String(byteValueOfFilterMember);
      int high = sortedSurrogates.size() - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int surrogateKey = sortedSurrogates.get(mid);
        byte[] dictionaryValue = getDictionaryBytesFromSurrogate(surrogateKey);
        int cmp = -1;
        if (this.getDataType() != DataType.STRING) {
          cmp = compareFilterKeyWithDictionaryKey(new String(dictionaryValue), filterKey,
              this.getDataType());

        } else {
          cmp =
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(dictionaryValue, byteValueOfFilterMember);
        }
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {

          surrogates.add(surrogateKey);
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

  private int compareFilterKeyWithDictionaryKey(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
      switch (dataType) {
        case INT:
          return Integer.compare((Integer.parseInt(dictionaryVal)), (Integer.parseInt(memberVal)));
        case DOUBLE:
          return Double
              .compare((Double.parseDouble(dictionaryVal)), (Double.parseDouble(memberVal)));
        case LONG:
          return Long.compare((Long.parseLong(dictionaryVal)), (Long.parseLong(memberVal)));
        case BOOLEAN:
          return Boolean
              .compare((Boolean.parseBoolean(dictionaryVal)), (Boolean.parseBoolean(memberVal)));
        case TIMESTAMP:
          SimpleDateFormat parser = new SimpleDateFormat(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));
          Date dateToStr;
          Date dictionaryDate;
          dateToStr = parser.parse(memberVal);
          dictionaryDate = parser.parse(dictionaryVal);
          return dictionaryDate.compareTo(dateToStr);
        case DECIMAL:
          java.math.BigDecimal javaDecValForDictVal = new java.math.BigDecimal(dictionaryVal);
          java.math.BigDecimal javaDecValForMemberVal = new java.math.BigDecimal(memberVal);
          return javaDecValForDictVal.compareTo(javaDecValForMemberVal);
        default:
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
      return -1;
    }
  }

  /**
   * getDataType().
   *
   * @return
   */
  public DataType getDataType() {
    return dataType;
  }

}
