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
import org.apache.carbondata.core.util.CarbonUtil;
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
   * @param newDictionaryChunk
   */
  @Override public void addDictionaryChunk(List<byte[]> newDictionaryChunk) {
    if (dictionaryChunks.size() > 0) {
      // Ensure that each time a new dictionary chunk is getting added to the
      // dictionary chunks list, equal distribution of dictionary values should
      // be there in the sublists of dictionary chunk list
      List<byte[]> lastDictionaryChunk = dictionaryChunks.get(dictionaryChunks.size() - 1);
      int dictionaryOneChunkSize = CarbonUtil.getDictionaryChunkSize();
      int differenceInLastDictionaryAndOneChunkSize =
          dictionaryOneChunkSize - lastDictionaryChunk.size();
      if (differenceInLastDictionaryAndOneChunkSize > 0) {
        // if difference is greater than new dictionary size then copy a part of list
        // else copy the complete new dictionary chunk list in the last dictionary chunk list
        if (differenceInLastDictionaryAndOneChunkSize >= newDictionaryChunk.size()) {
          lastDictionaryChunk.addAll(newDictionaryChunk);
        } else {
          List<byte[]> subListOfNewDictionaryChunk =
              newDictionaryChunk.subList(0, differenceInLastDictionaryAndOneChunkSize);
          lastDictionaryChunk.addAll(subListOfNewDictionaryChunk);
          List<byte[]> remainingNewDictionaryChunk = newDictionaryChunk
              .subList(differenceInLastDictionaryAndOneChunkSize, newDictionaryChunk.size());
          dictionaryChunks.add(remainingNewDictionaryChunk);
        }
      } else {
        dictionaryChunks.add(newDictionaryChunk);
      }
    } else {
      dictionaryChunks.add(newDictionaryChunk);
    }
  }

  /**
   * This method will return the size of of last dictionary chunk so that only that many
   * values are read from the dictionary reader
   *
   * @return size of last dictionary chunk
   */
  @Override public int getSizeOfLastDictionaryChunk() {
    int lastDictionaryChunkSize = 0;
    if (dictionaryChunks.size() > 0) {
      lastDictionaryChunkSize = dictionaryChunks.get(dictionaryChunks.size() - 1).size();
    }
    return lastDictionaryChunkSize;
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
      if (null == dictionaryValue) {
        return CarbonCommonConstants.INVALID_SURROGATE_KEY;
      }
      int cmp = -1;
      if (this.getDataType() != DataTypes.STRING) {
        cmp = compareFilterKeyWithDictionaryKey(
            new String(dictionaryValue, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)),
            filterKey, this.getDataType());

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
    return CarbonCommonConstants.INVALID_SURROGATE_KEY;
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
