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
package org.apache.carbondata.core.writer.sortindex;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryChunksWrapper;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;

import org.apache.commons.lang.ArrayUtils;

/**
 * The class prepares the column sort info ie sortIndex
 * and inverted sort index info
 */
public class CarbonDictionarySortInfoPreparator {

  /**
   * The method returns the column Sort Info
   *
   * @param newDistinctValues new distinct value to be added
   * @param dictionary        old distinct values
   * @param dataType          DataType of columns
   * @return CarbonDictionarySortInfo returns the column Sort Info
   */
  public CarbonDictionarySortInfo getDictionarySortInfo(List<String> newDistinctValues,
      Dictionary dictionary, DataType dataType) {
    CarbonDictionarySortModel[] dictionarySortModels =
        prepareDictionarySortModels(newDistinctValues, dictionary, dataType);
    return createColumnSortInfo(dictionarySortModels);
  }

  /**
   * The method prepares the sort_index and sort_index_inverted data
   *
   * @param dictionarySortModels
   */
  private CarbonDictionarySortInfo createColumnSortInfo(
      CarbonDictionarySortModel[] dictionarySortModels) {

    //Sort index after members are sorted
    int[] sortIndex;
    //inverted sort index to get the member
    int[] sortIndexInverted;

    Arrays.sort(dictionarySortModels);
    sortIndex = new int[dictionarySortModels.length];
    sortIndexInverted = new int[dictionarySortModels.length];

    for (int i = 0; i < dictionarySortModels.length; i++) {
      CarbonDictionarySortModel dictionarySortModel = dictionarySortModels[i];
      sortIndex[i] = dictionarySortModel.getKey();
      // the array index starts from 0 therefore -1 is done to avoid wastage
      // of 0th index in array and surrogate key starts from 1 there 1 is added to i
      // which is a counter starting from 0
      sortIndexInverted[dictionarySortModel.getKey() - 1] = i + 1;
    }
    List<Integer> sortIndexList = convertToList(sortIndex);
    List<Integer> sortIndexInvertedList = convertToList(sortIndexInverted);
    return new CarbonDictionarySortInfo(sortIndexList, sortIndexInvertedList);
  }

  /**
   * The method converts the int[] to List<Integer>
   *
   * @param data
   * @return
   */
  private List<Integer> convertToList(int[] data) {
    Integer[] wrapperType = ArrayUtils.toObject(data);
    return Arrays.asList(wrapperType);
  }

  /**
   * The method returns the array of CarbonDictionarySortModel
   *
   * @param distinctValues new distinct values
   * @param dictionary The wrapper wraps the list<list<bye[]>> and provide the
   *                   iterator to retrieve the chunks members.
   * @param dataType   DataType of columns
   * @return CarbonDictionarySortModel[] CarbonDictionarySortModel[] the model
   * CarbonDictionarySortModel contains the  member's surrogate and
   * its byte value
   */
  private CarbonDictionarySortModel[] prepareDictionarySortModels(List<String> distinctValues,
      Dictionary dictionary, DataType dataType) {
    CarbonDictionarySortModel[] dictionarySortModels = null;
    //The wrapper wraps the list<list<bye[]>> and provide the iterator to
    // retrieve the chunks members.
    int surrogate = 1;
    if (null != dictionary) {
      DictionaryChunksWrapper dictionaryChunksWrapper = dictionary.getDictionaryChunks();
      dictionarySortModels =
          new CarbonDictionarySortModel[dictionaryChunksWrapper.getSize() + distinctValues.size()];
      while (dictionaryChunksWrapper.hasNext()) {
        dictionarySortModels[surrogate - 1] =
            createDictionarySortModel(surrogate, dataType, dictionaryChunksWrapper.next());
        surrogate++;
      }
    } else {
      dictionarySortModels = new CarbonDictionarySortModel[distinctValues.size()];
    }
    // for new distinct values
    Iterator<String> distinctValue = distinctValues.iterator();
    while (distinctValue.hasNext()) {
      dictionarySortModels[surrogate - 1] = createDictionarySortModel(surrogate, dataType,
          distinctValue.next().getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      surrogate++;
    }
    return dictionarySortModels;
  }

  /**
   *
   * @param surrogate
   * @param dataType
   * @param value member value
   * @return CarbonDictionarySortModel
   */
  private CarbonDictionarySortModel createDictionarySortModel(int surrogate, DataType dataType,
      byte[] value) {
    String memberValue = new String(value, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    return new CarbonDictionarySortModel(surrogate, dataType, memberValue);
  }
}

