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
package org.carbondata.query.carbon.result.comparator;

import java.nio.charset.Charset;
import java.util.Comparator;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.util.DataTypeUtil;

/**
 * Variable length key comparator
 */
public class VariableLengthKeyResultComparator implements Comparator<ListBasedResultWrapper> {

  /**
   * sort order
   */
  private byte sortOrder;

  /**
   * no dictionary column index
   */
  private int noDictionaryColumnIndex;

  /**
   * data type of the column
   */
  private DataType dataType;

  public VariableLengthKeyResultComparator(byte sortOrder, int noDictionaryColumnIndex,
      DataType dataType) {
    this.sortOrder = sortOrder;
    this.noDictionaryColumnIndex = noDictionaryColumnIndex;
    this.dataType = dataType;
  }

  @Override public int compare(ListBasedResultWrapper listBasedResultWrapperFirst,
      ListBasedResultWrapper listBasedResultWrapperSecond) {
    // get the result
    byte[] noDictionaryKeysFirst =
        listBasedResultWrapperFirst.getKey().getNoDictionaryKeyByIndex(noDictionaryColumnIndex);
    // convert the result based on actual data type
    Object dataBasedOnDataTypeFirst =
        DataTypeUtil.getDataBasedOnDataType(new String(noDictionaryKeysFirst, Charset
            .forName(CarbonCommonConstants.DEFAULT_CHARSET)), dataType);
    byte[] noDictionaryKeysSecond =
        listBasedResultWrapperSecond.getKey().getNoDictionaryKeyByIndex(noDictionaryColumnIndex);
    Object dataBasedOnDataTypeSecond = DataTypeUtil.getDataBasedOnDataType(
        new String(noDictionaryKeysSecond, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)),
        dataType);
    int cmp = 0;
    // compare the result
    cmp = DataTypeUtil
        .compareBasedOnDatatYpe(dataBasedOnDataTypeFirst, dataBasedOnDataTypeSecond, dataType);
    if (sortOrder == 1) {
      cmp = cmp * -1;
    }
    return cmp;
  }

}
