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

package org.apache.carbondata.core.metadata.schema;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * column ranges specified by sort column bounds
 */
@InterfaceAudience.Internal
public class SortColumnRangeInfo implements ColumnRangeInfo, Serializable {
  private static final long serialVersionUID = 1L;
  // indices for the sort columns in the raw row
  private int[] sortColumnIndex;
  // is the sort column no dictionary encoded
  private boolean[] isSortColumnNoDict;
  // each literal sort column bounds specified by user
  private String[] userSpecifiedRanges;
  // separator for the field values in each bound
  private String separator;
  // number of value ranges for the columns
  private int numOfRanges;

  public SortColumnRangeInfo(int[] sortColumnIndex, boolean[] isSortColumnNoDict,
      String[] userSpecifiedRanges, String separator) {
    this.sortColumnIndex = sortColumnIndex;
    this.isSortColumnNoDict = isSortColumnNoDict;
    this.userSpecifiedRanges = userSpecifiedRanges;
    this.separator = separator;
    this.numOfRanges = userSpecifiedRanges.length + 1;
  }

  public int[] getSortColumnIndex() {
    return sortColumnIndex;
  }

  public boolean[] getIsSortColumnNoDict() {
    return isSortColumnNoDict;
  }

  public String[] getUserSpecifiedRanges() {
    return userSpecifiedRanges;
  }

  public String getSeparator() {
    return separator;
  }

  @Override
  public int getNumOfRanges() {
    return numOfRanges;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SortColumnRangeInfo{");
    sb.append("sortColumnIndex=").append(Arrays.toString(sortColumnIndex));
    sb.append(", isSortColumnNoDict=").append(Arrays.toString(isSortColumnNoDict));
    sb.append(", userSpecifiedRanges=").append(Arrays.toString(userSpecifiedRanges));
    sb.append(", separator='").append(separator).append('\'');
    sb.append(", numOfRanges=").append(numOfRanges);
    sb.append('}');
    return sb.toString();
  }
}
