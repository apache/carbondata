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

package org.apache.carbondata.core.util;

import java.io.Serializable;

/**
 * Represent min, max and alter sort column properties for each column in a block
 */
public class SegmentBlockMinMaxInfo implements Serializable {

  /**
   * true if column is a sort column
   */
  private boolean isSortColumn;

  /**
   * block level  min value for a column
   */
  private byte[] blockMinValue;

  /**
   * block level max value for a column
   */
  private byte[] blockMaxValue;

  /**
   * true if column measure if changed to dimension
   */
  private boolean isColumnDrift;

  public SegmentBlockMinMaxInfo(boolean isSortColumn, byte[] blockMinValue, byte[] blockMaxValue,
      boolean isColumnDrift) {
    this.isSortColumn = isSortColumn;
    this.blockMinValue = blockMinValue;
    this.blockMaxValue = blockMaxValue;
    this.isColumnDrift = isColumnDrift;
  }

  public boolean isSortColumn() {
    return isSortColumn;
  }

  public byte[] getBlockMinValue() {
    return blockMinValue;
  }

  public byte[] getBlockMaxValue() {
    return blockMaxValue;
  }

  public boolean isColumnDrift() {
    return isColumnDrift;
  }
}

