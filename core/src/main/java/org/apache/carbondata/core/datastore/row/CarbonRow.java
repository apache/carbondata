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

package org.apache.carbondata.core.datastore.row;

import java.io.Serializable;
import java.util.Arrays;

/**
 * This row class is used to transfer the row data from one step to other step
 */
public class CarbonRow implements Serializable {

  private Object[] data;

  private Object[] rawData;

  private short rangeId;

  public CarbonRow(Object[] data) {
    this.data = data;
  }

  /**
   *
   * @param data contains column values for only schema columns
   * @param rawData contains complete row of the rawData
   */
  public CarbonRow(Object[] data, Object[] rawData) {
    this.data = data;
    this.rawData = rawData;
  }

  public Object[] getData() {
    return data;
  }

  public void setData(Object[] data) {
    this.data = data;
  }

  public String getString(int ordinal) {
    return (String) data[ordinal];
  }

  public Object getObject(int ordinal) {
    return data[ordinal];
  }

  public void update(Object value, int ordinal) {
    data[ordinal] = value;
  }

  @Override public String toString() {
    return Arrays.toString(data);
  }

  public Object[] getRawData() {
    return rawData;
  }

  public void setRawData(Object[] rawData) {
    this.rawData = rawData;
  }

  public short getRangeId() {
    return rangeId;
  }

  public void setRangeId(short rangeId) {
    this.rangeId = rangeId;
  }

  public void clearData() {
    this.data = null;
  }
}
