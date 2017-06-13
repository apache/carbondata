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

import java.util.Arrays;

/**
 * This row class is used to transfer the row data from one step to other step
 */
public class CarbonRow {

  private Object[] data;

  public short bucketNumber;

  public CarbonRow(Object[] data) {
    this.data = data;
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

  public Object[] getObjectArray(int ordinal) {
    return (Object[]) data[ordinal];
  }

  public int[] getIntArray(int ordinal) {
    return (int[]) data[ordinal];
  }

  public void update(Object value, int ordinal) {
    data[ordinal] = value;
  }

  public CarbonRow getCopy() {
    Object[] copy = new Object[data.length];
    System.arraycopy(data, 0, copy, 0, copy.length);
    return new CarbonRow(copy);
  }

  @Override public String toString() {
    return Arrays.toString(data);
  }

}
