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

package org.apache.carbondata.processing.newflow.row;

import java.math.BigDecimal;

/**
 * This row class is used to transfer the row data from one step to other step
 */
public class CarbonRow {

  private Object[] data;

  public CarbonRow(Object[] data) {
    this.data = data;
  }

  public Object[] getData() {
    return data;
  }

  public int getInt(int ordinal) {
    return (int) data[ordinal];
  }

  public long getLong(int ordinal) {
    return (long) data[ordinal];
  }

  public float getFloat(int ordinal) {
    return (float) data[ordinal];
  }

  public double getDouble(int ordinal) {
    return (double) data[ordinal];
  }

  public BigDecimal getDecimal(int ordinal) {
    return (BigDecimal) data[ordinal];
  }

  public String getString(int ordinal) {
    return (String) data[ordinal];
  }

  public Object getObject(int ordinal) {
    return data[ordinal];
  }

  public byte[] getBinary(int ordinal) {
    return (byte[]) data[ordinal];
  }

  public void update(Object value, int ordinal) {
    data[ordinal] = value;
  }

}
