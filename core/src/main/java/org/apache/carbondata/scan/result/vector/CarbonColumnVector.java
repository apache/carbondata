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
package org.apache.carbondata.scan.result.vector;

import org.apache.spark.sql.types.Decimal;

public interface CarbonColumnVector {

  public void putShort(int rowId, short value);

  public void putInt(int rowId, int value);

  public void putLong(int rowId, long value);

  public void putDecimal(int rowId, Decimal value, int precision);

  public void putDouble(int rowId, double value);

  public void putBytes(int rowId, byte[] value);

  public void putBytes(int rowId, int offset, int length, byte[] value);

  public void putNull(int rowId);

  public boolean isNull(int rowId);

  public void putObject(int rowId, Object obj);

  public Object getData(int rowId);

  public void reset();

}
