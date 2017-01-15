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

package org.apache.carbondata.core.datastore.dataholder;

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.compression.ValueCompressionHolder;

// This class is used with Uncompressor to hold the decompressed column chunk in memory
public class CarbonReadDataHolder {

  private ValueCompressionHolder unCompressValue;

  public CarbonReadDataHolder(ValueCompressionHolder unCompressValue) {
    this.unCompressValue = unCompressValue;
  }

  public long getReadableLongValueByIndex(int index) {
    return this.unCompressValue.getLongValue(index);
  }

  public BigDecimal getReadableBigDecimalValueByIndex(int index) {
    return this.unCompressValue.getBigDecimalValue(index);
  }

  public double getReadableDoubleValueByIndex(int index) {
    return this.unCompressValue.getDoubleValue(index);
  }

  public void freeMemory() {
    unCompressValue.freeMemory();
  }
}
