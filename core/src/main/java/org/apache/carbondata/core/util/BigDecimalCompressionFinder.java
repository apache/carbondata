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

import org.apache.carbondata.core.util.ValueCompressionUtil.COMPRESSION_TYPE;
import org.apache.carbondata.core.util.ValueCompressionUtil.DataType;

public class BigDecimalCompressionFinder extends CompressionFinder {

  /**
   * non decimal part compression type
   */
  private COMPRESSION_TYPE leftCompType;

  /**
   * decimal part compression type
   */
  private COMPRESSION_TYPE rightCompType;

  /**
   * non decimal actual data type
   */
  private DataType leftActualDataType;

  /**
   * decimal actual data type
   */
  private DataType rightActualDataType;

  /**
   * non decimal converted data type
   */
  private DataType leftConvertedDataType;

  /**
   * decimal converted data type
   */
  private DataType rightConvertedDataType;

  public BigDecimalCompressionFinder(COMPRESSION_TYPE compType,
      DataType actualDataType, DataType convertedDataType, char measureStoreType) {
    super(compType, actualDataType, convertedDataType, measureStoreType);
  }

  public BigDecimalCompressionFinder(COMPRESSION_TYPE[] compType,
      DataType[] actualDataType, DataType[] convertedDataType, char measureStoreType) {
    super(null, null, null, measureStoreType);
    this.leftCompType = compType[0];
    this.rightCompType = compType[1];
    this.leftActualDataType = actualDataType[0];
    this.rightActualDataType = actualDataType[1];
    this.leftConvertedDataType = convertedDataType[0];
    this.rightConvertedDataType = convertedDataType[1];
  }

  public COMPRESSION_TYPE getLeftCompType() {
    return leftCompType;
  }

  public COMPRESSION_TYPE getRightCompType() {
    return rightCompType;
  }

  public DataType getLeftActualDataType() {
    return leftActualDataType;
  }

  public DataType getRightActualDataType() {
    return rightActualDataType;
  }

  public DataType getLeftConvertedDataType() { return leftConvertedDataType; }

  public DataType getRightConvertedDataType() {
    return rightConvertedDataType;
  }

}