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

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public class DataTypeConverterImpl implements DataTypeConverter, Serializable {

  private static final long serialVersionUID = -1718154403432354200L;

  public Object convertToDecimal(Object data) {
    java.math.BigDecimal javaDecVal = new java.math.BigDecimal(data.toString());
    return javaDecVal;
  }

  public Object convertFromByteToUTF8String(Object data) {
    return new String((byte[]) data, CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
  }

  public byte[] convertFromStringToByte(Object data) {
    return data.toString().getBytes(CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
  }

  public Object convertFromStringToUTF8String(Object data) {
    return data.toString();
  }
}
