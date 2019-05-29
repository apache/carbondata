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

package org.apache.carbondata.processing.loading.converter.impl.binary;

import java.nio.charset.Charset;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import org.apache.commons.codec.binary.Base64;

public class Base64BinaryDecoder implements BinaryDecoder {
  @Override
  public byte[] decode(String input) {
    byte[] parsedValue = (String.valueOf(input))
        .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    if (Base64.isArrayByteBase64(parsedValue)) {
      parsedValue = Base64.decodeBase64(parsedValue);
    } else {
      throw new CarbonDataLoadingException("Binary decoder is " +
          CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_BASE64
          + ", but data is not base64");
    }
    return parsedValue;
  }
}
