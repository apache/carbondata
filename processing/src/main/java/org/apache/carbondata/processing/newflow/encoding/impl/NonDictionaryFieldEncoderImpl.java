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
package org.apache.carbondata.processing.newflow.encoding.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

public class NonDictionaryFieldEncoderImpl implements FieldEncoder<ByteBuffer> {

  private DataType dataType;

  private int index;

  public NonDictionaryFieldEncoderImpl(DataField dataField, int index) {
    this.dataType = dataField.getColumn().getDataType();
    this.index = index;
  }

  @Override public ByteBuffer encode(CarbonRow row) {
    String dimensionValue = row.getString(index);
    if (dataType != DataType.STRING) {
      if (null == DataTypeUtil.normalizeIntAndLongValues(dimensionValue, dataType)) {
        dimensionValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
      }
    }
    ByteBuffer buffer = ByteBuffer
        .wrap(dimensionValue.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
    buffer.rewind();
    return buffer;
  }
}
