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

package org.apache.carbondata.processing.loading.converter.impl;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.impl.binary.BinaryDecoder;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

/**
 * Converter for binary
 */
public class BinaryFieldConverterImpl implements FieldConverter {

  private int index;
  private BinaryDecoder binaryDecoder;
  public BinaryFieldConverterImpl(int index, BinaryDecoder binaryDecoder) {
    this.index = index;
    this.binaryDecoder = binaryDecoder;
  }

  @Override
  public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    row.update(convert(row.getObject(index), logHolder), index);
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder)
      throws RuntimeException {
    if (value instanceof String) {
      return binaryDecoder.decode((String) value);
    } else if (value instanceof byte[]) {
      return value;
    } else {
      throw new CarbonDataLoadingException("Binary only support String and byte[] data type," +
          " binary decoder only support Base64, Hex or no decode for string");
    }
  }

  @Override
  public void clear() {
  }
}