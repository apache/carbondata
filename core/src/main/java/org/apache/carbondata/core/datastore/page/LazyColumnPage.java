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

package org.apache.carbondata.core.datastore.page;

/**
 * This is a decorator of column page, it performs transformation lazily (when caller calls getXXX
 * method to get the value from the page)
 */
public class LazyColumnPage extends ColumnPage {

  // decorated column page
  private ColumnPage columnPage;

  // encode that will apply to page data in getXXX
  private PrimitiveCodec codec;

  private LazyColumnPage(ColumnPage columnPage, PrimitiveCodec codec) {
    super(columnPage.getDataType(), columnPage.getPageSize());
    this.columnPage = columnPage;
    this.codec = codec;
  }

  public static ColumnPage newPage(ColumnPage columnPage, PrimitiveCodec transform) {
    return new LazyColumnPage(columnPage, transform);
  }

  @Override
  public String toString() {
    return String.format("[encode: %s, data type: %s", codec, columnPage.getDataType());
  }

  @Override
  public long getLong(int rowId) {
    switch (columnPage.getDataType()) {
      case BYTE:
        return codec.decodeLong(columnPage.getByte(rowId));
      case SHORT:
        return codec.decodeLong(columnPage.getShort(rowId));
      case INT:
        return codec.decodeLong(columnPage.getInt(rowId));
      case LONG:
        return columnPage.getLong(rowId);
      default:
        throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public double getDouble(int rowId) {
    switch (columnPage.getDataType()) {
      case BYTE:
        return codec.decodeDouble(columnPage.getByte(rowId));
      case SHORT:
        return codec.decodeDouble(columnPage.getShort(rowId));
      case INT:
        return codec.decodeDouble(columnPage.getInt(rowId));
      case LONG:
        return codec.decodeDouble(columnPage.getLong(rowId));
      case FLOAT:
        return codec.decodeDouble(columnPage.getFloat(rowId));
      case DOUBLE:
        return columnPage.getDouble(rowId);
      default:
        throw new RuntimeException("internal error: " + this.toString());
    }
  }
}
