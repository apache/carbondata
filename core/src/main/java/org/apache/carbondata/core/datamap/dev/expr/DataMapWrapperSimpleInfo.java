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

package org.apache.carbondata.core.datamap.dev.expr;

import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 * schema for datamap wrapper.
 * Currently a DataMapWrapper contains more than one datamap, this class is used to describe its
 * schema. For example a AndDataMapExprWrapper contains BloomFilter in its left and Lucene in
 * its right, then its schema would be AND(BloomFilter, Lucene)
 */
public class DataMapWrapperSimpleInfo {
  enum WrapperType {
    PRIMITIVE,
    AND,
    OR
  }

  private WrapperType wrapperType;
  private DataMapWrapperSimpleInfo left;
  private DataMapWrapperSimpleInfo right;
  private DataMapSchema schema;

  private DataMapWrapperSimpleInfo(WrapperType wrapperType, DataMapWrapperSimpleInfo left,
      DataMapWrapperSimpleInfo right) {
    this.wrapperType = wrapperType;
    this.left = left;
    this.right = right;
  }

  private DataMapWrapperSimpleInfo(DataMapSchema schema) {
    this.wrapperType = WrapperType.PRIMITIVE;
    this.schema = schema;
  }

  public static DataMapWrapperSimpleInfo fromDataMapWrapper(DataMapExprWrapper dataMapExprWrapper) {
    if (dataMapExprWrapper instanceof DataMapExprWrapperImpl) {
      return new DataMapWrapperSimpleInfo(
          ((DataMapExprWrapperImpl) dataMapExprWrapper).getDataMapSchema());
    } else if (dataMapExprWrapper instanceof AndDataMapExprWrapper) {
      return new DataMapWrapperSimpleInfo(WrapperType.AND,
          fromDataMapWrapper(dataMapExprWrapper.getLeftDataMapWrapper()),
          fromDataMapWrapper(dataMapExprWrapper.getRightDataMapWrapprt()));
    } else {
      return new DataMapWrapperSimpleInfo(WrapperType.OR,
          fromDataMapWrapper(dataMapExprWrapper.getLeftDataMapWrapper()),
          fromDataMapWrapper(dataMapExprWrapper.getRightDataMapWrapprt()));
    }
  }

  public String getDataMapWrapperName() {
    if (WrapperType.PRIMITIVE == wrapperType) {
      return schema.getDataMapName();
    } else {
      return String.format("%s(%s, %s)",
          wrapperType, left.getDataMapWrapperName(), right.getDataMapWrapperName());
    }
  }

  public String getDataMapWrapperProvider() {
    if (WrapperType.PRIMITIVE == wrapperType) {
      return schema.getProviderName();
    } else {
      return String.format("%s(%s, %s)",
          wrapperType, left.getDataMapWrapperProvider(), right.getDataMapWrapperProvider());
    }
  }

  @Override
  public String toString() {
    return "DatamapWrapperSchema: Name->" + getDataMapWrapperName()
        + ", Provider->" + getDataMapWrapperProvider();
  }
}
