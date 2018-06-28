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
 * schema for datamap wrapper
 */
public class DataMapWrapperSchema {
  enum WrapperType {
    PRIMITIVE,
    AND,
    OR
  }

  private WrapperType wrapperType;
  private DataMapWrapperSchema left;
  private DataMapWrapperSchema right;
  private DataMapSchema schema;

  private DataMapWrapperSchema(WrapperType wrapperType, DataMapWrapperSchema left,
      DataMapWrapperSchema right) {
    this.wrapperType = wrapperType;
    this.left = left;
    this.right = right;
  }

  private DataMapWrapperSchema(DataMapSchema schema) {
    this.wrapperType = WrapperType.PRIMITIVE;
    this.schema = schema;
  }

  public static DataMapWrapperSchema fromDataMapWrapper(DataMapExprWrapper dataMapExprWrapper) {
    if (dataMapExprWrapper instanceof DataMapExprWrapperImpl) {
      return new DataMapWrapperSchema(
          ((DataMapExprWrapperImpl) dataMapExprWrapper).getDataMapSchema());
    } else if (dataMapExprWrapper instanceof AndDataMapExprWrapper) {
      return new DataMapWrapperSchema(WrapperType.AND,
          fromDataMapWrapper(dataMapExprWrapper.getLeftDataMapWrapper()),
          fromDataMapWrapper(dataMapExprWrapper.getRightDataMapWrapprt()));
    } else {
      return new DataMapWrapperSchema(WrapperType.OR,
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
