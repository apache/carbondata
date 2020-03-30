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

package org.apache.carbondata.core.index.dev.expr;

import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 * schema for index wrapper.
 * Currently a IndexWrapper contains more than one index, this class is used to describe its
 * schema. For example a AndIndexExprWrapper contains BloomFilter in its left and Lucene in
 * its right, then its schema would be AND(BloomFilter, Lucene)
 */
public class IndexWrapperSimpleInfo {
  enum WrapperType {
    PRIMITIVE,
    AND,
    OR
  }

  private WrapperType wrapperType;
  private IndexWrapperSimpleInfo left;
  private IndexWrapperSimpleInfo right;
  private IndexSchema schema;

  private IndexWrapperSimpleInfo(WrapperType wrapperType, IndexWrapperSimpleInfo left,
      IndexWrapperSimpleInfo right) {
    this.wrapperType = wrapperType;
    this.left = left;
    this.right = right;
  }

  private IndexWrapperSimpleInfo(IndexSchema schema) {
    this.wrapperType = WrapperType.PRIMITIVE;
    this.schema = schema;
  }

  public static IndexWrapperSimpleInfo fromIndexWrapper(IndexExprWrapper indexExprWrapper) {
    if (indexExprWrapper instanceof IndexExprWrapperImpl) {
      return new IndexWrapperSimpleInfo(
          ((IndexExprWrapperImpl) indexExprWrapper).getIndexSchema());
    } else if (indexExprWrapper instanceof AndIndexExprWrapper) {
      return new IndexWrapperSimpleInfo(WrapperType.AND,
          fromIndexWrapper(indexExprWrapper.getLeftIndexWrapper()),
          fromIndexWrapper(indexExprWrapper.getRightIndexWrapprt()));
    } else {
      return new IndexWrapperSimpleInfo(WrapperType.OR,
          fromIndexWrapper(indexExprWrapper.getLeftIndexWrapper()),
          fromIndexWrapper(indexExprWrapper.getRightIndexWrapprt()));
    }
  }

  public String getIndexWrapperName() {
    if (WrapperType.PRIMITIVE == wrapperType) {
      return schema.getIndexName();
    } else {
      return String.format("%s(%s, %s)",
          wrapperType, left.getIndexWrapperName(), right.getIndexWrapperName());
    }
  }

  public String getIndexWrapperProvider() {
    if (WrapperType.PRIMITIVE == wrapperType) {
      return schema.getProviderName();
    } else {
      return String.format("%s(%s, %s)",
          wrapperType, left.getIndexWrapperProvider(), right.getIndexWrapperProvider());
    }
  }

  @Override
  public String toString() {
    return "IndexWrapperSchema: Name->" + getIndexWrapperName()
        + ", Provider->" + getIndexWrapperProvider();
  }
}
