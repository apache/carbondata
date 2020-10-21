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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Subclass of this codec depends on statistics of the column page (adaptive) to perform apply
 * and decode, it also employs compressor to compress the encoded data
 */
public abstract class AdaptiveCodec implements ColumnPageCodec {

  // TODO: cache and reuse the same encoder since snappy is thread-safe

  // statistics of this page, can be used by subclass
  protected final SimpleStatsResult stats;

  // the data type used for storage
  protected final DataType targetDataType;

  // the data type specified in schema
  protected final DataType srcDataType;

  protected AdaptiveCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    this.stats = stats;
    this.srcDataType = srcDataType;
    this.targetDataType = targetDataType;
  }

  public DataType getTargetDataType() {
    return targetDataType;
  }

  @Override
  public String toString() {
    return String.format("%s[src type: %s, target type: %s, stats(%s)]",
        getClass().getName(), srcDataType, targetDataType, stats);
  }

  protected String debugInfo() {
    return this.toString();
  }

}
