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

package org.apache.carbondata.core.datamap;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang3.StringUtils;

/**
 * Metadata of the datamap, set by DataMap developer
 */
@InterfaceAudience.Developer("DataMap")
@InterfaceStability.Evolving
public class DataMapMeta {
  private String dataMapName;

  private List<CarbonColumn> indexedColumns;

  private List<ExpressionType> optimizedOperation;

  public DataMapMeta(List<CarbonColumn> indexedColumns,
      List<ExpressionType> optimizedOperation) {
    this.indexedColumns = indexedColumns;
    this.optimizedOperation = optimizedOperation;
  }

  public DataMapMeta(String dataMapName, List<CarbonColumn> indexedColumns,
      List<ExpressionType> optimizedOperation) {
    this(indexedColumns, optimizedOperation);
    this.dataMapName = dataMapName;
  }

  public String getDataMapName() {
    return dataMapName;
  }

  public List<CarbonColumn> getIndexedColumns() {
    return indexedColumns;
  }

  public List<String> getIndexedColumnNames() {
    return (List<String>) CollectionUtils.collect(indexedColumns, new Transformer() {
      @Override public Object transform(Object input) {
        return ((CarbonColumn) input).getColName();
      }
    });
  }

  public List<ExpressionType> getOptimizedOperation() {
    return optimizedOperation;
  }

  @Override public String toString() {
    return new StringBuilder("DataMapMeta{")
        .append("dataMapName='").append(dataMapName).append('\'')
        .append(", indexedColumns=[")
        .append(StringUtils.join(getIndexedColumnNames(), ", ")).append("]\'")
        .append(", optimizedOperation=").append(optimizedOperation)
        .append('}')
        .toString();
  }
}
