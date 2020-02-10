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

package org.apache.spark.sql.secondaryindex.Jobs;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * class that holds dataMaps, column cardinality, columnSchema and other related information for
 * DistributableBlockletDataMapLoader return value
 * TODO: When this code is moved to open source, this class can be removed and the required code
 * can be added to BlockletDataMapIndexWrapper class
 */
public class BlockletDataMapDetailsWithSchema implements Serializable {

  private static final long serialVersionUID = 8879439848531370730L;

  private BlockletDataMapIndexWrapper blockletDataMapIndexWrapper;

  private List<ColumnSchema> columnSchemaList;

  public BlockletDataMapDetailsWithSchema(
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper, boolean isSchemaModified) {
    this.blockletDataMapIndexWrapper = blockletDataMapIndexWrapper;
    List<BlockDataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
    if (!dataMaps.isEmpty()) {
      // In one task all dataMaps will have the same cardinality and schema therefore
      // segmentPropertyIndex can be fetched from one dataMap
      SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper
          segmentPropertiesWrapper = dataMaps.get(0).getSegmentPropertiesWrapper();
      // flag to check whether carbon table schema is modified. ColumnSchemaList will be
      // serialized from executor to driver only if schema is modified
      if (isSchemaModified) {
        columnSchemaList = segmentPropertiesWrapper.getColumnsInTable();
      }
    }
  }

  public BlockletDataMapIndexWrapper getBlockletDataMapIndexWrapper() {
    return blockletDataMapIndexWrapper;
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

}
