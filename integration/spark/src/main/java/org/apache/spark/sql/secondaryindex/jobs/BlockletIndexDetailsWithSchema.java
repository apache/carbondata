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

package org.apache.spark.sql.secondaryindex.jobs;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletIndexWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * class that holds indexes, column cardinality, columnSchema and other related information for
 * BlockletIndexInputFormat return value
 * TODO: When this code is moved to open source, this class can be removed and the required code
 * can be added to BlockletIndexWrapper class
 */
public class BlockletIndexDetailsWithSchema implements Serializable {

  private static final long serialVersionUID = 8879439848531370730L;

  private BlockletIndexWrapper blockletIndexWrapper;

  private List<ColumnSchema> columnSchemaList;

  public BlockletIndexDetailsWithSchema(
      BlockletIndexWrapper blockletIndexWrapper, boolean isSchemaModified) {
    this.blockletIndexWrapper = blockletIndexWrapper;
    List<BlockIndex> indexes = blockletIndexWrapper.getIndexes();
    if (!indexes.isEmpty()) {
      // In one task all indexes will have the same cardinality and schema therefore
      // segmentPropertyIndex can be fetched from one index
      SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper
          segmentPropertiesWrapper = indexes.get(0).getSegmentPropertiesWrapper();
      // flag to check whether carbon table schema is modified. ColumnSchemaList will be
      // serialized from executor to driver only if schema is modified
      if (isSchemaModified) {
        columnSchemaList = segmentPropertiesWrapper.getColumnsInTable();
      }
    }
  }

  public BlockletIndexWrapper getBlockletIndexWrapper() {
    return blockletIndexWrapper;
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

}
