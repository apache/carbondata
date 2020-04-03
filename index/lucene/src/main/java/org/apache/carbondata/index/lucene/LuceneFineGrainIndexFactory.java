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

package org.apache.carbondata.index.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.index.IndexInputSplit;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.dev.IndexModel;
import org.apache.carbondata.core.index.dev.IndexWriter;
import org.apache.carbondata.core.index.dev.fgindex.FineGrainIndex;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

/**
 * FG level of lucene Index
 */
@InterfaceAudience.Internal
public class LuceneFineGrainIndexFactory extends LuceneIndexFactoryBase<FineGrainIndex> {

  public LuceneFineGrainIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema)
      throws MalformedIndexCommandException {
    super(carbonTable, indexSchema);
  }

  /**
   * Get the datamap for segmentId
   */
  @Override
  public List<FineGrainIndex> getIndexes(Segment segment) throws IOException {
    List<FineGrainIndex> lstDataMap = new ArrayList<>();
    FineGrainIndex dataMap = new LuceneFineGrainIndex(analyzer, getIndexSchema());
    dataMap.init(new IndexModel(
        IndexWriter.getDefaultIndexPath(tableIdentifier.getTablePath(),
            segment.getSegmentNo(), dataMapName), segment.getConfiguration()));
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  @Override
  public List<FineGrainIndex> getIndexes(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    return getIndexes(segment);
  }

  /**
   * Get datamaps for distributable object.
   */
  @Override
  public List<FineGrainIndex> getIndexes(IndexInputSplit distributable)
      throws IOException {
    List<FineGrainIndex> lstDataMap = new ArrayList<>();
    FineGrainIndex dataMap = new LuceneFineGrainIndex(analyzer, getIndexSchema());
    String indexPath = ((LuceneIndexInputSplit) distributable).getIndexPath();
    dataMap.init(new IndexModel(indexPath, FileFactory.getConfiguration()));
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return IndexLevel.FG;
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    switch (operation) {
      case ALTER_RENAME:
        return true;
      case ALTER_DROP:
        return true;
      case ALTER_ADD_COLUMN:
        return true;
      case ALTER_CHANGE_DATATYPE:
        return true;
      case ALTER_COLUMN_RENAME:
        return true;
      case STREAMING:
        return false;
      case DELETE:
        return true;
      case UPDATE:
        return true;
      case PARTITION:
        return true;
      default:
        return false;
    }
  }
}
