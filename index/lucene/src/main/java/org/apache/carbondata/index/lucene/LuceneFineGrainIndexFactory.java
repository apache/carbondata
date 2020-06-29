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
import java.util.Set;

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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;

import org.apache.hadoop.fs.Path;

/**
 * FG level of lucene Index
 */
@InterfaceAudience.Internal
public class LuceneFineGrainIndexFactory extends LuceneIndexFactoryBase<FineGrainIndex> {

  public LuceneFineGrainIndexFactory(CarbonTable carbonTable, IndexSchema indexSchema)
      throws MalformedIndexCommandException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    super(carbonTable, indexSchema);
  }

  /**
   * Get the index for segmentId
   */
  @Override
  public List<FineGrainIndex> getIndexes(Segment segment) throws IOException {
    List<FineGrainIndex> indexes = new ArrayList<>();
    FineGrainIndex index = new LuceneFineGrainIndex(analyzer, getIndexSchema());
    index.init(
        new IndexModel(
            IndexWriter.getDefaultIndexPath(
                tableIdentifier.getTablePath(),
                segment.getSegmentNo(),
                indexName),
            segment.getConfiguration()));
    indexes.add(index);
    return indexes;
  }

  @Override
  public List<FineGrainIndex> getIndexes(Segment segment, Set<Path> partitionLocations)
      throws IOException {
    return getIndexes(segment);
  }

  /**
   * Get indexes for distributable object.
   */
  @Override
  public List<FineGrainIndex> getIndexes(IndexInputSplit distributable)
      throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
    List<FineGrainIndex> indexes = new ArrayList<>();
    FineGrainIndex index = new LuceneFineGrainIndex(analyzer, getIndexSchema());
    String indexPath = ((LuceneIndexInputSplit) distributable).getIndexPath();
    index.init(new IndexModel(indexPath, FileFactory.getConfiguration()));
    indexes.add(index);
    return indexes;
  }

  @Override
  public IndexLevel getIndexLevel() {
    return IndexLevel.FG;
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2347
    switch (operation) {
      case ALTER_RENAME:
        return true;
      case ALTER_DROP:
        return true;
      case ALTER_ADD_COLUMN:
        return true;
      case ALTER_CHANGE_DATATYPE:
        return true;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3149
      case ALTER_COLUMN_RENAME:
        return true;
      case STREAMING:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2823
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
