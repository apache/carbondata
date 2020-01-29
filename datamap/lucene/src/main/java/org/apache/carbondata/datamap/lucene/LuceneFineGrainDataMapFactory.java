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

package org.apache.carbondata.datamap.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainDataMap;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 * FG level of lucene DataMap
 */
@InterfaceAudience.Internal
public class LuceneFineGrainDataMapFactory extends LuceneDataMapFactoryBase<FineGrainDataMap> {

  public LuceneFineGrainDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    super(carbonTable, dataMapSchema);
  }

  /**
   * Get the datamap for segmentId
   */
  @Override
  public List<FineGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<FineGrainDataMap> lstDataMap = new ArrayList<>();
    FineGrainDataMap dataMap = new LuceneFineGrainDataMap(analyzer, getDataMapSchema());
    dataMap.init(new DataMapModel(
        DataMapWriter.getDefaultDataMapPath(tableIdentifier.getTablePath(),
            segment.getSegmentNo(), dataMapName), segment.getConfiguration()));
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  @Override
  public List<FineGrainDataMap> getDataMaps(Segment segment, List<PartitionSpec> partitions)
      throws IOException {
    return getDataMaps(segment);
  }

  /**
   * Get datamaps for distributable object.
   */
  @Override
  public List<FineGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    List<FineGrainDataMap> lstDataMap = new ArrayList<>();
    FineGrainDataMap dataMap = new LuceneFineGrainDataMap(analyzer, getDataMapSchema());
    String indexPath = ((LuceneDataMapDistributable) distributable).getIndexPath();
    dataMap.init(new DataMapModel(indexPath, FileFactory.getConfiguration()));
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  @Override
  public DataMapLevel getDataMapLevel() {
    return DataMapLevel.FG;
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
