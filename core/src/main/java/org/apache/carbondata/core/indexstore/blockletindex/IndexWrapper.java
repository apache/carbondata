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
package org.apache.carbondata.core.indexstore.blockletindex;

import java.util.List;

import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;

/**
 * Wrapper of abstract index
 * TODO it could be removed after refactor
 */
public class IndexWrapper extends AbstractIndex {

  private List<TableBlockInfo> blockInfos;

  public IndexWrapper(List<TableBlockInfo> blockInfos) {
    this.blockInfos = blockInfos;
    segmentProperties = new SegmentProperties(blockInfos.get(0).getDetailInfo().getColumnSchemas(),
        blockInfos.get(0).getDetailInfo().getDimLens());
    dataRefNode = new BlockletDataRefNode(blockInfos, 0,
        segmentProperties.getDimensionColumnsValueSize());
  }

  @Override public void buildIndex(List<DataFileFooter> footerList) {
  }

  @Override public void clear() {
    super.clear();
    if (blockInfos != null) {
      for (TableBlockInfo blockInfo : blockInfos) {
        String dataMapWriterPath = blockInfo.getDataMapWriterPath();
        if (dataMapWriterPath != null) {
          CarbonFile file = FileFactory.getCarbonFile(dataMapWriterPath);
          FileFactory.deleteAllCarbonFilesOfDir(file);
        }
      }
    }
  }
}
