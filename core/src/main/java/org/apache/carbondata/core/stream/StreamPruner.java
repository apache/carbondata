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

package org.apache.carbondata.core.stream;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;

@InterfaceAudience.Internal
public class StreamPruner {

  private CarbonTable carbonTable;
  private FilterExecuter filterExecuter;

  private int totalFileNums = 0;

  public StreamPruner(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  public void init(FilterResolverIntf filterExp) {
    if (filterExp != null) {
      // cache all columns
      List<CarbonColumn> minMaxCacheColumns = new ArrayList<>();
      for (CarbonDimension dimension : carbonTable.getVisibleDimensions()) {
        if (!dimension.isComplex()) {
          minMaxCacheColumns.add(dimension);
        }
      }
      minMaxCacheColumns.addAll(carbonTable.getVisibleMeasures());
      // prepare cardinality of all dimensions
      List<ColumnSchema> listOfColumns =
          carbonTable.getTableInfo().getFactTable().getListOfColumns();
      // initial filter executor
      SegmentProperties segmentProperties = new SegmentProperties(listOfColumns);
      filterExecuter = FilterUtil.getFilterExecuterTree(
          filterExp, segmentProperties, null, minMaxCacheColumns, false);
    }
  }

  public List<StreamFile> prune(List<Segment> segments) throws IOException {
    if (filterExecuter == null) {
      // if filter is null, list all steam files
      return listAllStreamFiles(segments, false);
    } else {
      List<StreamFile> streamFileList = new ArrayList<>();
      for (StreamFile streamFile : listAllStreamFiles(segments, true)) {
        if (isScanRequire(streamFile)) {
          // if stream file is required to scan
          streamFileList.add(streamFile);
          streamFile.setMinMaxIndex(null);
        }
      }
      return streamFileList;
    }
  }

  private boolean isScanRequire(StreamFile streamFile) {
    // backward compatibility, old stream file without min/max index
    if (streamFile.getMinMaxIndex() == null) {
      return true;
    }
    byte[][] maxValue = streamFile.getMinMaxIndex().getMaxValues();
    byte[][] minValue = streamFile.getMinMaxIndex().getMinValues();
    BitSet bitSet = filterExecuter
        .isScanRequired(maxValue, minValue, streamFile.getMinMaxIndex().getIsMinMaxSet());
    if (!bitSet.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }

  // TODO optimize and move the code to StreamSegment , but it's in the streaming module.
  private List<StreamFile> listAllStreamFiles(List<Segment> segments, boolean withMinMax)
      throws IOException {
    List<StreamFile> streamFileList = new ArrayList<>();
    for (Segment segment : segments) {
      String segmentDir = CarbonTablePath.getSegmentPath(
          carbonTable.getAbsoluteTableIdentifier().getTablePath(), segment.getSegmentNo());
      String indexFile = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir);
      if (FileFactory.isFileExist(indexFile)) {
        CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
        indexReader.openThriftReader(indexFile);
        try {
          while (indexReader.hasNext()) {
            BlockIndex blockIndex = indexReader.readBlockIndexInfo();
            String filePath = segmentDir + File.separator + blockIndex.getFile_name();
            long length = blockIndex.getFile_size();
            StreamFile streamFile = new StreamFile(segment.getSegmentNo(), filePath, length);
            streamFileList.add(streamFile);
            if (withMinMax) {
              if (blockIndex.getBlock_index() != null
                  && blockIndex.getBlock_index().getMin_max_index() != null) {
                streamFile.setMinMaxIndex(CarbonMetadataUtil
                    .convertExternalMinMaxIndex(blockIndex.getBlock_index().getMin_max_index()));
              }
            }
          }
        } finally {
          indexReader.closeThriftReader();
        }
      }
    }
    totalFileNums = streamFileList.size();
    return streamFileList;
  }

  public int getTotalFileNums() {
    return totalFileNums;
  }
}
