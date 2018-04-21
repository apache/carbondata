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
package org.apache.carbondata.datamap.bloom;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang3.StringUtils;

@InterfaceAudience.Internal
public class BloomCoarseGrainDataMapFactory implements DataMapFactory<CoarseGrainDataMap> {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      BloomCoarseGrainDataMapFactory.class.getName());
  private static final String BLOOM_COLUMNS = "bloom_columns";
  private CarbonTable carbonTable;
  private DataMapMeta dataMapMeta;
  private String dataMapName;

  @Override
  public void init(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws IOException, MalformedDataMapCommandException {
    Objects.requireNonNull(carbonTable);
    Objects.requireNonNull(dataMapSchema);

    this.carbonTable = carbonTable;
    this.dataMapName = dataMapSchema.getDataMapName();

    List<String> indexedColumns = validateAndGetIndexedColumns(dataMapSchema, carbonTable);
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // todo: support more optimize operations
    optimizedOperations.add(ExpressionType.EQUALS);
    this.dataMapMeta = new DataMapMeta(this.dataMapName, indexedColumns, optimizedOperations);
    LOGGER.info(String.format("DataMap %s works for %s", this.dataMapName, this.dataMapMeta));
  }

  /**
   * validate Lucene DataMap
   * 1. require BLOOM_COLUMNS property
   * 2. BLOOM_COLUMNS can't contains illegal argument(empty, blank)
   * 3. BLOOM_COLUMNS can't contains duplicate same columns
   * 4. BLOOM_COLUMNS should be exists in table columns
   */
  private List<String> validateAndGetIndexedColumns(DataMapSchema dmSchema,
      CarbonTable carbonTable) throws MalformedDataMapCommandException {
    String bloomColumnsStr = dmSchema.getProperties().get(BLOOM_COLUMNS);
    if (StringUtils.isBlank(bloomColumnsStr)) {
      throw new MalformedDataMapCommandException(
          String.format("Bloom coarse datamap require proper %s property", BLOOM_COLUMNS));
    }
    String[] bloomColumns = StringUtils.split(bloomColumnsStr, ",", -1);
    List<String> bloomColumnList = new ArrayList<String>(bloomColumns.length);
    Set<String> bloomColumnSet = new HashSet<String>(bloomColumns.length);
    for (String bloomCol : bloomColumns) {
      CarbonColumn column = carbonTable.getColumnByName(carbonTable.getTableName(),
          bloomCol.trim().toLowerCase());
      if (null == column) {
        throw new MalformedDataMapCommandException(
            String.format("%s: %s does not exist in table. Please check create datamap statement",
                BLOOM_COLUMNS, bloomCol));
      }
      if (!bloomColumnSet.add(column.getColName())) {
        throw new MalformedDataMapCommandException(String.format("%s has duplicate column: %s",
            BLOOM_COLUMNS, bloomCol));
      }
      bloomColumnList.add(column.getColName());
    }
    return bloomColumnList;
  }

  @Override
  public DataMapWriter createWriter(Segment segment, String writeDirectoryPath) {
    LOGGER.info(
        String.format("Data of BloomCoarseGranDataMap %s for table %s will be written to %s",
            this.dataMapName, this.carbonTable.getTableName() , writeDirectoryPath));
    return new BloomDataMapWriter(this.carbonTable.getAbsoluteTableIdentifier(),
        this.dataMapMeta, segment, writeDirectoryPath);
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<CoarseGrainDataMap>(1);
    BloomCoarseGrainDataMap bloomDM = new BloomCoarseGrainDataMap();
    try {
      bloomDM.init(new DataMapModel(BloomDataMapWriter.genDataMapStorePath(
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath(), segment.getSegmentNo()),
          dataMapName)));
    } catch (MemoryException e) {
      throw new IOException("Error occurs while init Bloom DataMap", e);
    }
    dataMaps.add(bloomDM);
    return dataMaps;
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    return null;
  }

  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    return null;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(Segment segment) {

  }

  @Override
  public void clear() {

  }

  @Override
  public void deleteDatamapData() {
    SegmentStatusManager ssm = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier());
    try {
      List<Segment> validSegments = ssm.getValidAndInvalidSegments().getValidSegments();
      for (Segment segment : validSegments) {
        String segmentId = segment.getSegmentNo();
        String datamapPath = CarbonTablePath.getSegmentPath(
            carbonTable.getAbsoluteTableIdentifier().getTablePath(), segmentId)
            + File.separator + dataMapName;
        if (FileFactory.isFileExist(datamapPath)) {
          CarbonFile file = FileFactory.getCarbonFile(datamapPath,
              FileFactory.getFileType(datamapPath));
          CarbonUtil.deleteFoldersAndFilesSilent(file);
        }
      }
    } catch (IOException | InterruptedException ex) {
      LOGGER.error("drop datamap failed, failed to delete datamap directory");
    }
  }
  @Override
  public DataMapMeta getMeta() {
    return this.dataMapMeta;
  }

  @Override
  public DataMapLevel getDataMapType() {
    return DataMapLevel.CG;
  }
}
