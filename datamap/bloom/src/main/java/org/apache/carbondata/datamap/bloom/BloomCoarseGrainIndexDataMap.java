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
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.IndexDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang3.StringUtils;

@InterfaceAudience.Internal
public class BloomCoarseGrainIndexDataMap extends IndexDataMap<CoarseGrainDataMap> {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      BloomCoarseGrainIndexDataMap.class.getName());
  /**
   * property for size of bloom filter
   */
  private static final String BLOOM_SIZE = "bloom_size";
  /**
   * default size for bloom filter: suppose one blocklet contains 20 pages
   * and all the indexed value is distinct.
   */
  private static final int DEFAULT_BLOOM_FILTER_SIZE = 32000 * 20;
  private CarbonTable carbonTable;
  private DataMapMeta dataMapMeta;
  private String dataMapName;
  private int bloomFilterSize;

  @Override
  public void init(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    Objects.requireNonNull(carbonTable);
    Objects.requireNonNull(dataMapSchema);

    this.carbonTable = carbonTable;
    this.dataMapName = dataMapSchema.getDataMapName();

    List<String> indexedColumns = getIndexedColumns(dataMapSchema);
    this.bloomFilterSize = validateAndGetBloomFilterSize(dataMapSchema);
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // todo: support more optimize operations
    optimizedOperations.add(ExpressionType.EQUALS);
    this.dataMapMeta = new DataMapMeta(this.dataMapName, indexedColumns, optimizedOperations);
    LOGGER.info(String.format("DataMap %s works for %s with bloom size %d",
        this.dataMapName, this.dataMapMeta, this.bloomFilterSize));
  }

  /**
   * validate Lucene DataMap BLOOM_SIZE
   * 1. BLOOM_SIZE property is optional, 32000 * 20 will be the default size.
   * 2. BLOOM_SIZE should be an integer that greater than 0
   */
  private int validateAndGetBloomFilterSize(DataMapSchema dmSchema)
      throws MalformedDataMapCommandException {
    String bloomFilterSizeStr = dmSchema.getProperties().get(BLOOM_SIZE);
    if (StringUtils.isBlank(bloomFilterSizeStr)) {
      LOGGER.warn(
          String.format("Bloom filter size is not configured for datamap %s, use default value %d",
              dataMapName, DEFAULT_BLOOM_FILTER_SIZE));
      return DEFAULT_BLOOM_FILTER_SIZE;
    }
    int bloomFilterSize;
    try {
      bloomFilterSize = Integer.parseInt(bloomFilterSizeStr);
    } catch (NumberFormatException e) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be an integer",
              bloomFilterSizeStr));
    }
    // todo: reconsider the boundaries of bloom filter size
    if (bloomFilterSize <= 0) {
      throw new MalformedDataMapCommandException(
          String.format("Invalid value of bloom filter size '%s', it should be greater than 0",
              bloomFilterSizeStr));
    }
    return bloomFilterSize;
  }

  @Override
  public DataMapWriter createWriter(Segment segment, String writeDirectoryPath) {
    LOGGER.info(
        String.format("Data of BloomCoarseGranDataMap %s for table %s will be written to %s",
            this.dataMapName, this.carbonTable.getTableName() , writeDirectoryPath));
    return new BloomDataMapWriter(this.carbonTable.getAbsoluteTableIdentifier(),
        this.dataMapMeta, this.bloomFilterSize, segment, writeDirectoryPath);
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<CoarseGrainDataMap>(1);
    try {
      String dataMapStorePath = BloomDataMapWriter.genDataMapStorePath(
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath(), segment.getSegmentNo()),
          dataMapName);
      CarbonFile[] carbonFiles = FileFactory.getCarbonFile(dataMapStorePath).listFiles();
      for (CarbonFile carbonFile : carbonFiles) {
        BloomCoarseGrainDataMap bloomDM = new BloomCoarseGrainDataMap();
        bloomDM.init(new DataMapModel(carbonFile.getAbsolutePath()));
        dataMaps.add(bloomDM);
      }
    } catch (Exception e) {
      throw new IOException("Error occurs while init Bloom DataMap", e);
    }
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
