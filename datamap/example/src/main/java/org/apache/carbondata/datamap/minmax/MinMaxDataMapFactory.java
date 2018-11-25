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

package org.apache.carbondata.datamap.minmax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapLevel;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.features.TableOperation;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.log4j.Logger;

/**
 * Min Max DataMap Factory
 */
@InterfaceAudience.Internal
public class MinMaxDataMapFactory extends CoarseGrainDataMapFactory {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MinMaxDataMapFactory.class.getName());
  private DataMapMeta dataMapMeta;
  private String dataMapName;
  // segmentId -> list of index files
  private Map<String, Set<String>> segmentMap = new ConcurrentHashMap<>();
  private Cache<MinMaxDataMapCacheKeyValue.Key, MinMaxDataMapCacheKeyValue.Value> cache;

  public MinMaxDataMapFactory(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    super(carbonTable, dataMapSchema);

    // this is an example for datamap, we can choose the columns and operations that
    // will be supported by this datamap. Furthermore, we can add cache-support for this datamap.

    this.dataMapName = dataMapSchema.getDataMapName();
    List<CarbonColumn> indexedColumns = carbonTable.getIndexedColumns(dataMapSchema);

    // operations that will be supported on the indexed columns
    List<ExpressionType> optOperations = new ArrayList<>();
    optOperations.add(ExpressionType.NOT);
    optOperations.add(ExpressionType.EQUALS);
    optOperations.add(ExpressionType.NOT_EQUALS);
    optOperations.add(ExpressionType.GREATERTHAN);
    optOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
    optOperations.add(ExpressionType.LESSTHAN);
    optOperations.add(ExpressionType.LESSTHAN_EQUALTO);
    optOperations.add(ExpressionType.IN);
    this.dataMapMeta = new DataMapMeta(indexedColumns, optOperations);

    // init cache. note that the createCache ensures the singleton of the cache
    try {
      this.cache = CacheProvider.getInstance()
          .createCache(new CacheType("minmax_cache"), MinMaxDataMapCache.class.getName());
    } catch (Exception e) {
      LOGGER.error("Failed to create cache for minmax datamap", e);
      throw new MalformedDataMapCommandException(e.getMessage());
    }
  }

  /**
   * createWriter will return the MinMaxDataWriter.
   *
   * @param segment
   * @param shardName
   * @return
   */
  @Override
  public DataMapWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format(
          "Data of MinMaxDataMap %s for table %s will be written to %s",
          dataMapName, getCarbonTable().getTableName(), shardName));
    }
    return new MinMaxDataMapDirectWriter(getCarbonTable().getTablePath(), dataMapName,
        dataMapMeta.getIndexedColumns(), segment, shardName, segmentProperties);
  }

  @Override
  public DataMapBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) throws IOException {
    return new MinMaxDataMapBuilder(getCarbonTable().getTablePath(), dataMapName,
        dataMapMeta.getIndexedColumns(), segment, shardName, segmentProperties);
  }

  /**
   * getDataMaps Factory method Initializes the Min Max Data Map and returns.
   *
   * @param segment
   * @return
   * @throws IOException
   */
  @Override
  public List<CoarseGrainDataMap> getDataMaps(Segment segment) throws IOException {
    List<CoarseGrainDataMap> dataMaps = new ArrayList<>();
    Set<String> shardPaths = segmentMap.get(segment.getSegmentNo());
    if (shardPaths == null) {
      String dataMapStorePath = DataMapWriter.getDefaultDataMapPath(
          getCarbonTable().getTablePath(), segment.getSegmentNo(), dataMapName);
      CarbonFile[] carbonFiles = FileFactory.getCarbonFile(dataMapStorePath).listFiles();
      shardPaths = new HashSet<>();
      for (CarbonFile carbonFile : carbonFiles) {
        shardPaths.add(carbonFile.getAbsolutePath());
      }
      segmentMap.put(segment.getSegmentNo(), shardPaths);
    }

    for (String shard : shardPaths) {
      MinMaxIndexDataMap dataMap = new MinMaxIndexDataMap();
      dataMap.init(new MinMaxDataMapModel(shard, cache, segment.getConfiguration()));
      dataMap.initOthers(getCarbonTable(), dataMapMeta.getIndexedColumns());
      dataMaps.add(dataMap);
    }
    return dataMaps;
  }

  @Override
  public DataMapMeta getMeta() {
    return this.dataMapMeta;
  }

  @Override
  public DataMapLevel getDataMapLevel() {
    return DataMapLevel.CG;
  }

  @Override
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    List<CoarseGrainDataMap> coarseGrainDataMaps = new ArrayList<>();
    MinMaxIndexDataMap minMaxIndexDataMap = new MinMaxIndexDataMap();
    String indexPath = ((MinMaxDataMapDistributable) distributable).getIndexPath();
    minMaxIndexDataMap.init(
        new MinMaxDataMapModel(indexPath, cache, FileFactory.getConfiguration()));
    minMaxIndexDataMap.initOthers(getCarbonTable(), dataMapMeta.getIndexedColumns());
    coarseGrainDataMaps.add(minMaxIndexDataMap);
    return coarseGrainDataMaps;
  }

  /**
   * returns all the directories of lucene index files for query
   * Note: copied from BloomFilterDataMapFactory, will extract to a common interface
   */
  private CarbonFile[] getAllIndexDirs(String tablePath, String segmentId) {
    List<CarbonFile> indexDirs = new ArrayList<>();
    List<TableDataMap> dataMaps;
    try {
      // there can be multiple bloom datamaps present on a table, so get all datamaps and form
      // the path till the index file directories in all datamaps folders present in each segment
      dataMaps = DataMapStoreManager.getInstance().getAllDataMap(getCarbonTable());
    } catch (IOException ex) {
      LOGGER.error(String.format(
          "failed to get datamaps for tablePath %s, segmentId %s", tablePath, segmentId), ex);
      throw new RuntimeException(ex);
    }
    if (dataMaps.size() > 0) {
      for (TableDataMap dataMap : dataMaps) {
        if (dataMap.getDataMapSchema().getDataMapName().equals(this.dataMapName)) {
          List<CarbonFile> indexFiles;
          String dmPath = CarbonTablePath.getDataMapStorePath(tablePath, segmentId,
              dataMap.getDataMapSchema().getDataMapName());
          FileFactory.FileType fileType = FileFactory.getFileType(dmPath);
          final CarbonFile dirPath = FileFactory.getCarbonFile(dmPath, fileType);
          indexFiles = Arrays.asList(dirPath.listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              return file.isDirectory();
            }
          }));
          indexDirs.addAll(indexFiles);
        }
      }
    }
    return indexDirs.toArray(new CarbonFile[0]);
  }

  @Override
  public List<DataMapDistributable> toDistributable(Segment segment) {
    List<DataMapDistributable> dataMapDistributableList = new ArrayList<>();
    CarbonFile[] indexDirs =
        getAllIndexDirs(getCarbonTable().getTablePath(), segment.getSegmentNo());
    if (segment.getFilteredIndexShardNames().size() == 0) {
      for (CarbonFile indexDir : indexDirs) {
        DataMapDistributable bloomDataMapDistributable =
            new MinMaxDataMapDistributable(indexDir.getAbsolutePath());
        dataMapDistributableList.add(bloomDataMapDistributable);
      }
      return dataMapDistributableList;
    }
    for (CarbonFile indexDir : indexDirs) {
      // Filter out the tasks which are filtered through CG datamap.
      if (!segment.getFilteredIndexShardNames().contains(indexDir.getName())) {
        continue;
      }
      DataMapDistributable bloomDataMapDistributable =
          new MinMaxDataMapDistributable(indexDir.getAbsolutePath());
      dataMapDistributableList.add(bloomDataMapDistributable);
    }
    return dataMapDistributableList;
  }

  @Override
  public void fireEvent(Event event) {

  }

  @Override
  public void clear(Segment segment) {
    Set<String> shards = segmentMap.remove(segment.getSegmentNo());
    if (null != shards) {
      for (String shard : shards) {
        cache.invalidate(new MinMaxDataMapCacheKeyValue.Key(shard));
      }
    }
  }

  @Override
  public synchronized void clear() {
    if (segmentMap.size() > 0) {
      List<String> segments = new ArrayList<>(segmentMap.keySet());
      for (String segmentId : segments) {
        clear(new Segment(segmentId, null, null));
      }
    }
  }

  @Override
  public void deleteDatamapData(Segment segment) throws IOException {
    try {
      String segmentId = segment.getSegmentNo();
      String datamapPath = CarbonTablePath
          .getDataMapStorePath(getCarbonTable().getTablePath(), segmentId, dataMapName);
      if (FileFactory.isFileExist(datamapPath)) {
        CarbonFile file =
            FileFactory.getCarbonFile(datamapPath, FileFactory.getFileType(datamapPath));
        CarbonUtil.deleteFoldersAndFilesSilent(file);
      }
    } catch (InterruptedException ex) {
      throw new IOException("Failed to delete datamap for segment_" + segment.getSegmentNo());
    }
  }

  @Override
  public void deleteDatamapData() {
    SegmentStatusManager ssm =
        new SegmentStatusManager(getCarbonTable().getAbsoluteTableIdentifier());
    try {
      List<Segment> validSegments = ssm.getValidAndInvalidSegments().getValidSegments();
      for (Segment segment : validSegments) {
        deleteDatamapData(segment);
      }
    } catch (IOException e) {
      LOGGER.error("drop datamap failed, failed to delete datamap directory");
    }
  }

  @Override
  public boolean willBecomeStale(TableOperation operation) {
    switch (operation) {
      case ALTER_RENAME:
        return false;
      case ALTER_DROP:
        return true;
      case ALTER_ADD_COLUMN:
        return false;
      case ALTER_CHANGE_DATATYPE:
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

  @Override
  public boolean isOperationBlocked(TableOperation operation, Object... targets) {
    switch (operation) {
      case ALTER_DROP: {
        // alter table drop columns
        // will be blocked if the columns in bloomfilter datamap
        List<String> columnsToDrop = (List<String>) targets[0];
        List<String> indexedColumnNames = dataMapMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          for (String column : columnsToDrop) {
            if (column.equalsIgnoreCase(indexedcolumn)) {
              return true;
            }
          }
        }
        return false;
      }
      case ALTER_CHANGE_DATATYPE: {
        // alter table change one column datatype
        // will be blocked if the column in bloomfilter datamap
        String columnToChangeDatatype = (String) targets[0];
        List<String> indexedColumnNames = dataMapMeta.getIndexedColumnNames();
        for (String indexedcolumn : indexedColumnNames) {
          if (indexedcolumn.equalsIgnoreCase(columnToChangeDatatype)) {
            return true;
          }
        }
        return false;
      }
      default:
        return false;
    }
  }

  @Override
  public boolean supportRebuild() {
    return false;
  }
}
