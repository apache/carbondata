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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.IndexInputSplit;
import org.apache.carbondata.core.datamap.IndexLevel;
import org.apache.carbondata.core.datamap.IndexMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableIndex;
import org.apache.carbondata.core.datamap.dev.Index;
import org.apache.carbondata.core.datamap.dev.IndexBuilder;
import org.apache.carbondata.core.datamap.dev.IndexFactory;
import org.apache.carbondata.core.datamap.dev.IndexWriter;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Base implementation for CG and FG lucene index factory.
 */
@InterfaceAudience.Internal
abstract class LuceneIndexFactoryBase<T extends Index> extends IndexFactory<T> {

  /**
   * Size of the cache to maintain in Lucene writer, if specified then it tries to aggregate the
   * unique data till the cache limit and flush to Lucene.
   * It is best suitable for low cardinality dimensions.
   */
  static final String FLUSH_CACHE = "flush_cache";

  /**
   * By default it does not use any cache.
   */
  static final String FLUSH_CACHE_DEFAULT_SIZE = "-1";

  /**
   * when made as true then store the data in blocklet wise in lucene , it means new folder will be
   * created for each blocklet thus it eliminates storing on blockletid in lucene.
   * And also it makes lucene small chuns of data
   */
  static final String SPLIT_BLOCKLET = "split_blocklet";

  /**
   * By default it is false
   */
  static final String SPLIT_BLOCKLET_DEFAULT = "true";
  /**
   * Logger
   */
  final Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

  /**
   * table's index columns
   */
  IndexMeta indexMeta = null;

  /**
   * analyzer for lucene
   */
  Analyzer analyzer = null;

  /**
   * index name
   */
  String dataMapName = null;

  /**
   * table identifier
   */
  AbsoluteTableIdentifier tableIdentifier = null;

  List<CarbonColumn> indexedCarbonColumns = null;

  int flushCacheSize;

  boolean storeBlockletWise;

  public LuceneIndexFactoryBase(CarbonTable carbonTable, DataMapSchema dataMapSchema)
      throws MalformedIndexCommandException {
    super(carbonTable, dataMapSchema);
    Objects.requireNonNull(carbonTable.getAbsoluteTableIdentifier());
    Objects.requireNonNull(dataMapSchema);

    this.tableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    this.dataMapName = dataMapSchema.getDataMapName();

    // validate DataMapSchema and get index columns
    indexedCarbonColumns =  carbonTable.getIndexedColumns(dataMapSchema.getIndexColumns());
    flushCacheSize = validateAndGetWriteCacheSize(dataMapSchema);
    storeBlockletWise = validateAndGetStoreBlockletWise(dataMapSchema);

    // add optimizedOperations
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // optimizedOperations.add(ExpressionType.EQUALS);
    // optimizedOperations.add(ExpressionType.GREATERTHAN);
    // optimizedOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
    // optimizedOperations.add(ExpressionType.LESSTHAN);
    // optimizedOperations.add(ExpressionType.LESSTHAN_EQUALTO);
    // optimizedOperations.add(ExpressionType.NOT);
    optimizedOperations.add(ExpressionType.TEXT_MATCH);
    this.indexMeta = new IndexMeta(indexedCarbonColumns, optimizedOperations);
    // get analyzer
    // TODO: how to get analyzer ?
    analyzer = new StandardAnalyzer();
  }

  public static int validateAndGetWriteCacheSize(DataMapSchema schema) {
    String cacheStr = schema.getProperties().get(FLUSH_CACHE);
    if (cacheStr == null) {
      cacheStr = FLUSH_CACHE_DEFAULT_SIZE;
    }
    int cacheSize;
    try {
      cacheSize = Integer.parseInt(cacheStr);
    } catch (NumberFormatException e) {
      cacheSize = -1;
    }
    return cacheSize;
  }

  public static boolean validateAndGetStoreBlockletWise(DataMapSchema schema) {
    String splitBlockletStr = schema.getProperties().get(SPLIT_BLOCKLET);
    if (splitBlockletStr == null) {
      splitBlockletStr = SPLIT_BLOCKLET_DEFAULT;
    }
    boolean splitBlockletWise;
    try {
      splitBlockletWise = Boolean.parseBoolean(splitBlockletStr);
    } catch (NumberFormatException e) {
      splitBlockletWise = true;
    }
    return splitBlockletWise;
  }

  /**
   * this method will delete the datamap folders during drop datamap
   * @throws MalformedIndexCommandException
   */
  private void deleteDatamap() throws MalformedIndexCommandException {
    SegmentStatusManager ssm = new SegmentStatusManager(tableIdentifier);
    try {
      List<Segment> validSegments =
          ssm.getValidAndInvalidSegments(getCarbonTable().isMV()).getValidSegments();
      for (Segment segment : validSegments) {
        deleteIndexData(segment);
      }
    } catch (IOException | RuntimeException ex) {
      throw new MalformedIndexCommandException(
          "drop datamap failed, failed to delete datamap directory");
    }
  }

  /**
   * Return a new write for this datamap
   */
  @Override
  public IndexWriter createWriter(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    LOGGER.info("lucene data write to " + shardName);
    return new LuceneIndexWriter(getCarbonTable().getTablePath(), dataMapName,
        indexMeta.getIndexedColumns(), segment, shardName, flushCacheSize,
        storeBlockletWise);
  }

  @Override
  public IndexBuilder createBuilder(Segment segment, String shardName,
      SegmentProperties segmentProperties) {
    return new LuceneIndexBuilder(getCarbonTable().getTablePath(), dataMapName,
        segment, shardName, indexMeta.getIndexedColumns(), flushCacheSize, storeBlockletWise);
  }

  /**
   * Get all distributable objects of a segmentId
   */
  @Override
  public List<IndexInputSplit> toDistributable(Segment segment) {
    List<IndexInputSplit> lstDataMapDistribute = new ArrayList<>();
    CarbonFile[] indexDirs =
        getAllIndexDirs(tableIdentifier.getTablePath(), segment.getSegmentNo());
    if (segment.getFilteredIndexShardNames().size() == 0) {
      for (CarbonFile indexDir : indexDirs) {
        IndexInputSplit luceneIndexInputSplit =
            new LuceneIndexInputSplit(tableIdentifier.getTablePath(),
                indexDir.getAbsolutePath());
        luceneIndexInputSplit.setSegment(segment);
        luceneIndexInputSplit.setDataMapSchema(getDataMapSchema());
        lstDataMapDistribute.add(luceneIndexInputSplit);
      }
      return lstDataMapDistribute;
    }
    for (CarbonFile indexDir : indexDirs) {
      // Filter out the tasks which are filtered through CG datamap.
      if (getDataMapLevel() != IndexLevel.FG &&
          !segment.getFilteredIndexShardNames().contains(indexDir.getName())) {
        continue;
      }
      IndexInputSplit luceneIndexInputSplit = new LuceneIndexInputSplit(
          CarbonTablePath.getSegmentPath(tableIdentifier.getTablePath(), segment.getSegmentNo()),
          indexDir.getAbsolutePath());
      luceneIndexInputSplit.setSegment(segment);
      luceneIndexInputSplit.setDataMapSchema(getDataMapSchema());
      lstDataMapDistribute.add(luceneIndexInputSplit);
    }
    return lstDataMapDistribute;
  }

  @Override
  public void fireEvent(Event event) {

  }

  /**
   * Clear all datamaps from memory
   */
  @Override
  public void clear() {

  }

  @Override
  public void deleteIndexData(Segment segment) throws IOException {
    deleteSegmentIndexData(segment.getSegmentNo());
  }

  @Override
  public void deleteSegmentIndexData(String segmentId) throws IOException {
    try {
      String datamapPath = CarbonTablePath
          .getDataMapStorePath(tableIdentifier.getTablePath(), segmentId, dataMapName);
      if (FileFactory.isFileExist(datamapPath)) {
        CarbonFile file = FileFactory.getCarbonFile(datamapPath);
        CarbonUtil.deleteFoldersAndFilesSilent(file);
      }
    } catch (InterruptedException ex) {
      throw new IOException("drop datamap failed, failed to delete datamap directory");
    }
  }

  @Override
  public void deleteIndexData() {
    try {
      deleteDatamap();
    } catch (MalformedIndexCommandException ex) {
      LOGGER.error("failed to delete datamap directory ", ex);
    }
  }

  /**
   * Return metadata of this datamap
   */
  public IndexMeta getMeta() {
    return indexMeta;
  }

  /**
   * returns all the directories of lucene index files for query
   * @param tablePath
   * @param segmentId
   * @return
   */
  private CarbonFile[] getAllIndexDirs(String tablePath, String segmentId) {
    List<CarbonFile> indexDirs = new ArrayList<>();
    List<TableIndex> dataMaps = new ArrayList<>();
    try {
      // there can be multiple lucene datamaps present on a table, so get all datamaps and form
      // the path till the index file directories in all datamaps folders present in each segment
      dataMaps = DataMapStoreManager.getInstance().getAllIndexes(getCarbonTable());
    } catch (IOException ex) {
      LOGGER.error("failed to get datamaps");
    }
    if (dataMaps.size() > 0) {
      for (TableIndex dataMap : dataMaps) {
        if (dataMap.getDataMapSchema().getDataMapName().equals(this.dataMapName)) {
          List<CarbonFile> indexFiles;
          String dmPath = CarbonTablePath.getDataMapStorePath(tablePath, segmentId,
              dataMap.getDataMapSchema().getDataMapName());
          final CarbonFile dirPath = FileFactory.getCarbonFile(dmPath);
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

  /**
   * Further validate whether it is string column and dictionary column.
   * Currently only string and non-dictionary column is supported for Lucene Index
   */
  @Override
  public void validate() throws MalformedIndexCommandException {
    super.validate();
    List<CarbonColumn> indexColumns =
        getCarbonTable().getIndexedColumns(getDataMapSchema().getIndexColumns());

    for (CarbonColumn column : indexColumns) {
      if (column.getDataType() != DataTypes.STRING) {
        throw new MalformedIndexCommandException(String.format(
            "Only String column is supported, column '%s' is %s type. ",
            column.getColName(), column.getDataType()));
      } else if (column.getDataType() == DataTypes.DATE) {
        throw new MalformedIndexCommandException(String.format(
            "Dictionary column is not supported, column '%s' is dictionary column",
            column.getColName()));
      }
    }
  }
}
