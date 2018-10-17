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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapBuilder;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.datamap.lucene.LuceneDataMapWriter.addData;
import static org.apache.carbondata.datamap.lucene.LuceneDataMapWriter.addToCache;
import static org.apache.carbondata.datamap.lucene.LuceneDataMapWriter.flushCache;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.roaringbitmap.RoaringBitmap;

public class LuceneDataMapBuilder implements DataMapBuilder {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LuceneDataMapWriter.class.getName());

  private String dataMapPath;

  private List<CarbonColumn> indexColumns;

  private int columnsCount;

  private IndexWriter indexWriter = null;

  private Analyzer analyzer = null;

  private int writeCacheSize;

  private Map<LuceneDataMapWriter.LuceneColumnKeys, Map<Integer, RoaringBitmap>> cache =
      new HashMap<>();

  private ByteBuffer intBuffer = ByteBuffer.allocate(4);

  private boolean storeBlockletWise;

  private int currentBlockletId = -1;

  LuceneDataMapBuilder(String tablePath, String dataMapName, Segment segment, String shardName,
      List<CarbonColumn> indexColumns, int writeCacheSize, boolean storeBlockletWise) {
    this.dataMapPath = CarbonTablePath
        .getDataMapStorePathOnShardName(tablePath, segment.getSegmentNo(), dataMapName, shardName);
    this.indexColumns = indexColumns;
    this.columnsCount = indexColumns.size();
    this.writeCacheSize = writeCacheSize;
    this.storeBlockletWise = storeBlockletWise;
  }

  @Override
  public void initialize() throws IOException {
    if (!storeBlockletWise) {
      // get index path, put index data into segment's path
      indexWriter = createIndexWriter(dataMapPath);
    }
  }

  private IndexWriter createIndexWriter(String dataMapPath) throws IOException {
    Path indexPath = FileFactory.getPath(dataMapPath);
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // if index path exists, should delete it because we are
    // rebuilding the whole datamap for all segments
    if (fs.exists(indexPath)) {
      fs.delete(indexPath, true);
    }
    if (!fs.mkdirs(indexPath)) {
      LOGGER.error("Failed to create directory " + indexPath);
    }

    if (null == analyzer) {
      analyzer = new StandardAnalyzer();
    }

    // create a index writer
    Directory indexDir = new HdfsDirectory(indexPath, FileFactory.getConfiguration());

    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
    if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE,
            CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE_DEFAULT)
        .equalsIgnoreCase(CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE_DEFAULT)) {
      indexWriterConfig.setCodec(new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_SPEED));
    } else {
      indexWriterConfig
          .setCodec(new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION));
    }

    return new IndexWriter(indexDir, new IndexWriterConfig(analyzer));
  }

  @Override
  public void addRow(int blockletId, int pageId, int rowId, Object[] values)
      throws IOException {
    if (storeBlockletWise) {
      if (currentBlockletId != blockletId) {
        close();
        indexWriter = createIndexWriter(dataMapPath + File.separator + blockletId);
        currentBlockletId = blockletId;
      }
    }
    // add other fields
    LuceneDataMapWriter.LuceneColumnKeys columns =
        new LuceneDataMapWriter.LuceneColumnKeys(columnsCount);
    for (int colIdx = 0; colIdx < columnsCount; colIdx++) {
      columns.getColValues()[colIdx] = values[colIdx];
    }
    if (writeCacheSize > 0) {
      addToCache(columns, rowId, pageId, blockletId, cache, intBuffer, storeBlockletWise);
      flushCacheIfPossible();
    } else {
      addData(columns, rowId, pageId, blockletId, intBuffer, indexWriter, indexColumns,
          storeBlockletWise);
    }

  }

  private void flushCacheIfPossible() throws IOException {
    if (cache.size() >= writeCacheSize) {
      flushCache(cache, indexColumns, indexWriter, storeBlockletWise);
    }
  }

  @Override
  public void finish() throws IOException {
    flushCache(cache, indexColumns, indexWriter, storeBlockletWise);
  }

  @Override
  public void close() throws IOException {
    if (indexWriter != null) {
      indexWriter.close();
    }
  }

  @Override
  public boolean isIndexForCarbonRawBytes() {
    return false;
  }
}
