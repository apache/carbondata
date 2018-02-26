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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.solr.store.hdfs.HdfsDirectory;

@InterfaceAudience.Internal
public class LuceneCoarseGrainDataMap extends CoarseGrainDataMap {

  /**
   * log information
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LuceneCoarseGrainDataMap.class.getName());

  public static final int BLOCKID_ID = 0;

  public static final int BLOCKLETID_ID = 1;

  public static final int PAGEID_ID = 2;

  public static final int ROWID_ID = 3;
  /**
   * searcher object for this datamap
   */
  private IndexSearcher indexSearcher = null;

  /**
   * datamap name
   */
  private String dataMapName = null;

  /**
   * segment id
   */
  private String segmentId = null;

  /**
   * talbe identifier
   */
  private AbsoluteTableIdentifier tableIdentifier = null;

  /**
   * default max values to return
   */
  private static int MAX_RESULT_NUMBER = 100;

  /**
   * analyzer for lucene index
   */
  private Analyzer analyzer = null;

  LuceneCoarseGrainDataMap(AbsoluteTableIdentifier tableIdentifier, String dataMapName,
      String segmentId, Analyzer analyzer) {
    this.analyzer = analyzer;
    this.dataMapName = dataMapName;
    this.segmentId = segmentId;
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * It is called to load the data map to memory or to initialize it.
   */
  @Override
  public void init(DataMapModel dataMapModel) throws MemoryException, IOException {
    // get this path from file path
    Path indexPath = FileFactory.getPath(dataMapModel.getFilePath());

    LOGGER.info("Lucene index read path " + indexPath.toString());

    // get file system , use hdfs file system , realized in solr project
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // check this path valid
    if (!fs.exists(indexPath)) {
      String errorMessage = String.format("index directory %s not exists.", indexPath);
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    if (!fs.isDirectory(indexPath)) {
      String errorMessage = String.format("error index path %s, must be directory", indexPath);
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    // open this index path , use HDFS default configuration
    Directory indexDir = new HdfsDirectory(indexPath, FileFactory.getConfiguration());

    IndexReader indexReader = DirectoryReader.open(indexDir);
    if (indexReader == null) {
      throw new RuntimeException("failed to create index reader object");
    }

    // create a index searcher object
    indexSearcher = new IndexSearcher(indexReader);
  }

  /**
   * Prune the datamap with filter expression. It returns the list of
   * blocklets where these filters can exist.
   */
  @Override
  public List<Blocklet> prune(FilterResolverIntf filterExp, SegmentProperties segmentProperties,
      List<String> partitions) throws IOException {

    // convert filter expr into lucene list query
    List<String> fields = new ArrayList<String>();

    // only for test , query all data
    String strQuery = "*:*";

    String[] sFields = new String[fields.size()];
    fields.toArray(sFields);

    // get analyzer
    if (analyzer == null) {
      analyzer = new StandardAnalyzer();
    }

    // use MultiFieldQueryParser to parser query
    QueryParser queryParser = new MultiFieldQueryParser(sFields, analyzer);
    Query query;
    try {
      query = queryParser.parse(strQuery);
    } catch (ParseException e) {
      String errorMessage = String
          .format("failed to filter block with query %s, detail is %s", strQuery, e.getMessage());
      LOGGER.error(errorMessage);
      return null;
    }

    // execute index search
    TopDocs result;
    try {
      result = indexSearcher.search(query, MAX_RESULT_NUMBER);
    } catch (IOException e) {
      String errorMessage =
          String.format("failed to search lucene data, detail is %s", e.getMessage());
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    // temporary data, delete duplicated data
    // Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
    Map<String, Set<Number>> mapBlocks = new HashMap<String, Set<Number>>();

    for (ScoreDoc scoreDoc : result.scoreDocs) {
      // get a document
      Document doc = indexSearcher.doc(scoreDoc.doc);

      // get all fields
      List<IndexableField> fieldsInDoc = doc.getFields();

      // get this block id Map<BlockId, Set<BlockletId>>>>
      String blockId = fieldsInDoc.get(BLOCKID_ID).stringValue();
      Set<Number> setBlocklets = mapBlocks.get(blockId);
      if (setBlocklets == null) {
        setBlocklets = new HashSet<Number>();
        mapBlocks.put(blockId, setBlocklets);
      }

      // get the blocklet id Set<BlockletId>
      Number blockletId = fieldsInDoc.get(BLOCKLETID_ID).numericValue();
      if (!setBlocklets.contains(blockletId.intValue())) {
        setBlocklets.add(blockletId.intValue());
      }
    }

    // result blocklets
    List<Blocklet> blocklets = new ArrayList<Blocklet>();

    // transform all blocks into result type blocklets Map<BlockId, Set<BlockletId>>
    for (Map.Entry<String, Set<Number>> mapBlock : mapBlocks.entrySet()) {
      String blockId = mapBlock.getKey();
      Set<Number> setBlocklets = mapBlock.getValue();

      // for blocklets in this block Set<BlockletId>
      for (Number blockletId : setBlocklets) {

        // add a CoarseGrainBlocklet
        blocklets.add(new Blocklet(blockId, blockletId.toString()));
      }
    }

    return blocklets;
  }

  @Override
  public boolean isScanRequired(FilterResolverIntf filterExp) {
    return true;
  }

  /**
   * Clear complete index table and release memory.
   */
  @Override
  public void clear() {

  }
}
