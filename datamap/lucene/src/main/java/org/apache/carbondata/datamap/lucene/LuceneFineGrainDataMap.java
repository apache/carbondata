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
import java.util.*;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
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
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.solr.store.hdfs.HdfsDirectory;

@InterfaceAudience.Internal
public class LuceneFineGrainDataMap extends FineGrainDataMap {

  private static final int BLOCKID_ID = 0;

  private static final int BLOCKLETID_ID = 1;

  private static final int PAGEID_ID = 2;

  private static final int ROWID_ID = 3;

  /**
   * log information
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LuceneFineGrainDataMap.class.getName());

  /**
   * searcher object for this datamap
   */
  private IndexSearcher indexSearcher = null;

  /**
   * default max values to return
   */
  private static int MAX_RESULT_NUMBER = 100;

  /**
   * analyzer for lucene index
   */
  private Analyzer analyzer;

  LuceneFineGrainDataMap(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  /**
   * It is called to load the data map to memory or to initialize it.
   */
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
   * Return the query string in the first TEXT_MATCH expression in the expression tree
   */
  private String getQueryString(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.TEXT_MATCH) {
      return expression.getString();
    }

    for (Expression child : expression.getChildren()) {
      String queryString = getQueryString(child);
      if (queryString != null) {
        return queryString;
      }
    }
    return null;
  }

  /**
   * Prune the datamap with filter expression. It returns the list of
   * blocklets where these filters can exist.
   */
  @Override
  public List<FineGrainBlocklet> prune(FilterResolverIntf filterExp,
      SegmentProperties segmentProperties, List<String> partitions) throws IOException {

    // convert filter expr into lucene list query
    List<String> fields = new ArrayList<String>();

    // only for test , query all data
    String strQuery = getQueryString(filterExp.getFilterExpression());

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
      String errorMessage = String.format(
          "failed to filter block with query %s, detail is %s", strQuery, e.getMessage());
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
    Map<String, Map<String, Map<Integer, Set<Integer>>>> mapBlocks = new HashMap<>();

    for (ScoreDoc scoreDoc : result.scoreDocs) {
      // get a document
      Document doc = indexSearcher.doc(scoreDoc.doc);

      // get all fields
      List<IndexableField> fieldsInDoc = doc.getFields();

      // get this block id Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
      String blockId = fieldsInDoc.get(BLOCKID_ID).stringValue();
      Map<String, Map<Integer, Set<Integer>>> mapBlocklets = mapBlocks.get(blockId);
      if (mapBlocklets == null) {
        mapBlocklets = new HashMap<>();
        mapBlocks.put(blockId, mapBlocklets);
      }

      // get the blocklet id Map<BlockletId, Map<PageId, Set<RowId>>>
      String blockletId = fieldsInDoc.get(BLOCKLETID_ID).stringValue();
      Map<Integer, Set<Integer>> mapPageIds = mapBlocklets.get(blockletId);
      if (mapPageIds == null) {
        mapPageIds = new HashMap<>();
        mapBlocklets.put(blockletId, mapPageIds);
      }

      // get the page id Map<PageId, Set<RowId>>
      Number pageId = fieldsInDoc.get(PAGEID_ID).numericValue();
      Set<Integer> setRowId = mapPageIds.get(pageId.intValue());
      if (setRowId == null) {
        setRowId = new HashSet<>();
        mapPageIds.put(pageId.intValue(), setRowId);
      }

      // get the row id Set<RowId>
      Number rowId = fieldsInDoc.get(ROWID_ID).numericValue();
      setRowId.add(rowId.intValue());
    }

    // result blocklets
    List<FineGrainBlocklet> blocklets = new ArrayList<>();

    // transform all blocks into result type blocklets
    // Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
    for (Map.Entry<String, Map<String, Map<Integer, Set<Integer>>>> mapBlock :
        mapBlocks.entrySet()) {
      String blockId = mapBlock.getKey();
      Map<String, Map<Integer, Set<Integer>>> mapBlocklets = mapBlock.getValue();
      // for blocklets in this block Map<BlockletId, Map<PageId, Set<RowId>>>
      for (Map.Entry<String, Map<Integer, Set<Integer>>> mapBlocklet : mapBlocklets.entrySet()) {
        String blockletId = mapBlocklet.getKey();
        Map<Integer, Set<Integer>> mapPageIds = mapBlocklet.getValue();
        List<FineGrainBlocklet.Page> pages = new ArrayList<FineGrainBlocklet.Page>();

        // for pages in this blocklet Map<PageId, Set<RowId>>>
        for (Map.Entry<Integer, Set<Integer>> mapPageId : mapPageIds.entrySet()) {
          // construct array rowid
          int[] rowIds = new int[mapPageId.getValue().size()];
          int i = 0;
          // for rowids in this page Set<RowId>
          for (Integer rowid : mapPageId.getValue()) {
            rowIds[i++] = rowid;
          }
          // construct one page
          FineGrainBlocklet.Page page = new FineGrainBlocklet.Page();
          page.setPageId(mapPageId.getKey());
          page.setRowId(rowIds);

          // add this page into list pages
          pages.add(page);
        }

        // add a FineGrainBlocklet
        blocklets.add(new FineGrainBlocklet(blockId, blockletId, pages));
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
