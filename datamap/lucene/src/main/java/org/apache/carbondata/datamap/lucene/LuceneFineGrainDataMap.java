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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainBlocklet;
import org.apache.carbondata.core.datamap.dev.fgdatamap.FineGrainDataMap;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.MatchExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
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
import org.apache.lucene.util.BytesRef;
import org.apache.solr.store.hdfs.HdfsDirectory;

@InterfaceAudience.Internal
public class LuceneFineGrainDataMap extends FineGrainDataMap {

  /**
   * log information
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LuceneFineGrainDataMap.class.getName());

  /**
   * search limit will help in deciding the size of priority queue which is used by lucene to store
   * the documents in heap. By default it is 100 means in one search max of 10 documents can be
   * stored in heap by lucene. This way it will help in reducing the GC.
   * Note: If it is removed or it's value is increased it will lead to almost 90%
   * of the query time in GC in worst case scenarios if it's value is increased beyond a limit
   */
  private static final int SEARCH_LIMIT = 100;

  /**
   * searcher object for this datamap
   */
  private Map<String, IndexSearcher> indexSearcherMap = null;

  /**
   * analyzer for lucene index
   */
  private Analyzer analyzer;

  private String filePath;

  private int writeCacheSize;

  private boolean storeBlockletWise;

  private IndexReader indexReader;

  LuceneFineGrainDataMap(Analyzer analyzer, DataMapSchema schema) {
    this.analyzer = analyzer;
    writeCacheSize = LuceneDataMapFactoryBase.validateAndGetWriteCacheSize(schema);
    storeBlockletWise = LuceneDataMapFactoryBase.validateAndGetStoreBlockletWise(schema);
  }

  /**
   * It is called to load the data map to memory or to initialize it.
   */
  public void init(DataMapModel dataMapModel) throws IOException {
    long startTime = System.currentTimeMillis();
    // get this path from file path
    Path indexPath = FileFactory.getPath(dataMapModel.getFilePath());

    LOGGER.info("Lucene index read path " + indexPath.toString());

    this.filePath = indexPath.getName();

    this.indexSearcherMap = new HashMap<>();

    // get file system , use hdfs file system , realized in solr project
    CarbonFile indexFilePath = FileFactory.getCarbonFile(indexPath.toString());

    // check this path valid
    if (!indexFilePath.exists()) {
      String errorMessage = String.format("index directory %s not exists.", indexPath);
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    if (!indexFilePath.isDirectory()) {
      String errorMessage = String.format("error index path %s, must be directory", indexPath);
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    if (storeBlockletWise) {
      CarbonFile[] blockletDirs = indexFilePath.listFiles();
      for (CarbonFile blockletDir : blockletDirs) {
        IndexSearcher indexSearcher = createIndexSearcher(new Path(blockletDir.getAbsolutePath()));
        indexSearcherMap.put(blockletDir.getName(), indexSearcher);
      }

    } else {
      IndexSearcher indexSearcher = createIndexSearcher(indexPath);
      indexSearcherMap.put("-1", indexSearcher);

    }
    LOGGER.info(
        "Time taken to initialize lucene searcher: " + (System.currentTimeMillis() - startTime));
  }

  private IndexSearcher createIndexSearcher(Path indexPath) throws IOException {
    // open this index path , use HDFS default configuration
    Directory indexDir = new HdfsDirectory(indexPath, FileFactory.getConfiguration());

    this.indexReader = DirectoryReader.open(indexDir);
    if (indexReader == null) {
      throw new RuntimeException("failed to create index reader object");
    }

    // create a index searcher object
    return new IndexSearcher(indexReader);
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
   * Return Maximum records
   * @return
   */
  private int getMaxDoc(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.TEXT_MATCH) {
      int maxDoc = ((MatchExpression) expression).getMaxDoc();
      if (maxDoc < 0) {
        maxDoc = Integer.MAX_VALUE;
      }
      return maxDoc;
    }

    for (Expression child : expression.getChildren()) {
      return getMaxDoc(child);
    }
    return Integer.MAX_VALUE;
  }

  /**
   * Prune the datamap with filter expression. It returns the list of
   * blocklets where these filters can exist.
   */
  @Override
  public List<FineGrainBlocklet> prune(FilterResolverIntf filterExp,
      SegmentProperties segmentProperties, List<PartitionSpec> partitions) throws IOException {

    // convert filter expr into lucene list query
    List<String> fields = new ArrayList<String>();

    // only for test , query all data
    String strQuery = getQueryString(filterExp.getFilterExpression());
    int maxDocs;
    try {
      maxDocs = getMaxDoc(filterExp.getFilterExpression());
    } catch (NumberFormatException e) {
      maxDocs = Integer.MAX_VALUE;
    }

    if (null == strQuery) {
      return null;
    }

    String[] sFields = new String[fields.size()];
    fields.toArray(sFields);

    // get analyzer
    if (analyzer == null) {
      analyzer = new StandardAnalyzer();
    }

    // use MultiFieldQueryParser to parser query
    QueryParser queryParser = new MultiFieldQueryParser(sFields, analyzer);
    queryParser.setAllowLeadingWildcard(true);
    Query query;
    try {
      query = queryParser.parse(strQuery);
    } catch (ParseException e) {
      String errorMessage = String.format(
          "failed to filter block with query %s, detail is %s", strQuery, e.getMessage());
      LOGGER.error(errorMessage, e);
      return null;
    }
    // temporary data, delete duplicated data
    // Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
    Map<String, Map<Integer, List<Short>>> mapBlocks = new HashMap<>();

    long luceneSearchStartTime = System.currentTimeMillis();
    for (Map.Entry<String, IndexSearcher> searcherEntry : indexSearcherMap.entrySet()) {
      IndexSearcher indexSearcher = searcherEntry.getValue();
      // take the min of total documents available in the reader and limit if set by the user
      maxDocs = Math.min(maxDocs, indexSearcher.getIndexReader().maxDoc());
      // execute index search
      TopDocs result = null;
      // the number of documents to be queried in one search. It will always be minimum of
      // search result and maxDocs
      int numberOfDocumentsToBeQueried = 0;
      // counter for maintaining the total number of documents finished querying
      int documentHitCounter = 0;
      try {
        numberOfDocumentsToBeQueried = Math.min(maxDocs, SEARCH_LIMIT);
        result = indexSearcher.search(query, numberOfDocumentsToBeQueried);
        documentHitCounter += numberOfDocumentsToBeQueried;
      } catch (IOException e) {
        String errorMessage =
            String.format("failed to search lucene data, detail is %s", e.getMessage());
        LOGGER.error(errorMessage, e);
        throw new IOException(errorMessage, e);
      }

      ByteBuffer intBuffer = ByteBuffer.allocate(4);
      // last scoreDoc in a result to be used in searchAfter API
      ScoreDoc lastScoreDoc = null;
      while (true) {
        for (ScoreDoc scoreDoc : result.scoreDocs) {
          // get a document
          Document doc = indexSearcher.doc(scoreDoc.doc);
          // get all fields
          List<IndexableField> fieldsInDoc = doc.getFields();
          if (writeCacheSize > 0) {
            // It fills rowids to the map, its value is combined with multiple rows.
            fillMapForCombineRows(intBuffer, mapBlocks, fieldsInDoc, searcherEntry.getKey());
          } else {
            // Fill rowids to the map
            fillMap(intBuffer, mapBlocks, fieldsInDoc, searcherEntry.getKey());
          }
          lastScoreDoc = scoreDoc;
        }
        // result will have the total number of hits therefore we always need to query on the
        // left over documents
        int remainingHits = result.totalHits - documentHitCounter;
        // break the loop if count reaches maxDocs to be searched or remaining hits become <=0
        if (remainingHits <= 0 || documentHitCounter >= maxDocs) {
          break;
        }
        numberOfDocumentsToBeQueried = Math.min(remainingHits, SEARCH_LIMIT);
        result = indexSearcher.searchAfter(lastScoreDoc, query, numberOfDocumentsToBeQueried);
        documentHitCounter += numberOfDocumentsToBeQueried;
      }
    }
    LOGGER.info(
        "Time taken for lucene search: " + (System.currentTimeMillis() - luceneSearchStartTime)
            + " ms");

    // result blocklets
    List<FineGrainBlocklet> blocklets = new ArrayList<>();

    // transform all blocks into result type blocklets
    // Map<BlockId, Map<BlockletId, Map<PageId, Set<RowId>>>>
    for (Map.Entry<String, Map<Integer, List<Short>>> mapBlocklet :
        mapBlocks.entrySet()) {
      String blockletId = mapBlocklet.getKey();
      Map<Integer, List<Short>> mapPageIds = mapBlocklet.getValue();
      List<FineGrainBlocklet.Page> pages = new ArrayList<FineGrainBlocklet.Page>();

      // for pages in this blocklet Map<PageId, Set<RowId>>>
      for (Map.Entry<Integer, List<Short>> mapPageId : mapPageIds.entrySet()) {
        // construct array rowid
        int[] rowIds = new int[mapPageId.getValue().size()];
        int i = 0;
        // for rowids in this page Set<RowId>
        for (Short rowid : mapPageId.getValue()) {
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
      blocklets.add(new FineGrainBlocklet(filePath, blockletId, pages));
    }

    return blocklets;
  }

  /**
   * It fills the rowids to the map, its value is combined with multiple rowids as we store group
   * rows and combine as per there uniqueness.
   */
  private void fillMapForCombineRows(ByteBuffer intBuffer,
      Map<String, Map<Integer, List<Short>>> mapBlocks, List<IndexableField> fieldsInDoc,
      String blockletId) {
    for (int i = 0; i < fieldsInDoc.size(); i++) {
      BytesRef bytesRef = fieldsInDoc.get(i).binaryValue();
      ByteBuffer buffer = ByteBuffer.wrap(bytesRef.bytes);

      int pageId;
      if (storeBlockletWise) {
        // If we store as per blockletwise then just read pageid only we don't store blockletid
        pageId = buffer.getShort();
      } else {
        int combineKey = buffer.getInt();
        intBuffer.clear();
        intBuffer.putInt(combineKey);
        intBuffer.rewind();
        blockletId = String.valueOf(intBuffer.getShort());
        pageId = intBuffer.getShort();
      }

      Map<Integer, List<Short>> mapPageIds = mapBlocks.get(blockletId);
      if (mapPageIds == null) {
        mapPageIds = new HashMap<>();
        mapBlocks.put(blockletId, mapPageIds);
      }
      List<Short> setRowId = mapPageIds.get(pageId);
      if (setRowId == null) {
        setRowId = new ArrayList<>();
        mapPageIds.put(pageId, setRowId);
      }

      while (buffer.hasRemaining()) {
        setRowId.add(buffer.getShort());
      }
    }
  }

  /**
   * Fill the map with rowids from documents
   */
  private void fillMap(ByteBuffer intBuffer, Map<String, Map<Integer, List<Short>>> mapBlocks,
      List<IndexableField> fieldsInDoc, String blockletId) {
    int combineKey = fieldsInDoc.get(0).numericValue().intValue();
    intBuffer.clear();
    intBuffer.putInt(combineKey);
    intBuffer.rewind();
    short rowId;
    int pageId;
    if (storeBlockletWise) {
      // If we store as per blockletwise then just read pageid and rowid
      // only we don't store blockletid
      pageId = intBuffer.getShort();
      rowId = intBuffer.getShort();
    } else {
      blockletId = String.valueOf(intBuffer.getShort());
      pageId = intBuffer.getShort();
      rowId = fieldsInDoc.get(1).numericValue().shortValue();
    }
    Map<Integer, List<Short>> mapPageIds = mapBlocks.get(blockletId);
    if (mapPageIds == null) {
      mapPageIds = new HashMap<>();
      mapBlocks.put(blockletId, mapPageIds);
    }
    List<Short> setRowId = mapPageIds.get(pageId);
    if (setRowId == null) {
      setRowId = new ArrayList<>();
      mapPageIds.put(pageId, setRowId);
    }
    setRowId.add(rowId);
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

  @Override
  public void finish() {
    if (null != indexReader) {
      try {
        int referenceCount = indexReader.getRefCount();
        if (referenceCount > 0) {
          indexReader.decRef();
          if (null != indexSearcherMap) {
            indexSearcherMap.clear();
          }
        }
      } catch (IOException e) {
        LOGGER.error("Ignoring the exception, Error while closing the lucene index reader", e);
      }
    }
  }
}
