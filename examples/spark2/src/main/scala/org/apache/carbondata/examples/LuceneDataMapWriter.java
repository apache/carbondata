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
package org.apache.carbondata.examples;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import static org.apache.lucene.index.IndexOptions.DOCS_AND_FREQS;

public class LuceneDataMapWriter implements DataMapWriter {

  /**
   * The events for lucene index are as following.
   * a) on Block Start - Initialize the Index Writer and the Directory.
   * b) On Block End - Close the index writer.
   * c) On Blocklet Start - Get a new
   * d) On Blocklet End - Do Nothing
   * e) On Page Add - Get a new document for each page And add the couments to the writer.
   */

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(TableInfo.class.getName());

  private IndexWriter indexWriter;

  private String luceneindexFilePath;

  private Analyzer analyzer;

  private String directoryPath;

  private String filePath;

  private IndexWriterConfig config;

  @Override public void onBlockStart(String blockId, String filePath) {
    try {
      this.filePath = filePath;
      directoryPath = filePath.substring(0, filePath.lastIndexOf(File.separator) + 1);
      luceneindexFilePath = constructIndexFullPath(blockId, directoryPath);
      analyzer = new WhitespaceAnalyzer();
      FSDirectory index = FSDirectory.open(Paths.get(luceneindexFilePath));
      config = new IndexWriterConfig(analyzer);
      indexWriter = new IndexWriter(index, config);
    } catch (IOException ex) {
      LOGGER.audit("Error while creating the index");
    }
  }

  @Override public void onBlockEnd(String blockId, List<BlockIndexInfo> blockIndexInfoList) {
    try {
      indexWriter.commit();
      indexWriter.close();
    } catch (IOException ex) {
      LOGGER.audit("Error while writing the index");
    }
  }

  @Override public void onBlockletStart(int blockletId) {
  }

  @Override public void onBlockletEnd(int blockletId) {

  }

  @Override public void onPageAdded(int blockletId, int pageId, ColumnPage[] pages) {
    constructLuceneIndex(blockletId, pages);
  }

  /**
   * Construct Lucene Index.
   * @param blockletId
   * @param pageId
   * @param pages
   */
  public void constructLuceneIndex(int blockletId, ColumnPage[] pages) {
    // Construct Lucene Index.
    Document doc = new Document();
    FieldType textFieldType = new FieldType();
    textFieldType.setStored(true);
    textFieldType.setTokenized(false);
    textFieldType.setIndexOptions(DOCS_AND_FREQS);
    byte[] value = new byte[pages[0].getBytes(0).length - 2];
    Charset charset = Charset.forName("UTF-8");

    for (int rowIndex = 0; rowIndex < pages[0].getPageSize(); rowIndex++) {
      System.arraycopy(pages[0].getBytes(rowIndex), 2, value, 0, value.length);
      doc.add(new Field("BlockletId", Integer.toString(blockletId), textFieldType));
      doc.add(new Field("BlockletPath", filePath, textFieldType));
      doc.add(new Field("Content", new String(value, charset), textFieldType));
      try {
        indexWriter.addDocument(doc);
      } catch (IOException ex) {
        LOGGER.audit("Error while adding values to the index");
      }
    }
  }

  /**
   * construct the index file from the blockID.
   *
   * @param blockId
   * @param directoryPath
   * @return
   */
  private String constructIndexFullPath(String blockId, String directoryPath) {
    String sub1 = blockId.substring(blockId.indexOf("_") - 1, blockId.indexOf("."));
    return directoryPath + sub1 + ".luceneIndex";
  }
}