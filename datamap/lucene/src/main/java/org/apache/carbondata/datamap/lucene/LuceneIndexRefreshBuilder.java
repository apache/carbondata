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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntRangeField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.store.hdfs.HdfsDirectory;

public class LuceneIndexRefreshBuilder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LuceneDataMapWriter.class.getName());

  private String strIndexPath;

  private String[] indexColumns;
  private DataType[] dataTypes;

  private int columnsCount;

  private IndexWriter indexWriter = null;

  private IndexWriter pageIndexWriter = null;

  private Analyzer analyzer = null;

  public LuceneIndexRefreshBuilder(String strIndexPath, String[] indexColumns,
      DataType[] dataTypes) {
    this.strIndexPath = strIndexPath;
    this.indexColumns = indexColumns;
    this.columnsCount = indexColumns.length;
    this.dataTypes = dataTypes;
  }

  public void initialize() throws IOException {
    // get index path, put index data into segment's path
    Path indexPath = FileFactory.getPath(strIndexPath);
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // if index path not exists, create it
    if (!fs.exists(indexPath)) {
      fs.mkdirs(indexPath);
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

    indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(analyzer));
  }

  private IndexWriter createPageIndexWriter() throws IOException {
    // save index data into ram, write into disk after one page finished
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter ramIndexWriter = new IndexWriter(ramDir, new IndexWriterConfig(analyzer));

    return ramIndexWriter;
  }

  private void addPageIndex(IndexWriter pageIndexWriter) throws IOException {

    Directory directory = pageIndexWriter.getDirectory();

    // close ram writer
    pageIndexWriter.close();

    // add ram index data into disk
    indexWriter.addIndexes(new Directory[] { directory });

    // delete this ram data
    directory.close();
  }

  public void addDocument(Object[] values) throws IOException {

    if (values.length != indexColumns.length + 3) {
      throw new IOException("The column number (" + values.length + ") of the row  is incorrect.");
    }
    int rowId = (int) values[indexColumns.length + 2];
    if (rowId == 0) {
      if (pageIndexWriter != null) {
        addPageIndex(pageIndexWriter);
      }
      pageIndexWriter = createPageIndexWriter();
    }

    // create a new document
    Document doc = new Document();

    // add blocklet Id
    doc.add(new IntPoint(LuceneDataMapWriter.BLOCKLETID_NAME,
        new int[] { (int) values[columnsCount] }));
    doc.add(new StoredField(LuceneDataMapWriter.BLOCKLETID_NAME, (int) values[columnsCount]));

    // add page id
    doc.add(new IntPoint(LuceneDataMapWriter.PAGEID_NAME,
        new int[] { (int) values[columnsCount + 1] }));
    doc.add(new StoredField(LuceneDataMapWriter.PAGEID_NAME, (int) values[columnsCount + 1]));

    // add row id
    doc.add(new IntPoint(LuceneDataMapWriter.ROWID_NAME, new int[] { rowId }));
    doc.add(new StoredField(LuceneDataMapWriter.ROWID_NAME, rowId));

    // add other fields
    for (int colIdx = 0; colIdx < columnsCount; colIdx++) {
      addField(doc, indexColumns[colIdx], dataTypes[colIdx], values[colIdx]);
    }

    pageIndexWriter.addDocument(doc);
  }

  private boolean addField(Document doc, String fieldName, DataType type, Object value) {
    if (type == DataTypes.BYTE) {
      // byte type , use int range to deal with byte, lucene has no byte type
      IntRangeField field =
          new IntRangeField(fieldName, new int[] { Byte.MIN_VALUE }, new int[] { Byte.MAX_VALUE });
      field.setIntValue((int) value);
      doc.add(field);
    } else if (type == DataTypes.SHORT) {
      // short type , use int range to deal with short type, lucene has no short type
      IntRangeField field = new IntRangeField(fieldName, new int[] { Short.MIN_VALUE },
          new int[] { Short.MAX_VALUE });
      field.setShortValue((short) value);
      doc.add(field);
    } else if (type == DataTypes.INT) {
      // int type , use int point to deal with int type
      doc.add(new IntPoint(fieldName, new int[] { (int) value }));
    } else if (type == DataTypes.LONG) {
      // long type , use long point to deal with long type
      doc.add(new LongPoint(fieldName, new long[] { (long) value }));
    } else if (type == DataTypes.FLOAT) {
      doc.add(new FloatPoint(fieldName, new float[] { (float) value }));
    } else if (type == DataTypes.DOUBLE) {
      doc.add(new DoublePoint(fieldName, new double[] { (double) value }));
    } else if (type == DataTypes.STRING) {
      doc.add(new TextField(fieldName, (String) value, Field.Store.NO));
    } else if (type == DataTypes.DATE) {
      // TODO: how to get data value
    } else if (type == DataTypes.TIMESTAMP) {
      // TODO: how to get
    } else if (type == DataTypes.BOOLEAN) {
      IntRangeField field = new IntRangeField(fieldName, new int[] { 0 }, new int[] { 1 });
      field.setIntValue((boolean) value ? 1 : 0);
      doc.add(field);
    } else {
      LOGGER.error("unsupport data type " + type);
      throw new RuntimeException("unsupported data type " + type);
    }
    return true;
  }

  public void finish() throws IOException {
    if (indexWriter != null && pageIndexWriter != null) {
      addPageIndex(pageIndexWriter);
    }
  }

  public void close() throws IOException {
    if (indexWriter != null) {
      indexWriter.close();
    }
  }

}
