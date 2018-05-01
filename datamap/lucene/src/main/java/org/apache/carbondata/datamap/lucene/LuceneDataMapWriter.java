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
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;
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

/**
 * Implementation to write lucene index while loading
 */
@InterfaceAudience.Internal
public class LuceneDataMapWriter extends DataMapWriter {
  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LuceneDataMapWriter.class.getName());

  /**
   * index writer
   */
  private IndexWriter indexWriter = null;

  private Analyzer analyzer = null;

  private boolean isFineGrain = true;

  public static final String BLOCKLETID_NAME = "blockletId";

  private String indexShardName = null;

  public static final String PAGEID_NAME = "pageId";

  public static final String ROWID_NAME = "rowId";

  LuceneDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, boolean isFineGrain) {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    this.isFineGrain = isFineGrain;
  }

  /**
   * Start of new block notification.
   */
  public void onBlockStart(String blockId) throws IOException {
    if (indexWriter != null) {
      return;
    }
    // get index path, put index data into segment's path
    Path indexPath = FileFactory.getPath(dataMapPath);
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // if index path not exists, create it
    if (!fs.exists(indexPath)) {
      if (!fs.mkdirs(indexPath)) {
        throw new IOException("Failed to create directory " + dataMapPath);
      }
    }

    if (null == analyzer) {
      analyzer = new StandardAnalyzer();
    }

    // the indexWriter closes the FileSystem on closing the writer, so for a new configuration
    // and disable the cache for the index writer, it will be closed on closing the writer
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl.disable.cache", "true");

    // create a index writer
    Directory indexDir = new HdfsDirectory(indexPath, conf);

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

  /**
   * End of block notification
   */
  public void onBlockEnd(String blockId) throws IOException {

  }

  private RAMDirectory ramDir;
  private IndexWriter ramIndexWriter;

  /**
   * Start of new blocklet notification.
   */
  public void onBlockletStart(int blockletId) throws IOException {
    // save index data into ram, write into disk after one page finished
    ramDir = new RAMDirectory();
    ramIndexWriter = new IndexWriter(ramDir, new IndexWriterConfig(analyzer));
  }

  /**
   * End of blocklet notification
   */
  public void onBlockletEnd(int blockletId) throws IOException {
    // close ram writer
    ramIndexWriter.close();

    // add ram index data into disk
    indexWriter.addIndexes(ramDir);

    // delete this ram data
    ramDir.close();
  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  public void addRow(int blockletId, int pageId, int rowId, CarbonRow row) throws IOException {

    // create a new document
    Document doc = new Document();
    // add blocklet Id
    doc.add(new IntPoint(BLOCKLETID_NAME, blockletId));
    doc.add(new StoredField(BLOCKLETID_NAME, blockletId));
    //doc.add(new NumericDocValuesField(BLOCKLETID_NAME,blockletId));

    // add page id and row id in Fine Grain data map
    if (isFineGrain) {
      // add page Id
      doc.add(new IntPoint(PAGEID_NAME, pageId));
      doc.add(new StoredField(PAGEID_NAME, pageId));
      //doc.add(new NumericDocValuesField(PAGEID_NAME,pageId));

      // add row id
      doc.add(new IntPoint(ROWID_NAME, rowId));
      doc.add(new StoredField(ROWID_NAME, rowId));
      //doc.add(new NumericDocValuesField(ROWID_NAME,rowId));
    }

    // add indexed columns value into the document
    Object[] rowData = row.getData();
    List<CarbonColumn> indexColumns = getIndexColumns();
    for (int i = 0; i < rowData.length; i++) {
      if (rowData[i] != null) {
        addField(doc, rowData[i], indexColumns.get(i), Field.Store.NO);
      }
    }

    // add this document
    ramIndexWriter.addDocument(doc);
  }

  private boolean addField(Document doc, Object data, CarbonColumn column, Field.Store store) {
    //get field name
    String fieldName = column.getColName();

    //get field type
    DataType type = column.getDataType();

    if (type == DataTypes.BYTE) {
      // byte type , use int range to deal with byte, lucene has no byte type
      byte value = (byte) data;
      IntRangeField field =
          new IntRangeField(fieldName, new int[] { Byte.MIN_VALUE }, new int[] { Byte.MAX_VALUE });
      field.setIntValue(value);
      doc.add(field);

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, (int) value));
      }
    } else if (type == DataTypes.SHORT) {
      // short type , use int range to deal with short type, lucene has no short type
      short value = (short) data;
      IntRangeField field = new IntRangeField(fieldName, new int[] { Short.MIN_VALUE },
          new int[] { Short.MAX_VALUE });
      field.setShortValue(value);
      doc.add(field);

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, (int) value));
      }
    } else if (type == DataTypes.INT) {
      // int type , use int point to deal with int type
      int value = (int) data;
      doc.add(new IntPoint(fieldName, value));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (type == DataTypes.LONG) {
      // long type , use long point to deal with long type
      long value = (long) data;
      doc.add(new LongPoint(fieldName, value));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (type == DataTypes.FLOAT) {
      float value = (float) data;
      doc.add(new FloatPoint(fieldName, value));
      if (store == Field.Store.YES) {
        doc.add(new FloatPoint(fieldName, value));
      }
    } else if (type == DataTypes.DOUBLE) {
      double value = (double) data;
      doc.add(new DoublePoint(fieldName, value));
      if (store == Field.Store.YES) {
        doc.add(new DoublePoint(fieldName, value));
      }
    } else if (type == DataTypes.STRING) {
      byte[] value = (byte[]) data;
      // TODO: how to get string value
      String strValue = null;
      try {
        strValue = new String(value, 2, value.length - 2, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      doc.add(new TextField(fieldName, strValue, store));
    } else if (type == DataTypes.DATE) {
      throw new RuntimeException("unsupported data type " + type);
    } else if (type == DataTypes.TIMESTAMP) {
      throw new RuntimeException("unsupported data type " + type);
    } else if (type == DataTypes.BOOLEAN) {
      boolean value = (boolean) data;
      IntRangeField field = new IntRangeField(fieldName, new int[] { 0 }, new int[] { 1 });
      field.setIntValue(value ? 1 : 0);
      doc.add(field);
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value ? 1 : 0));
      }
    } else {
      LOGGER.error("unsupport data type " + type);
      throw new RuntimeException("unsupported data type " + type);
    }
    return true;
  }

  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  public void finish() throws IOException {
    // finished a file , close this index writer
    if (indexWriter != null) {
      indexWriter.close();
    }
  }

}
