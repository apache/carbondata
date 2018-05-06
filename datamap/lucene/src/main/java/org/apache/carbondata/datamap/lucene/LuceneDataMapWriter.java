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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;

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
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.roaringbitmap.RoaringBitmap;

/**
 * Implementation to write lucene index while loading
 */
@InterfaceAudience.Internal public class LuceneDataMapWriter extends DataMapWriter {
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

  private Map<LuceneColumns, Map<Integer, RoaringBitmap>> map = new HashMap<>();

  private Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private int cacheSize;

  private ByteBuffer intBuffer = ByteBuffer.allocate(4);


  LuceneDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, boolean isFineGrain, int flushSize) {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    this.isFineGrain = isFineGrain;
    this.cacheSize = flushSize;
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

    indexWriter = new IndexWriter(indexDir, indexWriterConfig);
  }

  /**
   * End of block notification
   */
  public void onBlockEnd(String blockId) throws IOException {
  }

  /**
   * Start of new blocklet notification.
   */
  public void onBlockletStart(int blockletId) throws IOException {
  }

  /**
   * End of blocklet notification
   */
  public void onBlockletEnd(int blockletId) throws IOException {
  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  public void onPageAdded(int blockletId, int pageId, int pageSize, ColumnPage[] pages)
      throws IOException {
    // save index data into ram, write into disk after one page finished
    int columnsCount = pages.length;
    if (columnsCount <= 0) {
      LOGGER.warn("empty data");
      return;
    }
    for (int rowId = 0; rowId < pageSize; rowId++) {
      // add indexed columns value into the document
      LuceneColumns columns = new LuceneColumns(getIndexColumns().size());
      int i = 0;
      for (ColumnPage page : pages) {
        if (!page.getNullBits().get(rowId)) {
          columns.keys[i++] = getValue(page, rowId);
        }
        addToCache(columns, rowId, pageId, blockletId);
      }
      flushCacheIfCan();
    }
  }

  private boolean addField(Document doc, Object key, String fieldName, Field.Store store) {
    //get field name
    if (key instanceof Byte) {
      // byte type , use int range to deal with byte, lucene has no byte type
      byte value = (Byte) key;
      IntRangeField field =
          new IntRangeField(fieldName, new int[] { Byte.MIN_VALUE }, new int[] { Byte.MAX_VALUE });
      field.setIntValue(value);
      doc.add(field);

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, (int) value));
      }
    } else if (key instanceof Short) {
      // short type , use int range to deal with short type, lucene has no short type
      short value = (Short) key;
      IntRangeField field = new IntRangeField(fieldName, new int[] { Short.MIN_VALUE },
          new int[] { Short.MAX_VALUE });
      field.setShortValue(value);
      doc.add(field);

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, (int) value));
      }
    } else if (key instanceof Integer) {
      // int type , use int point to deal with int type
      int value = (Integer) key;
      doc.add(new IntPoint(fieldName, new int[] { value }));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (key instanceof Long) {
      // long type , use long point to deal with long type
      long value = (Long) key;
      doc.add(new LongPoint(fieldName, new long[] { value }));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (key instanceof Float) {
      float value = (Float) key;
      doc.add(new FloatPoint(fieldName, new float[] { value }));
      if (store == Field.Store.YES) {
        doc.add(new FloatPoint(fieldName, value));
      }
    } else if (key instanceof Double) {
      double value = (Double) key;
      doc.add(new DoublePoint(fieldName, new double[] { value }));
      if (store == Field.Store.YES) {
        doc.add(new DoublePoint(fieldName, value));
      }
    } else if (key instanceof String) {
      String strValue = (String) key;
      doc.add(new TextField(fieldName, strValue, store));
    } else if (key instanceof Boolean) {
      boolean value = (Boolean) key;
      IntRangeField field = new IntRangeField(fieldName, new int[] { 0 }, new int[] { 1 });
      field.setIntValue(value ? 1 : 0);
      doc.add(field);
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value ? 1 : 0));
      }
    }
    return true;
  }

  private Object getValue(ColumnPage page, int rowId) {

    //get field type
    DataType type = page.getColumnSpec().getSchemaDataType();
    Object value = null;
    if (type == DataTypes.BYTE) {
      // byte type , use int range to deal with byte, lucene has no byte type
      value = page.getByte(rowId);
    } else if (type == DataTypes.SHORT) {
      // short type , use int range to deal with short type, lucene has no short type
      value = page.getShort(rowId);
    } else if (type == DataTypes.INT) {
      // int type , use int point to deal with int type
      value = page.getInt(rowId);
    } else if (type == DataTypes.LONG) {
      // long type , use long point to deal with long type
      value = page.getLong(rowId);
    } else if (type == DataTypes.FLOAT) {
      value = page.getFloat(rowId);
    } else if (type == DataTypes.DOUBLE) {
      value = page.getDouble(rowId);
    } else if (type == DataTypes.STRING) {
      byte[] bytes = page.getBytes(rowId);
      try {
        value = new String(bytes, 2, bytes.length - 2, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    } else if (type == DataTypes.DATE) {
      throw new RuntimeException("unsupported data type " + type);
    } else if (type == DataTypes.TIMESTAMP) {
      throw new RuntimeException("unsupported data type " + type);
    } else if (type == DataTypes.BOOLEAN) {
      value = page.getBoolean(rowId);
    } else {
      LOGGER.error("unsupport data type " + type);
      throw new RuntimeException("unsupported data type " + type);
    }
    return value;
  }

  private void addToCache(LuceneColumns key, int rowId, int pageId, int blockletId) {
    Map<Integer, RoaringBitmap> setMap = map.get(key);
    if (setMap == null) {
      setMap = new HashMap<>();
      map.put(key, setMap);
    }
    intBuffer.clear();
    intBuffer.putShort((short) blockletId);
    intBuffer.putShort((short) pageId);
    intBuffer.rewind();
    int combinKey = intBuffer.getInt();
    RoaringBitmap bitSet = setMap.get(combinKey);
    if (bitSet == null) {
      bitSet = new RoaringBitmap();
      setMap.put(combinKey, bitSet);
    }
    bitSet.add(rowId);
  }

  private void flushCacheIfCan() throws IOException {
    if (map.size() > cacheSize) {
      flushCache();
    }
  }

  private void flushCache() throws IOException {
    for (Map.Entry<LuceneColumns, Map<Integer, RoaringBitmap>> entry : map.entrySet()) {
      Document document = new Document();
      LuceneColumns key = entry.getKey();
      for (int i = 0; i < key.getKeys().length; i++) {
        addField(document, key.getKeys()[i], getIndexColumns().get(i).getColName(),
            Field.Store.NO);
      }
      Map<Integer, RoaringBitmap> value = entry.getValue();
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutputStream outputStream = new DataOutputStream(stream);
      outputStream.writeInt(value.size());
      for (Map.Entry<Integer, RoaringBitmap> pageData : value.entrySet()) {
        outputStream.writeInt(pageData.getKey());
        pageData.getValue().serialize(outputStream);
      }
      outputStream.close();
      document.add(new StoredField(PAGEID_NAME, compressor.compressByte(stream.toByteArray())));

      indexWriter.addDocument(document);
    }
    map.clear();
  }

  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  public void finish() throws IOException {
    flushCache();
    // finished a file , close this index writer
    if (indexWriter != null) {
      indexWriter.close();
    }
  }

  private static class LuceneColumns {

    private Object[] keys;

    public LuceneColumns(int size) {
      keys = new Object[size];
    }

    public Object[] getKeys() {
      return keys;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LuceneColumns that = (LuceneColumns) o;
      return Arrays.equals(keys, that.keys);
    }

    @Override public int hashCode() {
      return Arrays.hashCode(keys);
    }
  }
}
