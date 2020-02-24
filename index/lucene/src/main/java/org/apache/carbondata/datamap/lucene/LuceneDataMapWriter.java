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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Implementation to write lucene index while loading
 */
@InterfaceAudience.Internal
public class LuceneDataMapWriter extends DataMapWriter {
  /**
   * logger
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LuceneDataMapWriter.class.getName());

  /**
   * index writer
   */
  private IndexWriter indexWriter = null;

  private Analyzer analyzer = null;

  public static final String PAGEID_NAME = "pageId";

  public static final String ROWID_NAME = "rowId";

  private Codec speedCodec = new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);

  private Codec compressionCodec =
      new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_COMPRESSION);

  private Map<LuceneColumnKeys, Map<Integer, RoaringBitmap>> cache = new HashMap<>();

  private int cacheSize;

  private ByteBuffer intBuffer = ByteBuffer.allocate(4);

  private boolean storeBlockletWise;

  LuceneDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, int flushSize,
      boolean storeBlockletWise) {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    this.cacheSize = flushSize;
    this.storeBlockletWise = storeBlockletWise;
  }

  /**
   * Start of new block notification.
   */
  public void onBlockStart(String blockId) {

  }

  /**
   * End of block notification
   */
  public void onBlockEnd(String blockId) {

  }

  private RAMDirectory ramDir;
  private IndexWriter ramIndexWriter;

  /**
   * Start of new blocklet notification.
   */
  public void onBlockletStart(int blockletId) throws IOException {
    if (null == analyzer) {
      if (CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS,
              CarbonCommonConstants.CARBON_LUCENE_INDEX_STOP_WORDS_DEFAULT)
          .equalsIgnoreCase("true")) {
        analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
      } else {
        analyzer = new StandardAnalyzer();
      }
    }
    // save index data into ram, write into disk after one page finished
    ramDir = new RAMDirectory();
    ramIndexWriter = new IndexWriter(ramDir, new IndexWriterConfig(analyzer));

    if (indexWriter != null) {
      return;
    }
    // get index path, put index data into segment's path
    String dataMapPath;
    if (storeBlockletWise) {
      dataMapPath = this.dataMapPath + File.separator + blockletId;
    } else {
      dataMapPath = this.dataMapPath;
    }
    Path indexPath = FileFactory.getPath(dataMapPath);
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // if index path not exists, create it
    if (!fs.exists(indexPath)) {
      if (!fs.mkdirs(indexPath)) {
        throw new IOException("Failed to create directory " + dataMapPath);
      }
    }

    // the indexWriter closes the FileSystem on closing the writer, so for a new configuration
    // and disable the cache for the index writer, it will be closed on closing the writer
    Configuration conf = FileFactory.getConfiguration();
    conf.set("fs.hdfs.impl.disable.cache", "true");

    // create a index writer
    Directory indexDir = new HdfsDirectory(indexPath, conf);

    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
    if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE,
            CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE_DEFAULT)
        .equalsIgnoreCase(CarbonCommonConstants.CARBON_LUCENE_COMPRESSION_MODE_DEFAULT)) {
      indexWriterConfig.setCodec(speedCodec);
    } else {
      indexWriterConfig
          .setCodec(compressionCodec);
    }

    indexWriter = new IndexWriter(indexDir, indexWriterConfig);
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

    if (storeBlockletWise) {
      flushCache(cache, getIndexColumns(), indexWriter, storeBlockletWise);
      indexWriter.close();
      indexWriter = null;
    }
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
      LOGGER.warn("No data in the page " + pageId + "with blockletid " + blockletId
          + " to write lucene datamap");
      return;
    }
    for (int rowId = 0; rowId < pageSize; rowId++) {
      // add indexed columns value into the document
      LuceneColumnKeys columns = new LuceneColumnKeys(getIndexColumns().size());
      int i = 0;
      for (ColumnPage page : pages) {
        if (!page.getNullBits().get(rowId)) {
          columns.colValues[i++] = getValue(page, rowId);
        }
      }
      if (cacheSize > 0) {
        addToCache(columns, rowId, pageId, blockletId, cache, intBuffer, storeBlockletWise);
      } else {
        addData(columns, rowId, pageId, blockletId, intBuffer, ramIndexWriter, getIndexColumns(),
            storeBlockletWise);
      }
    }
    if (cacheSize > 0) {
      flushCacheIfPossible();
    }
  }

  private static void addField(Document doc, Object key, String fieldName, Field.Store store) {
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
      LOGGER.error("unsupported data type " + type);
      throw new RuntimeException("unsupported data type " + type);
    }
    return value;
  }

  public static void addToCache(LuceneColumnKeys key, int rowId, int pageId, int blockletId,
      Map<LuceneColumnKeys, Map<Integer, RoaringBitmap>> cache, ByteBuffer intBuffer,
      boolean storeBlockletWise) {
    Map<Integer, RoaringBitmap> setMap = cache.get(key);
    if (setMap == null) {
      setMap = new HashMap<>();
      cache.put(key, setMap);
    }
    int combinKey;
    if (!storeBlockletWise) {
      intBuffer.clear();
      intBuffer.putShort((short) blockletId);
      intBuffer.putShort((short) pageId);
      intBuffer.rewind();
      combinKey = intBuffer.getInt();
    } else {
      combinKey = pageId;
    }
    RoaringBitmap bitSet = setMap.get(combinKey);
    if (bitSet == null) {
      bitSet = new RoaringBitmap();
      setMap.put(combinKey, bitSet);
    }
    bitSet.add(rowId);
  }

  public static void addData(LuceneColumnKeys key, int rowId, int pageId, int blockletId,
      ByteBuffer intBuffer, IndexWriter indexWriter, List<CarbonColumn> indexCols,
      boolean storeBlockletWise) throws IOException {

    Document document = new Document();
    for (int i = 0; i < key.getColValues().length; i++) {
      addField(document, key.getColValues()[i], indexCols.get(i).getColName(), Field.Store.NO);
    }
    intBuffer.clear();
    if (storeBlockletWise) {
      // No need to store blocklet id to it.
      intBuffer.putShort((short) pageId);
      intBuffer.putShort((short) rowId);
      intBuffer.rewind();
      document.add(new StoredField(ROWID_NAME, intBuffer.getInt()));
    } else {
      intBuffer.putShort((short) blockletId);
      intBuffer.putShort((short) pageId);
      intBuffer.rewind();
      document.add(new StoredField(PAGEID_NAME, intBuffer.getInt()));
      document.add(new StoredField(ROWID_NAME, (short) rowId));
    }
    indexWriter.addDocument(document);
  }

  private void flushCacheIfPossible() throws IOException {
    if (cache.size() > cacheSize) {
      flushCache(cache, getIndexColumns(), indexWriter, storeBlockletWise);
    }
  }

  public static void flushCache(Map<LuceneColumnKeys, Map<Integer, RoaringBitmap>> cache,
      List<CarbonColumn> indexCols, IndexWriter indexWriter, boolean storeBlockletWise)
      throws IOException {
    for (Map.Entry<LuceneColumnKeys, Map<Integer, RoaringBitmap>> entry : cache.entrySet()) {
      Document document = new Document();
      LuceneColumnKeys key = entry.getKey();
      for (int i = 0; i < key.getColValues().length; i++) {
        addField(document, key.getColValues()[i], indexCols.get(i).getColName(), Field.Store.NO);
      }
      Map<Integer, RoaringBitmap> value = entry.getValue();
      int count = 0;
      for (Map.Entry<Integer, RoaringBitmap> pageData : value.entrySet()) {
        RoaringBitmap bitMap = pageData.getValue();
        int cardinality = bitMap.getCardinality();
        // Each row is short and pageid is stored in int
        ByteBuffer byteBuffer = ByteBuffer.allocate(cardinality * 2 + 4);
        if (!storeBlockletWise) {
          byteBuffer.putInt(pageData.getKey());
        } else {
          byteBuffer.putShort(pageData.getKey().shortValue());
        }
        IntIterator intIterator = bitMap.getIntIterator();
        while (intIterator.hasNext()) {
          byteBuffer.putShort((short) intIterator.next());
        }
        document.add(new StoredField(PAGEID_NAME + count, byteBuffer.array()));
        count++;
      }
      indexWriter.addDocument(document);
    }
    cache.clear();
  }

  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  public void finish() throws IOException {
    if (!isWritingFinished()) {
      flushCache(cache, getIndexColumns(), indexWriter, storeBlockletWise);
      // finished a file , close this index writer
      if (indexWriter != null) {
        indexWriter.close();
        indexWriter = null;
      }
      setWritingFinished(true);
    }
  }

  /**
   * Keeps column values of a single row.
   */
  public static class LuceneColumnKeys {

    private Object[] colValues;

    public LuceneColumnKeys(int size) {
      colValues = new Object[size];
    }

    public Object[] getColValues() {
      return colValues;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LuceneColumnKeys that = (LuceneColumnKeys) o;
      return Arrays.equals(colValues, that.colValues);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(colValues);
    }
  }
}
