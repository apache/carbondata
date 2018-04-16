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
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

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
import org.apache.lucene.document.StringField;
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

  private String blockId = null;

  private String dataMapName = null;

  private boolean isFineGrain = true;

  private List<String> indexedCarbonColumns = null;

  private static final String BLOCKID_NAME = "blockId";

  private static final String BLOCKLETID_NAME = "blockletId";

  private static final String PAGEID_NAME = "pageId";

  private static final String ROWID_NAME = "rowId";

  LuceneDataMapWriter(AbsoluteTableIdentifier identifier, String dataMapName, Segment segment,
      String writeDirectoryPath, boolean isFineGrain, List<String> indexedCarbonColumns) {
    super(identifier, segment, writeDirectoryPath);
    this.dataMapName = dataMapName;
    this.isFineGrain = isFineGrain;
    this.indexedCarbonColumns = indexedCarbonColumns;
  }

  private String getIndexPath(long taskId) {
    if (isFineGrain) {
      return genDataMapStorePathOnTaskId(identifier.getTablePath(), segmentId, dataMapName, taskId);
    } else {
      // TODO: where write data in coarse grain data map
      return genDataMapStorePathOnTaskId(identifier.getTablePath(), segmentId, dataMapName, taskId);
    }
  }

  /**
   * Start of new block notification.
   */
  public void onBlockStart(String blockId, long taskId) throws IOException {
    // save this block id for lucene index , used in onPageAdd function
    this.blockId = blockId;

    // get index path, put index data into segment's path
    String strIndexPath = getIndexPath(taskId);
    Path indexPath = FileFactory.getPath(strIndexPath);
    FileSystem fs = FileFactory.getFileSystem(indexPath);

    // if index path not exists, create it
    if (fs.exists(indexPath)) {
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

  /**
   * End of block notification
   */
  public void onBlockEnd(String blockId) throws IOException {
    // clean this block id
    this.blockId = null;

    // finished a file , close this index writer
    if (indexWriter != null) {
      indexWriter.close();
    }

  }

  /**
   * Start of new blocklet notification.
   */
  public void onBlockletStart(int blockletId) {

  }

  /**
   * End of blocklet notification
   */
  public void onBlockletEnd(int blockletId) {

  }

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  public void onPageAdded(int blockletId, int pageId, ColumnPage[] pages) throws IOException {
    // save index data into ram, write into disk after one page finished
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriter ramIndexWriter = new IndexWriter(ramDir, new IndexWriterConfig(analyzer));

    int columnsCount = pages.length;
    if (columnsCount <= 0) {
      LOGGER.warn("empty data");
      ramIndexWriter.close();
      ramDir.close();
      return;
    }
    int pageSize = pages[0].getPageSize();
    for (int rowId = 0; rowId < pageSize; rowId++) {
      // create a new document
      Document doc = new Document();

      // add block id, save this id
      doc.add(new StringField(BLOCKID_NAME, blockId, Field.Store.YES));

      // add blocklet Id
      doc.add(new IntPoint(BLOCKLETID_NAME, new int[] { blockletId }));
      doc.add(new StoredField(BLOCKLETID_NAME, blockletId));
      //doc.add(new NumericDocValuesField(BLOCKLETID_NAME,blockletId));

      // add page id and row id in Fine Grain data map
      if (isFineGrain) {
        // add page Id
        doc.add(new IntPoint(PAGEID_NAME, new int[] { pageId }));
        doc.add(new StoredField(PAGEID_NAME, pageId));
        //doc.add(new NumericDocValuesField(PAGEID_NAME,pageId));

        // add row id
        doc.add(new IntPoint(ROWID_NAME, new int[] { rowId }));
        doc.add(new StoredField(ROWID_NAME, rowId));
        //doc.add(new NumericDocValuesField(ROWID_NAME,rowId));
      }

      // add other fields
      for (int colIdx = 0; colIdx < columnsCount; colIdx++) {
        if (indexedCarbonColumns.contains(pages[colIdx].getColumnSpec().getFieldName())) {
          if (!pages[colIdx].getNullBits().get(rowId)) {
            addField(doc, pages[colIdx], rowId, Field.Store.NO);
          }
        }
      }

      // add this document
      ramIndexWriter.addDocument(doc);

    }
    // close ram writer
    ramIndexWriter.close();

    // add ram index data into disk
    indexWriter.addIndexes(new Directory[] { ramDir });

    // delete this ram data
    ramDir.close();
  }

  private boolean addField(Document doc, ColumnPage page, int rowId, Field.Store store) {
    //get field name
    String fieldName = page.getColumnSpec().getFieldName();

    //get field type
    DataType type = page.getColumnSpec().getSchemaDataType();

    if (type == DataTypes.BYTE) {
      // byte type , use int range to deal with byte, lucene has no byte type
      byte value = page.getByte(rowId);
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
      short value = page.getShort(rowId);
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
      int value = page.getInt(rowId);
      doc.add(new IntPoint(fieldName, new int[] { value }));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (type == DataTypes.LONG) {
      // long type , use long point to deal with long type
      long value = page.getLong(rowId);
      doc.add(new LongPoint(fieldName, new long[] { value }));

      // if need store it , add StoredField
      if (store == Field.Store.YES) {
        doc.add(new StoredField(fieldName, value));
      }
    } else if (type == DataTypes.FLOAT) {
      float value = page.getFloat(rowId);
      doc.add(new FloatPoint(fieldName, new float[] { value }));
      if (store == Field.Store.YES) {
        doc.add(new FloatPoint(fieldName, value));
      }
    } else if (type == DataTypes.DOUBLE) {
      double value = page.getDouble(rowId);
      doc.add(new DoublePoint(fieldName, new double[] { value }));
      if (store == Field.Store.YES) {
        doc.add(new DoublePoint(fieldName, value));
      }
    } else if (type == DataTypes.STRING) {
      byte[] value = page.getBytes(rowId);
      // TODO: how to get string value
      String strValue = null;
      try {
        strValue = new String(value, 2, value.length - 2, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      doc.add(new TextField(fieldName, strValue, store));
    } else if (type == DataTypes.DATE) {
      // TODO: how to get data value
    } else if (type == DataTypes.TIMESTAMP) {
      // TODO: how to get
    } else if (type == DataTypes.BOOLEAN) {
      boolean value = page.getBoolean(rowId);
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

  }

  /**
   * Return store path for datamap
   */
  public static String genDataMapStorePath(String tablePath, String segmentId, String dataMapName) {
    return CarbonTablePath.getSegmentPath(tablePath, segmentId) + File.separator + dataMapName;
  }

  /**
   * Return store path for datamap based on the taskId, if three tasks get launched during loading,
   * then three folders will be created based on the three task Ids and lucene index file will be
   * written into those folders
   * @return store path based on taskID
   */
  private static String genDataMapStorePathOnTaskId(String tablePath, String segmentId,
      String dataMapName, long taskId) {
    return CarbonTablePath.getSegmentPath(tablePath, segmentId) + File.separator + dataMapName
        + File.separator + dataMapName + CarbonCommonConstants.UNDERSCORE + taskId
        + CarbonCommonConstants.UNDERSCORE + System.currentTimeMillis();
  }

  /**
   * returns all the directories of lucene index files for query
   * @param tablePath
   * @param segmentId
   * @param dataMapName
   * @return
   */
  public static CarbonFile[] getAllIndexDirs(String tablePath, String segmentId,
      final String dataMapName) {
    String dmPath =
        CarbonTablePath.getSegmentPath(tablePath, segmentId) + File.separator + dataMapName;
    FileFactory.FileType fileType = FileFactory.getFileType(dmPath);
    final CarbonFile dirPath = FileFactory.getCarbonFile(dmPath, fileType);
    return dirPath.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        if (file.isDirectory() && file.getName().startsWith(dataMapName)) {
          return true;
        } else {
          return false;
        }
      }
    });
  }
}
