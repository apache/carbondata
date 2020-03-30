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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Writer interface for index.
 * Developer should implement this interface to write index files.
 * Writer will be called for every new block/blocklet/page is created when data load is executing.
 */
@InterfaceAudience.Developer("Index")
@InterfaceStability.Evolving
public abstract class IndexWriter {

  protected String tablePath;

  protected String segmentId;

  protected String indexPath;

  protected List<CarbonColumn> indexColumns;

  private boolean isWritingFinished;

  public IndexWriter(String tablePath, String indexName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName) {
    this.tablePath = tablePath;
    this.segmentId = segment.getSegmentNo();
    this.indexPath = CarbonTablePath.getIndexStorePathOnShardName(
        tablePath, segmentId, indexName, shardName);
    this.indexColumns = indexColumns;
  }

  protected final List<CarbonColumn> getIndexColumns() {
    return indexColumns;
  }

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  public abstract void onBlockStart(String blockId) throws IOException;

  /**
   * End of block notification
   */
  public abstract void onBlockEnd(String blockId) throws IOException;

  /**
   * Start of new blocklet notification.
   *
   * @param blockletId sequence number of blocklet in the block
   */
  public abstract void onBlockletStart(int blockletId) throws IOException;

  /**
   * End of blocklet notification
   *
   * @param blockletId sequence number of blocklet in the block
   */
  public abstract void onBlockletEnd(int blockletId) throws IOException;

  /**
   * Add columnar page data to the index, order of field is same as `indexColumns` in
   * IndexMeta returned in IndexFactory.
   * Implementation should copy the content of it as needed, because its memory
   * may be freed after this method returns, in case of unsafe memory
   */
  public abstract void onPageAdded(int blockletId, int pageId, int pageSize, ColumnPage[] pages)
      throws IOException;

  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  public abstract void finish() throws IOException;

  /**
   * It commits the index file by copying the file from temp folder to actual folder
   *
   * @param indexFile file path of index file
   * @throws IOException if IO fails
   */
  protected void commitFile(String indexFile) throws IOException {
    if (!indexFile.startsWith(indexPath)) {
      throw new UnsupportedOperationException(
          "index file " + indexFile + " is not written in provided directory path "
              + indexPath);
    }
    String indexFileName =
        indexFile.substring(indexPath.length(), indexFile.length());
    String carbonFilePath = indexFileName.substring(0, indexFileName.lastIndexOf("/"));
    String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
    if (carbonFilePath.length() > 0) {
      carbonFilePath = segmentPath + carbonFilePath;
      FileFactory.mkdirs(carbonFilePath);
    } else {
      carbonFilePath = segmentPath;
    }
    CarbonUtil.copyCarbonDataFileToCarbonStorePath(indexFile, carbonFilePath, 0);
  }

  /**
   * Return store path for index
   */
  public static String getDefaultIndexPath(
      String tablePath, String segmentId, String indexName) {
    return CarbonTablePath.getIndexesStorePath(tablePath, segmentId, indexName);
  }

  public boolean isWritingFinished() {
    return isWritingFinished;
  }

  public void setWritingFinished(boolean writingFinished) {
    isWritingFinished = writingFinished;
  }
}
