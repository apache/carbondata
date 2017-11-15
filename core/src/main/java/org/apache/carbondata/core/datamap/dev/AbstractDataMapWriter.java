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
package org.apache.carbondata.core.datamap.dev;

import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Data Map writer
 */
public abstract class AbstractDataMapWriter {

  protected AbsoluteTableIdentifier identifier;

  protected String segmentId;

  protected String writeDirectoryPath;

  public AbstractDataMapWriter(AbsoluteTableIdentifier identifier, String segmentId,
      String writeDirectoryPath) {
    this.identifier = identifier;
    this.segmentId = segmentId;
    this.writeDirectoryPath = writeDirectoryPath;
  }

  /**
   * Start of new block notification.
   *
   * @param blockId file name of the carbondata file
   */
  public abstract void onBlockStart(String blockId);

  /**
   * End of block notification
   */
  public abstract void onBlockEnd(String blockId);

  /**
   * Start of new blocklet notification.
   *
   * @param blockletId sequence number of blocklet in the block
   */
  public abstract void onBlockletStart(int blockletId);

  /**
   * End of blocklet notification
   *
   * @param blockletId sequence number of blocklet in the block
   */
  public abstract void onBlockletEnd(int blockletId);

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  public abstract void onPageAdded(int blockletId, int pageId, ColumnPage[] pages);

  /**
   * This is called during closing of writer.So after this call no more data will be sent to this
   * class.
   */
  public abstract void finish() throws IOException;

  /**
   * It copies the file from temp folder to actual folder
   *
   * @param dataMapFile
   * @throws IOException
   */
  protected void commitFile(String dataMapFile) throws IOException {
    if (!dataMapFile.startsWith(writeDirectoryPath)) {
      throw new UnsupportedOperationException(
          "Datamap file " + dataMapFile + " is not written in provided directory path "
              + writeDirectoryPath);
    }
    String dataMapFileName =
        dataMapFile.substring(writeDirectoryPath.length(), dataMapFile.length());
    String carbonFilePath = dataMapFileName.substring(0, dataMapFileName.lastIndexOf("/"));
    String segmentPath = CarbonTablePath.getSegmentPath(identifier.getTablePath(), segmentId);
    if (carbonFilePath.length() > 0) {
      carbonFilePath = segmentPath + carbonFilePath;
      FileFactory.mkdirs(carbonFilePath, FileFactory.getFileType(carbonFilePath));
    } else {
      carbonFilePath = segmentPath;
    }
    CarbonUtil.copyCarbonDataFileToCarbonStorePath(dataMapFile, carbonFilePath, 0);
  }

}
