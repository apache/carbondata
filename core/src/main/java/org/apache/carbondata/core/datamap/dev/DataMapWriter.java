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

import org.apache.carbondata.core.datastore.page.ColumnPage;

/**
 * Data Map writer
 */
public interface DataMapWriter {

  /**
   *  Start of new block notification.
   *  @param blockId file name of the carbondata file
   */
  void onBlockStart(String blockId);

  /**
   * End of block notification
   */
  void onBlockEnd(String blockId);

  /**
   * Start of new blocklet notification.
   * @param blockletId sequence number of blocklet in the block
   */
  void onBlockletStart(int blockletId);

  /**
   * End of blocklet notification
   * @param blockletId sequence number of blocklet in the block
   */
  void onBlockletEnd(int blockletId);

  /**
   * Add the column pages row to the datamap, order of pages is same as `indexColumns` in
   * DataMapMeta returned in DataMapFactory.
   *
   * Implementation should copy the content of `pages` as needed, because `pages` memory
   * may be freed after this method returns, if using unsafe column page.
   */
  void onPageAdded(int blockletId, int pageId, ColumnPage[] pages);

}
