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

package org.apache.carbondata.core.datastore.chunk.impl;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.blocklet.PresenceMeta;

public abstract class AbstractPrimitiveDimColumnPage implements DimensionColumnPage {

  protected ColumnPage columnPage;

  protected int[] actualRowId;

  protected int[] currentRowId;

  protected boolean isExplictSorted;

  protected PresenceMeta presenceMeta;

  protected AbstractPrimitiveDimColumnPage(ColumnPage columnPage, int[] actualRowId,
      int[] invertedIndexReverse) {
    this.columnPage = columnPage;
    this.isExplictSorted = null != actualRowId && actualRowId.length > 0;
    this.actualRowId = actualRowId;
    this.currentRowId = invertedIndexReverse;
    this.presenceMeta = columnPage.getPresenceMeta();
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    return 0;
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    return 0;
  }
  @Override public PresenceMeta getPresentMeta() {
    return columnPage.getPresenceMeta();
  }

  @Override public boolean isAdaptiveEncoded() {
    return true;
  }

  @Override public void freeMemory() {
    if (null != columnPage) {
      columnPage.freeMemory();
      this.currentRowId = null;
      this.actualRowId = null;
      columnPage = null;
    }
  }

  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override public boolean isExplicitSorted() {
    return isExplictSorted;
  }

  @Override public int getInvertedIndex(int rowId) {
    return actualRowId[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return currentRowId[rowId];
  }
}
