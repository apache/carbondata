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
package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * FineGrainBlocklet
 */
public class FineGrainBlocklet extends Blocklet implements Serializable {

  private List<Page> pages;

  public FineGrainBlocklet(String blockId, String blockletId, List<Page> pages) {
    super(blockId, blockletId);
    this.pages = pages;
  }

  // For serialization purpose
  public FineGrainBlocklet() {

  }

  public List<Page> getPages() {
    return pages;
  }

  public static class Page implements Writable,Serializable {

    private int pageId;

    private int[] rowId;

    public BitSet getBitSet() {
      BitSet bitSet =
          new BitSet(CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT);
      for (int row : rowId) {
        bitSet.set(row);
      }
      return bitSet;
    }

    @Override public void write(DataOutput out) throws IOException {
      out.writeInt(pageId);
      out.writeInt(rowId.length);
      for (int i = 0; i < rowId.length; i++) {
        out.writeInt(rowId[i]);
      }
    }

    @Override public void readFields(DataInput in) throws IOException {
      pageId = in.readInt();
      int length = in.readInt();
      rowId = new int[length];
      for (int i = 0; i < length; i++) {
        rowId[i] = in.readInt();
      }
    }

    public void setPageId(int pageId) {
      this.pageId = pageId;
    }

    public void setRowId(int[] rowId) {
      this.rowId = rowId;
    }
  }

  public BitSetGroup getBitSetGroup(int numberOfPages) {
    BitSetGroup bitSetGroup = new BitSetGroup(numberOfPages);
    for (int i = 0; i < pages.size(); i++) {
      bitSetGroup.setBitSet(pages.get(i).getBitSet(), pages.get(i).pageId);
    }
    return bitSetGroup;
  }

  @Override public void write(DataOutput out) throws IOException {
    super.write(out);
    int size = pages.size();
    out.writeInt(size);
    for (Page page : pages) {
      page.write(out);
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int size = in.readInt();
    pages = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Page page = new Page();
      page.readFields(in);
      pages.add(page);
    }
  }

  @Override public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }
}
