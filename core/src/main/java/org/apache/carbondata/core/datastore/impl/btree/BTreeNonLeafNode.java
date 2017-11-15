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
package org.apache.carbondata.core.datastore.impl.btree;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * No leaf node of a b+tree class which will keep the matadata(start key) of the
 * leaf node
 */
public class BTreeNonLeafNode implements BTreeNode {

  /**
   * Child nodes
   */
  private BTreeNode[] children;

  /**
   * list of keys in non leaf
   */
  private List<IndexKey> listOfKeys;

  BTreeNonLeafNode() {
    // creating a list which will store all the indexes
    listOfKeys = new ArrayList<IndexKey>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * below method will return the one node indexes
   *
   * @return getting a complete leaf ]node keys
   */
  @Override public IndexKey[] getNodeKeys() {
    return listOfKeys.toArray(new IndexKey[listOfKeys.size()]);
  }

  /**
   * as it is a non leaf node it will have the reference of all the leaf node
   * under it, setting all the children
   *
   */
  @Override public void setChildren(BTreeNode[] children) {
    this.children = children;
  }

  /**
   * setting the next node
   */
  @Override public void setNextNode(BTreeNode nextNode) {
    // no required in case of non leaf node
  }

  /**
   * get the leaf node based on children
   *
   * @return leaf node
   */
  @Override public BTreeNode getChild(int index) {
    return this.children[index];
  }

  /**
   * add a key of a leaf node
   *
   */
  @Override public void setKey(IndexKey key) {
    listOfKeys.add(key);

  }

  /**
   * @return whether its a leaf node or not
   */
  @Override public boolean isLeafNode() {
    return false;
  }

  /**
   * Method to get the next block this can be used while scanning when
   * iterator of this class can be used iterate over blocks
   *
   * @return next block
   */
  @Override public DataRefNode getNextDataRefNode() {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * to get the number of keys tuples present in the block
   *
   * @return number of keys in the block
   */
  @Override public int numRows() {
    return listOfKeys.size();
  }

  /**
   * Method can be used to get the block index .This can be used when multiple
   * thread can be used scan group of blocks in that can we can assign the
   * some of the blocks to one thread and some to other
   *
   * @return block number
   */
  @Override public long nodeIndex() {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override public short blockletIndex() {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * This method will be used to get the max value of all the columns this can
   * be used in case of filter query
   *
   */
  @Override public byte[][] getColumnsMaxValue() {
    // operation of getting the max value is not supported as its a non leaf
    // node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * This method will be used to get the max value of all the columns this can
   * be used in case of filter query
   *
   */
  @Override public byte[][] getColumnsMinValue() {
    // operation of getting the min value is not supported as its a non leaf
    // node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param columnIndexRange indexes of the blocks need to be read
   * @return dimension data chunks
   */
  @Override public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader,
      int[][] columnIndexRange) {

    // operation of getting the dimension chunks is not supported as its a
    // non leaf node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @return dimension data chunk
   */
  @Override public DimensionRawColumnChunk readDimensionChunk(FileReader fileReader,
      int columnIndex) {
    // operation of getting the dimension chunk is not supported as its a
    // non leaf node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param columnIndexRange block indexes to be read from file
   * @return measure column data chunk
   */
  @Override public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader,
      int[][] columnIndexRange) {
    // operation of getting the measure chunk is not supported as its a non
    // leaf node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param columnIndex block index to be read from file
   * @return measure data chunk
   */

  @Override public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex) {
    // operation of getting the measure chunk is not supported as its a non
    // leaf node
    // and in case of B+Tree data will be stored only in leaf node and
    // intermediate
    // node will be used only for searching the leaf node
    throw new UnsupportedOperationException("Unsupported operation");
  }

  public BitSetGroup getIndexedData() {
    return null;
  }

  /**
   * number of pages in blocklet
   * @return
   */
  @Override
  public int numberOfPages() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public int getPageRowCount(int pageNumber) {
    throw new UnsupportedOperationException("Unsupported operation");
  }
}
