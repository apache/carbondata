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

import java.io.IOException;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * Non leaf node abstract class
 */
public abstract class AbstractBTreeLeafNode implements BTreeNode {

  /**
   * number of keys in a btree
   */
  int numberOfKeys;

  /**
   * node number
   */
  long nodeNumber;

  /**
   * Next node of the leaf
   */
  private BTreeNode nextNode;

  /**
   * max key of the column this will be used to check whether this leaf will
   * be used for scanning or not
   */
  byte[][] maxKeyOfColumns;

  /**
   * min key of the column this will be used to check whether this leaf will
   * be used for scanning or not
   */
  byte[][] minKeyOfColumns;

  /**
   * Method to get the next block this can be used while scanning when
   * iterator of this class can be used iterate over blocks
   *
   * @return next block
   */
  @Override public int numRows() {
    return this.numberOfKeys;
  }

  /**
   * below method will used to set the next node
   *
   * @param nextNode
   */
  @Override public void setNextNode(BTreeNode nextNode) {
    this.nextNode = nextNode;
  }

  /**
   * Below method is to get the children based on index
   *
   * @param index children index
   * @return btree node
   */
  @Override public BTreeNode getChild(int index) {
    throw new UnsupportedOperationException("Operation not supported in case of leaf node");
  }

  /**
   * below method to set the node entry
   *
   * @param key node entry
   */
  @Override public void setKey(IndexKey key) {
    throw new UnsupportedOperationException("Operation not supported in case of leaf node");
  }

  /**
   * Method can be used to get the block index .This can be used when multiple
   * thread can be used scan group of blocks in that can we can assign the
   * some of the blocks to one thread and some to other
   *
   * @return block number
   */
  @Override public long nodeIndex() {
    return nodeNumber;
  }

  /**
   * This method will be used to get the max value of all the columns this can
   * be used in case of filter query
   *
   */
  @Override public byte[][] getColumnsMaxValue() {
    return maxKeyOfColumns;
  }

  /**
   * This method will be used to get the max value of all the columns this can
   * be used in case of filter query
   *
   */
  @Override public byte[][] getColumnsMinValue() {
    return minKeyOfColumns;
  }

  /**
   * to check whether node in a btree is a leaf node or not
   *
   * @return leaf node or not
   */
  @Override public boolean isLeafNode() {
    return true;
  }

  /**
   * Method to get the next block this can be used while scanning when
   * iterator of this class can be used iterate over blocks
   *
   * @return next block
   */
  @Override public DataRefNode getNextDataRefNode() {
    return nextNode;
  }

  /**
   * below method will return the one node indexes
   *
   * @return node entry array
   */
  @Override public IndexKey[] getNodeKeys() {
    // as this is a leaf node so this method implementation is not required
    throw new UnsupportedOperationException("Operation not supported in case of leaf node");
  }

  /**
   * below method will be used to set the children of intermediate node
   *
   * @param children array
   */
  @Override public void setChildren(BTreeNode[] children) {
    // no required in case of leaf node as leaf node will not have any children
    throw new UnsupportedOperationException("Operation not supported in case of leaf node");
  }

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param columnIndexRange indexes of the blocks need to be read
   * @return dimension data chunks
   */
  @Override public DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    // No required here as leaf which will will be use this class will implement its own get
    // dimension chunks
    return null;
  }

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @param columnIndex block index to be read
   * @return dimension data chunk
   */
  @Override public DimensionRawColumnChunk readDimensionChunk(FileReader fileReader,
      int columnIndex) throws IOException {
    // No required here as leaf which will will be use this class will implement
    // its own get dimension chunks
    return null;
  }

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param columnIndexRange block indexes to be read from file
   * @return measure column data chunk
   */
  @Override public MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader,
      int[][] columnIndexRange) throws IOException {
    // No required here as leaf which will will be use this class will implement its own get
    // measure chunks
    return null;
  }

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param columnIndex block index to be read from file
   * @return measure data chunk
   */
  @Override public MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex)
      throws IOException {
    // No required here as leaf which will will be use this class will implement its own get
    // measure chunks
    return null;
  }

  @Override
  public int getPageRowCount(int pageNumber) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override public BitSetGroup getIndexedData() {
    return null;
  }
}
