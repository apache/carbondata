/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.impl.btree;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to find a block in a btree
 */
public class BTreeDataRefNodeFinder implements DataRefNodeFinder {

  /**
   * no dictionary column value is of variable length so in each column value
   * it will -1
   */
  private static final int NO_DCITIONARY_COLUMN_VALUE = -1;

  /**
   * sized of the short value in bytes
   */
  private static final short SHORT_SIZE_IN_BYTES = 2;
  /**
   * this will holds the information about the size of each value of a column,
   * this will be used during Comparison of the btree node value and the
   * search value if value is more than zero then its a fixed length column
   * else its variable length column. So as data of both type of column store
   * separately so this value size array will be used for both purpose
   * comparison and jumping(which type value we need to compare)
   */
  private int[] eachColumnValueSize;

  /**
   * this will be used during search for no dictionary column
   */
  private int numberOfNoDictionaryColumns;

  public BTreeDataRefNodeFinder(int[] eachColumnValueSize) {
    this.eachColumnValueSize = eachColumnValueSize;

    for (int i = 0; i < eachColumnValueSize.length; i++) {
      if (eachColumnValueSize[i] == -1) {
        numberOfNoDictionaryColumns++;
      }
    }
  }

  /**
   * Below method will be used to get the first tentative data block based on
   * search key
   *
   * @param dataBlocks complete data blocks present
   * @param serachKey  key to be search
   * @return data block
   */
  @Override public DataRefNode findFirstDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
    // as its for btree type cast it to btree interface
    BTreeNode rootNode = (BTreeNode) dataRefBlock;
    while (!rootNode.isLeafNode()) {
      rootNode = findFirstLeafNode(searchKey, rootNode);
    }
    return rootNode;
  }

  /**
   * Below method will be used to get the last data tentative block based on
   * search key
   *
   * @param dataBlocks complete data blocks present
   * @param serachKey  key to be search
   * @return data block
   */
  @Override public DataRefNode findLastDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
    // as its for btree type cast it to btree interface
    BTreeNode rootNode = (BTreeNode) dataRefBlock;
    while (!rootNode.isLeafNode()) {
      rootNode = findLastLeafNode(searchKey, rootNode);
    }
    return rootNode;
  }

  /**
   * Binary search used to get the first tentative block of the btree based on
   * search key
   *
   * @param key  search key
   * @param node root node of btree
   * @return first tentative block
   */
  private BTreeNode findFirstLeafNode(IndexKey key, BTreeNode node) {
    int childNodeIndex;
    int low = 0;
    int high = node.nodeSize() - 1;
    int mid = 0;
    int compareRes = -1;
    IndexKey[] nodeKeys = node.getNodeKeys();
    //
    while (low <= high) {
      mid = (low + high) >>> 1;
      // compare the entries
      compareRes = compareIndexes(key, nodeKeys[mid]);
      if (compareRes < 0) {
        high = mid - 1;
      } else if (compareRes > 0) {
        low = mid + 1;
      } else {
        // if key is matched then get the first entry
        int currentPos = mid;
        while (currentPos - 1 >= 0 && compareIndexes(key, nodeKeys[currentPos - 1]) == 0) {
          currentPos--;
        }
        mid = currentPos;
        break;
      }
    }
    // if compare result is less than zero then we
    // and mid is more than 0 then we need to previous block as duplicates
    // record can be present
    if (compareRes < 0) {
      if (mid > 0) {
        mid--;
      }
      childNodeIndex = mid;
    } else {
      childNodeIndex = mid;
    }
    // get the leaf child
    node = node.getChild(childNodeIndex);
    return node;
  }

  /**
   * Binary search used to get the last tentative block of the btree based on
   * search key
   *
   * @param key  search key
   * @param node root node of btree
   * @return first tentative block
   */
  private BTreeNode findLastLeafNode(IndexKey key, BTreeNode node) {
    int childNodeIndex;
    int low = 0;
    int high = node.nodeSize() - 1;
    int mid = 0;
    int compareRes = -1;
    IndexKey[] nodeKeys = node.getNodeKeys();
    //
    while (low <= high) {
      mid = (low + high) >>> 1;
      // compare the entries
      compareRes = compareIndexes(key, nodeKeys[mid]);
      if (compareRes < 0) {
        high = mid - 1;
      } else if (compareRes > 0) {
        low = mid + 1;
      } else {
        int currentPos = mid;
        // if key is matched then get the first entry
        while (currentPos + 1 < node.nodeSize()
            && compareIndexes(key, nodeKeys[currentPos + 1]) == 0) {
          currentPos++;
        }
        mid = currentPos;
        break;
      }
    }
    // if compare result is less than zero then we
    // and mid is more than 0 then we need to previous block as duplicates
    // record can be present
    if (compareRes < 0) {
      if (mid > 0) {
        mid--;
      }
      childNodeIndex = mid;
    } else {
      childNodeIndex = mid;
    }
    node = node.getChild(childNodeIndex);
    return node;
  }

  /**
   * Comparison of index key will be following format of key <Dictionary> key
   * will be in byte array No dictionary key Index of FirstKey (2
   * bytes)><Index of SecondKey (2 bytes)><Index of NKey (2 bytes)> <First Key
   * ByteArray><2nd Key ByteArray><N Key ByteArray> in each column value size
   * of no dictionary column will be -1 if in each column value is not -1 then
   * compare the byte array based on size and increment the offset to
   * dictionary column size if size is -1 then its a no dictionary key so to
   * get the length subtract the size of current with next key offset it will
   * give the actual length if it is at last position or only one key is
   * present then subtract with length
   *
   * @param first  key
   * @param second key
   * @return comparison value
   */
  private int compareIndexes(IndexKey first, IndexKey second) {
    int dictionaryKeyOffset = 0;
    int nonDictionaryKeyOffset = 0;
    int compareResult = 0;
    int processedNoDictionaryColumn = numberOfNoDictionaryColumns;
    ByteBuffer firstNoDictionaryKeyBuffer = ByteBuffer.wrap(first.getNoDictionaryKeys());
    ByteBuffer secondNoDictionaryKeyBuffer = ByteBuffer.wrap(second.getNoDictionaryKeys());
    int actualOffset = 0;
    int actualOffset1 = 0;
    int firstNoDcitionaryLength = 0;
    int secondNodeDictionaryLength = 0;

    for (int i = 0; i < eachColumnValueSize.length; i++) {

      if (eachColumnValueSize[i] != NO_DCITIONARY_COLUMN_VALUE) {
        compareResult = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(first.getDictionaryKeys(), dictionaryKeyOffset, eachColumnValueSize[i],
                second.getDictionaryKeys(), dictionaryKeyOffset, eachColumnValueSize[i]);
        dictionaryKeyOffset += eachColumnValueSize[i];
      } else {
        if (processedNoDictionaryColumn > 1) {
          actualOffset = firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          firstNoDcitionaryLength =
              firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset + SHORT_SIZE_IN_BYTES)
                      - actualOffset;
          actualOffset1 = secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          secondNodeDictionaryLength =
              secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset + SHORT_SIZE_IN_BYTES)
                      - actualOffset1;
          compareResult = ByteUtil.UnsafeComparer.INSTANCE
                  .compareTo(first.getNoDictionaryKeys(), actualOffset, firstNoDcitionaryLength,
                          second.getNoDictionaryKeys(), actualOffset1, secondNodeDictionaryLength);
          nonDictionaryKeyOffset += SHORT_SIZE_IN_BYTES;
          processedNoDictionaryColumn--;
        } else {
          actualOffset = firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          actualOffset1 = secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          firstNoDcitionaryLength = first.getNoDictionaryKeys().length - actualOffset;
          secondNodeDictionaryLength = second.getNoDictionaryKeys().length - actualOffset1;
          compareResult = ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(first.getNoDictionaryKeys(), actualOffset, firstNoDcitionaryLength,
                  second.getNoDictionaryKeys(), actualOffset1, secondNodeDictionaryLength);
        }
      }
      if (compareResult != 0) {
        return compareResult;
      }
    }

    return 0;
  }
}
