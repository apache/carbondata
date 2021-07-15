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

package org.apache.carbondata.core.range;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET;

/**
 * This class prepares a tree for pruning using min-max of block
 */
public class BlockMinMaxTree implements Serializable {

  private MinMaxNode root;

  private final boolean isPrimitiveAndNotDate;
  private final boolean isDimensionColumn;
  private final DataType joinDataType;
  private final SerializableComparator comparator;

  public BlockMinMaxTree(boolean isPrimitiveAndNotDate, boolean isDimensionColumn,
      DataType joinDataType, SerializableComparator comparator) {
    this.isPrimitiveAndNotDate = isPrimitiveAndNotDate;
    this.isDimensionColumn = isDimensionColumn;
    this.joinDataType = joinDataType;
    this.comparator = comparator;
  }

  public MinMaxNode getRoot() {
    return root;
  }

  public void insert(MinMaxNode newMinMaxNode) {
    root = insert(getRoot(), newMinMaxNode);
  }

  private MinMaxNode insert(MinMaxNode root, MinMaxNode newMinMaxNode) {
    /* 1. check if the root null, then insert and make new node
     * 2. check if the new node completely overlaps with the root, where minCompare and maxCompare
     * both are zero, if yes add the filepaths and return
     * 3. if root is less than new node, check if the root has right subtree,
     *    if(yes) {
     *       replace the right node with the newnode's min and max based on comparison and then
     *        call insert with right node as new root and newnode
     *         insert(root.getRight, newnode)
     *     } else {
     *       make the new node as right node and set right node and return
     *     }
     * 4. if root is more than new node, check if the root has left subtree,
     *    if(yes) {
     *       replace the left node with the newnode's min and max based on comparison and then
     *        call insert with left node as new root and newnode
     *         insert(root.getLeft, newnode)
     *     } else {
     *       make the new node as left node and set left node and return
     *     }
     * */
    if (root == null) {
      root = newMinMaxNode;
      return root;
    }

    if (compareNodesBasedOnMinMax(root, newMinMaxNode) == 0) {
      root.addFilePats(newMinMaxNode.getFilePaths());
      return root;
    }

    if (compareNodesBasedOnMinMax(root, newMinMaxNode) < 0) {
      if (root.getRightSubTree() == null) {
        root.setRightSubTree(newMinMaxNode);
        root.setRightSubTreeMax(newMinMaxNode.getMax());
        root.setRightSubTreeMin(newMinMaxNode.getMin());
      } else {
        if (compareMinMax(root.getRightSubTreeMax(), newMinMaxNode.getMax()) < 0) {
          root.setRightSubTreeMax(newMinMaxNode.getMax());
        }
        if (compareMinMax(root.getRightSubTreeMin(), newMinMaxNode.getMin()) > 0) {
          root.setRightSubTreeMin(newMinMaxNode.getMin());
        }
        insert(root.getRightSubTree(), newMinMaxNode);
      }
    } else {
      if (root.getLeftSubTree() == null) {
        root.setLeftSubTree(newMinMaxNode);
        root.setLeftSubTreeMax(newMinMaxNode.getMax());
        root.setLeftSubTreeMin(newMinMaxNode.getMin());
      } else {
        if (compareMinMax(root.getLeftSubTreeMax(), newMinMaxNode.getMax()) < 0) {
          root.setLeftSubTreeMax(newMinMaxNode.getMax());
        }
        if (compareMinMax(root.getLeftSubTreeMin(), newMinMaxNode.getMin()) > 0) {
          root.setLeftSubTreeMin(newMinMaxNode.getMin());
        }
        insert(root.getLeftSubTree(), newMinMaxNode);
      }
    }
    return root;
  }

  private int compareNodesBasedOnMinMax(MinMaxNode root, MinMaxNode newMinMaxNode) {
    int minCompare = compareMinMax(root.getMin(), newMinMaxNode.getMin());
    int maxCompare = compareMinMax(root.getMax(), newMinMaxNode.getMax());
    if (minCompare == 0) {
      return maxCompare;
    } else {
      return minCompare;
    }
  }

  private int compareMinMax(Object key1, Object key2) {
    if (isDimensionColumn) {
      if (isPrimitiveAndNotDate) {
        return comparator.compare(key1, key2);
      } else {
        return ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(key1.toString().getBytes(Charset.forName(DEFAULT_CHARSET)),
                key2.toString().getBytes(Charset.forName(DEFAULT_CHARSET)));
      }
    } else {
      return comparator.compare(key1, key2);
    }
  }

  /**
   * This method returns the list of carbondata files where the input fieldValue might present
   */
  public Set<String> getMatchingFiles(byte[] fieldValue, Set<String> matchedFilesSet) {
    getMatchingFiles(getRoot(), fieldValue, matchedFilesSet);
    return matchedFilesSet;
  }

  private void getMatchingFiles(MinMaxNode root, byte[] fieldValue, Set<String> matchedFilesSet) {
    Object data;
    if (root == null) {
      return;
    }
    if (isDimensionColumn) {
      if (isPrimitiveAndNotDate) {
        data = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(fieldValue, joinDataType);
      } else {
        // for string comparator is Unsafe comparator, so cannot include in common code
        getMatchingFilesForString(root, fieldValue, matchedFilesSet);
        return;
      }
    } else {
      data = DataTypeUtil.getMeasureObjectFromDataType(fieldValue, joinDataType);
    }

    if (comparator.compare(data, root.getMax()) <= 0
        && comparator.compare(data, root.getMin()) >= 0) {
      matchedFilesSet.addAll(root.getFilePaths());
    }

    if (root.getLeftSubTree() != null && comparator.compare(data, root.getLeftSubTreeMax()) <= 0
        && comparator.compare(data, root.getLeftSubTreeMin()) >= 0) {
      getMatchingFiles(root.getLeftSubTree(), fieldValue, matchedFilesSet);
    }

    if (root.getRightSubTree() != null && comparator.compare(data, root.getRightSubTreeMax()) <= 0
        && comparator.compare(data, root.getRightSubTreeMin()) >= 0) {
      getMatchingFiles(root.getRightSubTree(), fieldValue, matchedFilesSet);
    }
  }

  private void getMatchingFilesForString(MinMaxNode root, byte[] fieldValue,
      Set<String> matchedFilesSet) {
    if (root == null) {
      return;
    }
    if (ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(fieldValue, root.getMin().toString().getBytes(Charset.forName(DEFAULT_CHARSET)))
        >= 0 && ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(fieldValue, root.getMax().toString().getBytes(Charset.forName(DEFAULT_CHARSET)))
        <= 0) {
      matchedFilesSet.addAll(root.getFilePaths());
    }
    if (root.getLeftSubTree() != null && ByteUtil.UnsafeComparer.INSTANCE.compareTo(fieldValue,
        root.getLeftSubTreeMin().toString().getBytes(Charset.forName(DEFAULT_CHARSET))) >= 0 &&
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(fieldValue,
            root.getLeftSubTreeMax().toString().getBytes(Charset.forName(DEFAULT_CHARSET))) <= 0) {
      getMatchingFilesForString(root.getLeftSubTree(), fieldValue, matchedFilesSet);
    }

    if (root.getRightSubTree() != null && ByteUtil.UnsafeComparer.INSTANCE.compareTo(fieldValue,
        root.getRightSubTreeMin().toString().getBytes(Charset.forName(DEFAULT_CHARSET))) >= 0 &&
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(fieldValue,
            root.getRightSubTreeMax().toString().getBytes(Charset.forName(DEFAULT_CHARSET))) <= 0) {
      getMatchingFilesForString(root.getRightSubTree(), fieldValue, matchedFilesSet);
    }
  }
}
