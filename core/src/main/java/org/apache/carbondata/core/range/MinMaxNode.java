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
import java.util.ArrayList;
import java.util.List;

/**
 * Each node to be inserted in BlockMinMaxTree for pruning.
 */
public class MinMaxNode implements Serializable {

  // list of files present in same range of min max of this node
  private List<String> filePaths = new ArrayList<>();

  private Object min;

  private Object max;

  private MinMaxNode leftSubTree;
  private MinMaxNode rightSubTree;
  private Object leftSubTreeMax;
  private Object leftSubTreeMin;
  private Object rightSubTreeMax;
  private Object rightSubTreeMin;

  public MinMaxNode(String filePaths, Object min, Object max) {
    this.filePaths.add(filePaths);
    this.min = min;
    this.max = max;
  }

  public void addFilePats(List<String> filePaths) {
    this.filePaths.addAll(filePaths);
  }

  public List<String> getFilePaths() {
    return filePaths;
  }

  public void setFilePaths(List<String> filePaths) {
    this.filePaths = filePaths;
  }

  public Object getMin() {
    return min;
  }

  public void setMin(Object min) {
    this.min = min;
  }

  public Object getMax() {
    return max;
  }

  public void setMax(Object max) {
    this.max = max;
  }

  public MinMaxNode getLeftSubTree() {
    return leftSubTree;
  }

  public void setLeftSubTree(MinMaxNode leftSubTree) {
    this.leftSubTree = leftSubTree;
  }

  public MinMaxNode getRightSubTree() {
    return rightSubTree;
  }

  public void setRightSubTree(MinMaxNode rightSubTree) {
    this.rightSubTree = rightSubTree;
  }

  public Object getLeftSubTreeMax() {
    return leftSubTreeMax;
  }

  public void setLeftSubTreeMax(Object leftSubTreeMax) {
    this.leftSubTreeMax = leftSubTreeMax;
  }

  public Object getLeftSubTreeMin() {
    return leftSubTreeMin;
  }

  public void setLeftSubTreeMin(Object leftSubTreeMin) {
    this.leftSubTreeMin = leftSubTreeMin;
  }

  public Object getRightSubTreeMax() {
    return rightSubTreeMax;
  }

  public void setRightSubTreeMax(Object rightSubTreeMax) {
    this.rightSubTreeMax = rightSubTreeMax;
  }

  public Object getRightSubTreeMin() {
    return rightSubTreeMin;
  }

  public void setRightSubTreeMin(Object rightSubTreeMin) {
    this.rightSubTreeMin = rightSubTreeMin;
  }

}
