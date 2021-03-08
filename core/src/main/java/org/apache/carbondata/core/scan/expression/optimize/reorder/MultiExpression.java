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

package org.apache.carbondata.core.scan.expression.optimize.reorder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;

/**
 * new Expression with multiple children (maybe more than two children).
 */
public abstract class MultiExpression extends StorageOrdinal {

  public MultiExpression() {
    this.minOrdinal = Integer.MAX_VALUE;
  }

  protected List<StorageOrdinal> children = new ArrayList<>();

  public static MultiExpression build(Expression expression) {
    MultiExpression multiExpression = null;
    if (expression instanceof AndExpression) {
      multiExpression = new AndMultiExpression();
    }
    if (expression instanceof OrExpression) {
      multiExpression = new OrMultiExpression();
    }
    if (multiExpression == null) {
      return null;
    }
    for (Expression child : expression.getChildren()) {
      buildChild(child, multiExpression);
    }
    return multiExpression;
  }

  private static void buildChild(Expression expression, MultiExpression parent) {
    if (parent.canMerge(expression)) {
      // multiple and(or) can be merge into same MultiExpression
      for (Expression child : expression.getChildren()) {
        buildChild(child, parent);
      }
    } else {
      MultiExpression multiExpression = build(expression);
      if (multiExpression == null) {
        // it is not and/or expression
        parent.addChild(expression);
      } else {
        // it is and, or expression
        parent.addChild(multiExpression);
      }
    }
  }

  public abstract boolean canMerge(Expression child);

  private void addChild(Expression child) {
    addChild(new ExpressionWithOrdinal(child));
  }

  private void addChild(StorageOrdinal storageOrdinal) {
    children.add(storageOrdinal);
  }

  private Map<String, Integer> columnMapOrdinal(CarbonTable table) {
    List<CarbonColumn> createOrderColumns = table.getCreateOrderColumn();
    Map<String, Integer> nameMapOrdinal = new HashMap<>(createOrderColumns.size());
    int dimensionCount = table.getAllDimensions().size();
    for (CarbonColumn column : createOrderColumns) {
      if (column.isDimension()) {
        nameMapOrdinal.put(column.getColName(), column.getOrdinal());
      } else {
        nameMapOrdinal.put(column.getColName(), dimensionCount + column.getOrdinal());
      }
    }
    return nameMapOrdinal;
  }

  public void removeRedundant() {
    // TODO remove redundancy filter if exists
  }

  public void combine() {
    // TODO combine multiple filters to single filter if needed
  }

  public void reorder(CarbonTable table) {
    updateMinOrdinal(columnMapOrdinal(table));
    sortChildrenByOrdinal();
  }

  public void sortChildrenByOrdinal() {
    children.sort(null);
    for (StorageOrdinal child : children) {
      if (child instanceof MultiExpression) {
        ((MultiExpression) child).sortChildrenByOrdinal();
      }
    }
  }

  @Override
  public void updateMinOrdinal(Map<String, Integer> columnMapOrdinal) {
    for (StorageOrdinal child : children) {
      child.updateMinOrdinal(columnMapOrdinal);
      if (child.minOrdinal < this.minOrdinal) {
        this.minOrdinal = child.minOrdinal;
      }
    }
  }
}
