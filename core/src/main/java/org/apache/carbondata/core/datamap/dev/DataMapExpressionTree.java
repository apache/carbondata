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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import static org.apache.carbondata.core.indexstore.schema.FilterType.EQUALTO;
import static org.apache.carbondata.core.indexstore.schema.FilterType.GREATER_THAN;
import static org.apache.carbondata.core.indexstore.schema.FilterType.GREATER_THAN_EQUAL;
import static org.apache.carbondata.core.indexstore.schema.FilterType.LESS_THAN;
import static org.apache.carbondata.core.indexstore.schema.FilterType.LESS_THAN_EQUAL;
import static org.apache.carbondata.core.indexstore.schema.FilterType.LIKE;

public class DataMapExpressionTree {

  private DataMapCoveringOptions.DataMapCovered coverage;

  private Map<DataMapFactory, List<DataMapColumnExpression>> dataMapFactoryListMap;

  public DataMapExpressionTree(DataMapCoveringOptions.DataMapCovered coverage,
      Map<DataMapFactory, List<DataMapColumnExpression>> dataMapFactoryListMap) {
    this.coverage = coverage;
    this.dataMapFactoryListMap = dataMapFactoryListMap;
  }

  public DataMapExpression dataMapExpressionTreeFormation () {
    DataMapAndNodeExpression prevAndExpression = null;
    DataMapExpression rootAndExpression = null;
    DataMapColumnNodeExpression dataMapColumnNodeExpression = null;
    List<DataMapColumnNodeExpression> dataMapColumnNodeExpressionList = new ArrayList<>();
    // Form the Datamap expression tree based on coverage and dataMapFactoryListMap.

    /**
     * The DataMap Tree formation is somewhat like This.
     *
     *                   DataMapNode
     *                      /      \
     *                     /        \
     *               FilterNode    FilterNode
     *                   ?  \          /       \
     *                  /   \         /         \
     *         ColumnNode   Literal  ColumnNode  Literal.
     *
     *
     *                       AndNode
     *                         /  \
     *                        /    \
     *                       /      \
     *                DataMapNode   DataMapNode
     *                    /            \
     *                   /              \
     *              FilterNode         FilterNode
     *                 /    \            /     \
     *               /       \          /       \
     *         ColumnNode   Literal  ColumnNode Literal
     */

    // Major points.
    // Multiple DataMaps can be linked with And Node.
    // DataMapNode can have multiple children and can have FilterNode as a child or
    // multiple filterNodes depending on the number of operators being linked to
    // each DataMap.
    if (coverage.equals(DataMapCoveringOptions.DataMapCovered.FULLY_COVERED)) {
      // There will a single DataMap.
      for (Map.Entry<DataMapFactory, List<DataMapColumnExpression>> dataMapFactoryListEntry :
          dataMapFactoryListMap.entrySet()) {
        List<DataMapColumnExpression> dataMapColumnExpressionList =
            dataMapFactoryListEntry.getValue();
        List<DataMapFilterNode> filterNodes = new ArrayList<>(dataMapColumnExpressionList.size());
        for (DataMapColumnExpression dataMapExpression1 : dataMapColumnExpressionList) {
          if (dataMapExpression1.getFilterType().equals(EQUALTO)) {
            DataMapFilterNode equalFilterNode = new DataMapEqualNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(equalFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(GREATER_THAN)) {
            DataMapFilterNode greaterThanFilterNode = new DataMapGreaterThanNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(greaterThanFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(GREATER_THAN_EQUAL)) {
            DataMapFilterNode greaterThanEqualFilterNode =
                new DataMapGreaterThanEqualNodeExpression(
                    new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                    dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                    dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(greaterThanEqualFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LESS_THAN)) {
            DataMapFilterNode lessThanFilterNode = new DataMapLessThanNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(lessThanFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LESS_THAN_EQUAL)) {
            DataMapFilterNode lessThanEqualFilterNode = new DataMapLessThanEqualNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(lessThanEqualFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LIKE)) {
            DataMapFilterNode likeFilterNode = new DataMapLikeNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(likeFilterNode);
          }
        }
        dataMapColumnNodeExpression = new DataMapColumnNodeExpression(filterNodes);
      }
      rootAndExpression = dataMapColumnNodeExpression;
      return rootAndExpression;
    } else if (coverage.equals(DataMapCoveringOptions.DataMapCovered.PARTIALLY_COVERED)) {
      for (Map.Entry<DataMapFactory, List<DataMapColumnExpression>> dataMapFactoryListEntry :
          dataMapFactoryListMap.entrySet()) {
        List<DataMapColumnExpression> dataMapColumnExpressionList =
            dataMapFactoryListEntry.getValue();
        List<DataMapFilterNode> filterNodes = new ArrayList<>(dataMapColumnExpressionList.size());
        for (DataMapColumnExpression dataMapExpression1 : dataMapColumnExpressionList) {
          if (dataMapExpression1.getFilterType().equals(EQUALTO)) {
            DataMapFilterNode equalFilterNode = new DataMapEqualNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(equalFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(GREATER_THAN)) {
            DataMapFilterNode greaterThanFilterNode = new DataMapGreaterThanNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(greaterThanFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(GREATER_THAN_EQUAL)) {
            DataMapFilterNode greaterThanEqualFilterNode =
                new DataMapGreaterThanEqualNodeExpression(
                    new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                    dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                    dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(greaterThanEqualFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LESS_THAN)) {
            DataMapFilterNode lessThanFilterNode = new DataMapLessThanNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(lessThanFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LESS_THAN_EQUAL)) {
            DataMapFilterNode lessThanEqualFilterNode = new DataMapLessThanEqualNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(lessThanEqualFilterNode);
          } else if (dataMapExpression1.getFilterType().equals(LIKE)) {
            DataMapFilterNode likeFilterNode = new DataMapLikeNodeExpression(
                new DataMapColumnNameNodeExpression(dataMapExpression1.getColumnName()), null,
                dataMapExpression1.getExpr(), dataMapFactoryListEntry.getKey(),
                dataMapFactoryListEntry.getKey().getMeta());
            filterNodes.add(likeFilterNode);
          }
        }
        dataMapColumnNodeExpression = new DataMapColumnNodeExpression(filterNodes);
        dataMapColumnNodeExpressionList.add(dataMapColumnNodeExpression);
      }

      // For a Tree with AND
      if (dataMapColumnNodeExpressionList.size() > 1) {
        int index = 0;
        List<DataMapExpression> finalList = new ArrayList<>();
        for (index = 0; index < dataMapColumnNodeExpressionList.size() - 1; index++) {
          finalList.add(new DataMapAndNodeExpression(null, null));
          finalList.add(dataMapColumnNodeExpressionList.get(index));
        }
        while (index < dataMapColumnNodeExpressionList.size()) {
          finalList.add(dataMapColumnNodeExpressionList.get(index));
        }

        // form the tree.
        rootAndExpression = finalList.get(0);
        for (index = 0; index < finalList.size(); index++) {
          if (finalList.get(index) instanceof DataMapAndNodeExpression) {
            DataMapAndNodeExpression andNodeExpression =
                (DataMapAndNodeExpression) finalList.get(index);
            andNodeExpression.setLeft(finalList.get(++index));
            andNodeExpression.setRight(finalList.get(++index));
          }
        }
      } else {
        rootAndExpression = dataMapColumnNodeExpressionList.get(0);
      }
      return rootAndExpression;
    }
    return null;
  }
//
//  public TableDataMap extractDataMapsFromExpressionTree(DataMapExpression dataMapExpression) {
//    TableDataMap tableDataMap = new TableDataMap();
//
//  }
}

