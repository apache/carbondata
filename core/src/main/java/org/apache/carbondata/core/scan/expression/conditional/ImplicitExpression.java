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

package org.apache.carbondata.core.scan.expression.conditional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Custom class to handle filter values for Implicit filter
 */
public class ImplicitExpression extends Expression {

  /**
   * Logger instance
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ImplicitExpression.class.getName());

  /**
   * map that contains the mapping of block id to the valid blocklets in that block which contain
   * the data as per the applied filter
   */
  private final Map<String, Set<String>> blockIdToBlockletIdMapping;

  /**
   * checks if implicit filter exceeds complex filter threshold
   */
  private boolean isComplexThresholdReached;

  public ImplicitExpression(List<Expression> implicitFilterList, boolean isTupleIdTillRowLevel) {
    // initialize map with half the size of filter list as one block id can contain
    // multiple blocklets
    blockIdToBlockletIdMapping = new HashMap<>(implicitFilterList.size() / 2);
    for (Expression value : implicitFilterList) {
      String blockletPath = ((LiteralExpression) value).getLiteralExpValue().toString();
      addBlockEntry(blockletPath, isTupleIdTillRowLevel);
    }
    int complexFilterThreshold = CarbonProperties.getInstance().getComplexFilterThresholdForSI();
    isComplexThresholdReached = implicitFilterList.size() > complexFilterThreshold;
    if (isComplexThresholdReached) {
      LOGGER.info("Implicit Filter Size: " + implicitFilterList.size() + ", Threshold is: "
          + complexFilterThreshold);
    }
  }

  public ImplicitExpression(Map<String, Set<String>> blockIdToBlockletIdMapping) {
    this.blockIdToBlockletIdMapping = blockIdToBlockletIdMapping;
  }

  private void addBlockEntry(String blockletPath, boolean isTupleIdTillRowLevel) {
    String[] blockletPathSplits = blockletPath.split(CarbonCommonConstants.FILE_SEPARATOR);
    String blockId =
        blockletPathSplits[0] + CarbonCommonConstants.FILE_SEPARATOR + blockletPathSplits[1];
    Set<String> blockletIds =
        blockIdToBlockletIdMapping.computeIfAbsent(blockId, k -> new HashSet<>());
    if (isTupleIdTillRowLevel) {
      // set row id's instead of blocklet id
      // case 1: if filter contains only complex filter
      if (blockletPathSplits.length > 3 && !isComplexThresholdReached) {
        blockletIds.add(
            blockletPathSplits[2] + CarbonCommonConstants.FILE_SEPARATOR + blockletPathSplits[3]
                + CarbonCommonConstants.FILE_SEPARATOR + blockletPathSplits[4]);
      } else {
        // case 2: if filter contains complex and primitive filter
        blockletIds.add(blockletPathSplits[2]);
      }
    } else {
      blockId =
          blockletPath.substring(0, blockletPath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      blockletIds = blockIdToBlockletIdMapping.computeIfAbsent(blockId, k -> new HashSet<>());
      blockletIds.add(blockletPath.substring(blockId.length() + 1));
    }
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) {
    throw new UnsupportedOperationException("Operation not supported for Implicit expression");
  }

  public Map<String, Set<String>> getBlockIdToBlockletIdMapping() {
    return blockIdToBlockletIdMapping;
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.IMPLICIT;
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

  @Override
  public String getString() {
    StringBuilder value = new StringBuilder();
    value.append("ImplicitExpression(");
    for (Map.Entry<String, Set<String>> entry : blockIdToBlockletIdMapping.entrySet()) {
      value.append(entry.getKey()).append(" --> ");
      value.append(
          StringUtils.join(entry.getValue().toArray(new String[entry.getValue().size()]), ","))
          .append(";");
      // return maximum of 100 characters in the getString method
      if (value.length() > 100) {
        value.append("...");
        break;
      }
    }
    value.append(')');
    return value.toString();
  }

  @Override
  public String getStatement() {
    return getString();
  }
}