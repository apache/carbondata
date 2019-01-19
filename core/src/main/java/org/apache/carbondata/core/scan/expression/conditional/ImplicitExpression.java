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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

import org.apache.commons.lang.StringUtils;

/**
 * Custom class to handle filter values for Implicit filter
 */
public class ImplicitExpression extends Expression {

  /**
   * map that contains the mapping of block id to the valid blocklets in that block which contain
   * the data as per the applied filter
   */
  private Map<String, Set<Integer>> blockIdToBlockletIdMapping;

  public ImplicitExpression(List<Expression> implicitFilterList) {
    // initialize map with half the size of filter list as one block id can contain
    // multiple blocklets
    blockIdToBlockletIdMapping = new HashMap<>(implicitFilterList.size() / 2);
    for (Expression value : implicitFilterList) {
      String blockletPath = ((LiteralExpression) value).getLiteralExpValue().toString();
      addBlockEntry(blockletPath);
    }
  }

  public ImplicitExpression(Map<String, Set<Integer>> blockIdToBlockletIdMapping) {
    this.blockIdToBlockletIdMapping = blockIdToBlockletIdMapping;
  }

  private void addBlockEntry(String blockletPath) {
    String blockId =
        blockletPath.substring(0, blockletPath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
    Set<Integer> blockletIds = blockIdToBlockletIdMapping.get(blockId);
    if (null == blockletIds) {
      blockletIds = new HashSet<>();
      blockIdToBlockletIdMapping.put(blockId, blockletIds);
    }
    blockletIds.add(Integer.parseInt(blockletPath.substring(blockId.length() + 1)));
  }

  @Override public ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException {
    throw new UnsupportedOperationException("Operation not supported for Implicit expression");
  }

  public Map<String, Set<Integer>> getBlockIdToBlockletIdMapping() {
    return blockIdToBlockletIdMapping;
  }

  @Override public ExpressionType getFilterExpressionType() {
    return ExpressionType.IMPLICIT;
  }

  @Override public void findAndSetChild(Expression oldExpr, Expression newExpr) {
  }

  @Override public String getString() {
    StringBuilder value = new StringBuilder();
    value.append("ImplicitExpression(");
    for (Map.Entry<String, Set<Integer>> entry : blockIdToBlockletIdMapping.entrySet()) {
      value.append(entry.getKey()).append(" --> ");
      value.append(
          StringUtils.join(entry.getValue().toArray(new Integer[entry.getValue().size()]), ","))
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

  @Override public String getStatement() {
    return getString();
  }
}