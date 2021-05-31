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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

/**
 * This expression will be added to Index filter when CDC pruning is enabled.
 */
public class CDCBlockImplicitExpression extends Expression {

  Set<String> blocksToScan;

  public CDCBlockImplicitExpression(String blockPathValues) {
    blocksToScan =
        Arrays.stream(blockPathValues.split(",")).map(String::trim).collect(Collectors.toSet());
  }

  @Override
  public ExpressionResult evaluate(RowIntf value) {
    throw new UnsupportedOperationException("Not allowed on Implicit expression");
  }

  @Override
  public ExpressionType getFilterExpressionType() {
    return ExpressionType.IMPLICIT;
  }

  @Override
  public void findAndSetChild(Expression oldExpr, Expression newExpr) {
    throw new UnsupportedOperationException("Not allowed on Implicit expression");
  }

  @Override
  public String getString() {
    return null;
  }

  @Override
  public String getStatement() {
    return null;
  }

  public Set<String> getBlocksToScan() {
    return blocksToScan;
  }
}
