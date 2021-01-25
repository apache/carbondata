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

package org.apache.carbondata.core.index.dev.secondaryindex;

import java.io.Serializable;

import org.apache.carbondata.core.scan.expression.Expression;

public class SIExpressionTree {

  public interface CarbonSIExpression extends Serializable {
  }

  public static class CarbonSIBinaryExpression implements CarbonSIExpression {
    public NodeType nodeType;
    public CarbonSIExpression leftSIExpression;
    public CarbonSIExpression rightSIExpression;

    public CarbonSIBinaryExpression(NodeType nodeType, CarbonSIExpression leftSIExpression,
        CarbonSIExpression righSIExpression) {
      this.nodeType = nodeType;
      this.leftSIExpression = leftSIExpression;
      this.rightSIExpression = righSIExpression;
    }
  }

  public static class CarbonSIUnaryExpression implements CarbonSIExpression {
    public String tableName;
    public Expression expression;

    public CarbonSIUnaryExpression(String tableName, Expression expression) {
      this.tableName = tableName;
      this.expression = expression;
    }
  }

  public enum NodeType {
    Or, And
  }
}
