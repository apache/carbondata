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

package org.apache.carbondata.core.scan.expression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;

public abstract class Expression implements Serializable {

  private static final long serialVersionUID = -7568676723039530713L;
  protected List<Expression> children =
      new ArrayList<Expression>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  public abstract ExpressionResult evaluate(RowIntf value)
      throws FilterUnsupportedException, FilterIllegalMemberException;

  public abstract ExpressionType getFilterExpressionType();

  public List<Expression> getChildren() {
    return children;
  }

  /**
   * This method finds the Expression pointed by oldExpr and replace it with newExpr.
   * @param oldExpr
   * @param newExpr
   */
  public abstract void findAndSetChild(Expression oldExpr, Expression newExpr);

  public abstract String getString();

}
