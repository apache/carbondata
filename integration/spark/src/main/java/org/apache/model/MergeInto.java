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

package org.apache.model;

import java.util.List;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.command.mutation.merge.MergeAction;

public class MergeInto {
  TmpTable target;
  TmpTable source;
  Expression mergeCondition;
  List<Expression> mergeExpressions;
  List<MergeAction> mergeActions;

  public MergeInto(TmpTable target, TmpTable source, Expression mergeCondition,
      List<Expression> mergeExpressions, List<MergeAction> mergeActions) {
    this.target = target;
    this.source = source;
    this.mergeCondition = mergeCondition;
    this.mergeExpressions = mergeExpressions;
    this.mergeActions = mergeActions;
  }

  public TmpTable getTarget() {
    return target;
  }

  public void setTarget(TmpTable target) {
    this.target = target;
  }

  public TmpTable getSource() {
    return source;
  }

  public void setSource(TmpTable source) {
    this.source = source;
  }

  public Expression getMergeCondition() {
    return mergeCondition;
  }

  public void setMergeCondition(Expression mergeCondition) {
    this.mergeCondition = mergeCondition;
  }

  public List<Expression> getMergeExpressions() {
    return mergeExpressions;
  }

  public void setMergeExpressions(List<Expression> mergeExpressions) {
    this.mergeExpressions = mergeExpressions;
  }

  public List<MergeAction> getMergeActions() {
    return mergeActions;
  }

  public void setMergeActions(List<MergeAction> mergeActions) {
    this.mergeActions = mergeActions;
  }
}
