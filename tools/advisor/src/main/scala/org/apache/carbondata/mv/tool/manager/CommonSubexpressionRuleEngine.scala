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

package org.apache.carbondata.mv.tool.manager

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNode

abstract class CoveringRule[ToolPlan <: TreeNode[ToolPlan]] extends Logging {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan_frequency_pairs: Seq[(ToolPlan, Int)]): Seq[(ToolPlan, Int)]
}

/**
 * Provides a covering subexpression manager, which finds expressions for summary data sets.
 */
abstract class CommonSubexpressionRuleEngine[ToolPlan <: TreeNode[ToolPlan]] {

  /** Defines a sequence of rules, to be overridden by the implementation. */
  protected val rules: Seq[CoveringRule[ToolPlan]]

  def execute(cses: Seq[(ToolPlan, Int)]): Seq[(ToolPlan, Int)] =
    rules.foldLeft(cses) {case (foldedCSEs, rule) => rule(foldedCSEs)}
}
