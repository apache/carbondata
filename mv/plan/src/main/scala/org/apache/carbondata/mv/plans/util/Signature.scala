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

package org.apache.carbondata.mv.plans.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNode

case class Signature(groupby: Boolean = true, datasets: Set[String] = Set.empty)

abstract class SignatureRule[BaseType <: TreeNode[BaseType]] extends Logging {
  def apply(subplan: BaseType, signatures: Seq[Option[Signature]]): Option[Signature]
}

abstract class SignatureGenerator[BaseType <: TreeNode[BaseType]] extends Logging {
  protected val rule: SignatureRule[BaseType]

  def generate(subplan: BaseType): Option[Signature] = {
    generateUp(subplan)
  }

  protected def generateChildren(subplan: BaseType, nextOperation: BaseType => Option[Signature]
  ): Seq[Option[Signature]] = {
    subplan.children.map { child =>
      nextOperation(child.asInstanceOf[BaseType])
    }
  }

  def generateUp(subplan: BaseType): Option[Signature] = {
    val childSignatures = generateChildren(subplan, t => generateUp(t))
    val lastSignature = rule(subplan, childSignatures)
    lastSignature
  }
}

