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

package org.apache.spark.sql.optimizer

import java.util

import org.apache.spark.sql.catalyst.expressions.Attribute

abstract class AbstractNode

case class ArrayCarbonNode(children: Seq[util.List[AbstractNode]])
  extends AbstractNode

case class AttributeReferenceWrapper(attr: Attribute) {

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReferenceWrapper =>
      attr.name.equalsIgnoreCase(ar.attr.name) && attr.exprId == ar.attr.exprId
    case _ => false
  }

  // constant hash value
  lazy val hash = (attr.name.toLowerCase + "." + attr.exprId.id).hashCode
  override def hashCode: Int = hash
}

case class Marker(set: util.Set[AttributeReferenceWrapper], binary: Boolean = false)

class CarbonPlanMarker {
  val markerStack = new util.Stack[Marker]
  var joinCount = 0

  def pushMarker(attrs: util.Set[AttributeReferenceWrapper]): Unit = {
    markerStack.push(Marker(attrs))
  }

  def pushBinaryMarker(attrs: util.Set[AttributeReferenceWrapper]): Unit = {
    markerStack.push(Marker(attrs, binary = true))
    joinCount = joinCount + 1
  }

  def revokeJoin(): util.Set[AttributeReferenceWrapper] = {
    if (joinCount > 0) {
      while (!markerStack.empty()) {
        val marker = markerStack.pop()
        if (marker.binary) {
          joinCount = joinCount - 1
          return marker.set
        }
      }
    }
    if (!markerStack.empty()) {
      markerStack.peek().set
    } else {
      new util.HashSet[AttributeReferenceWrapper]()
    }
  }

}


