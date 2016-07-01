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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

abstract class AbstractNode

case class Node(cd: CarbonDictionaryTempDecoder) extends AbstractNode

case class JoinNode(left: util.List[AbstractNode], right: util.List[AbstractNode])
  extends AbstractNode

case class CarbonDictionaryTempDecoder(
    attrList: util.Set[Attribute],
    attrsNotDecode: util.Set[Attribute],
    child: LogicalPlan,
    isOuter: Boolean = false) extends UnaryNode {
  var processed = false

  override def output: Seq[Attribute] = child.output
}

class CarbonDecoderProcessor {

  def getDecoderList(plan: LogicalPlan): util.List[AbstractNode] = {
    val nodeList = new util.ArrayList[AbstractNode]
    process(plan, nodeList)
    nodeList
  }

  private def process(plan: LogicalPlan, nodeList: util.List[AbstractNode]): Unit = {
    plan match {
      case cd: CarbonDictionaryTempDecoder =>
        nodeList.add(Node(cd))
        process(cd.child, nodeList)
      case j: BinaryNode =>
        val leftList = new util.ArrayList[AbstractNode]
        val rightList = new util.ArrayList[AbstractNode]
        nodeList.add(JoinNode(leftList, rightList))
        process(j.left, leftList)
        process(j.right, rightList)
      case e: UnaryNode => process(e.child, nodeList)
      case _ =>
    }
  }

  def updateDecoders(nodeList: util.List[AbstractNode]): Unit = {
    val scalaList = nodeList.asScala
    val decoderNotDecode = new util.HashSet[Attribute]
    updateDecoderInternal(scalaList, decoderNotDecode)
  }

  private def updateDecoderInternal(scalaList: mutable.Buffer[AbstractNode],
      decoderNotDecode: util.HashSet[Attribute]): Unit = {
    scalaList.reverseMap {
      case Node(cd: CarbonDictionaryTempDecoder) =>
        decoderNotDecode.asScala.foreach(cd.attrsNotDecode.add)
        decoderNotDecode.asScala.foreach(cd.attrList.remove)
        decoderNotDecode.addAll(cd.attrList)
      case JoinNode(left: util.List[AbstractNode], right: util.List[AbstractNode]) =>
        val leftNotDecode = new util.HashSet[Attribute]
        val rightNotDecode = new util.HashSet[Attribute]
        updateDecoderInternal(left.asScala, leftNotDecode)
        updateDecoderInternal(right.asScala, rightNotDecode)
        decoderNotDecode.addAll(leftNotDecode)
        decoderNotDecode.addAll(rightNotDecode)
    }
  }

}

case class Marker(set: util.Set[Attribute], join: Boolean = false)

class CarbonPlanMarker {
  val markerStack = new util.Stack[Marker]
  var joinCount = 0

  def pushMarker(attrs: util.Set[Attribute]): Unit = {
    markerStack.push(Marker(attrs))
  }

  def pushJoinMarker(attrs: util.Set[Attribute]): Unit = {
    markerStack.push(Marker(attrs, join = true))
    joinCount = joinCount + 1
  }

  def revokeJoin(): util.Set[Attribute] = {
    if (joinCount > 0) {
      while (!markerStack.empty()) {
        val marker = markerStack.pop()
        if (marker.join) {
          joinCount = joinCount - 1
          return marker.set
        }
      }
    }
    markerStack.peek().set
  }

}


