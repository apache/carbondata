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

import org.apache.carbondata.spark.CarbonAliasDecoderRelation

abstract class AbstractNode

case class Node(cd: CarbonDictionaryTempDecoder) extends AbstractNode

case class ArrayCarbonNode(children: Seq[util.List[AbstractNode]])
  extends AbstractNode

case class CarbonDictionaryTempDecoder(
    attrList: util.Set[AttributeReferenceWrapper],
    attrsNotDecode: util.Set[AttributeReferenceWrapper],
    child: LogicalPlan,
    isOuter: Boolean = false,
    aliasMap: Option[CarbonAliasDecoderRelation] = None) extends UnaryNode {
  var processed = false

  def getAttrsNotDecode: util.Set[Attribute] = {
    val set = new util.HashSet[Attribute]()
    attrsNotDecode.asScala.foreach(f => set.add(f.attr))
    set
  }

  def getAttrList: util.Set[Attribute] = {
    val set = new util.HashSet[Attribute]()
    attrList.asScala.foreach(f => set.add(f.attr))
    set
  }

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
        nodeList.add(ArrayCarbonNode(Seq(leftList, rightList)))
        process(j.left, leftList)
        process(j.right, rightList)
      case u: Union =>
        val nodeListSeq = u.children.map { child =>
          val list = new util.ArrayList[AbstractNode]
          process(child, list)
          list
        }
        nodeList.add(ArrayCarbonNode(nodeListSeq))
      case e: UnaryNode => process(e.child, nodeList)
      case i: InsertIntoTable => process(i.child, nodeList)
      case _ =>
    }
  }

  def updateDecoders(nodeList: util.List[AbstractNode]): Unit = {
    val scalaList = nodeList.asScala
    val decoderNotDecode = new util.HashSet[AttributeReferenceWrapper]
    updateDecoderInternal(scalaList, decoderNotDecode)
  }

  private def updateDecoderInternal(scalaList: mutable.Buffer[AbstractNode],
      decoderNotDecode: util.HashSet[AttributeReferenceWrapper]): Unit = {
    scalaList.reverseMap {
      case Node(cd: CarbonDictionaryTempDecoder) =>
        decoderNotDecode.asScala.foreach(cd.attrsNotDecode.add)
        decoderNotDecode.asScala.foreach(cd.attrList.remove)
        decoderNotDecode.addAll(cd.attrList)
      case ArrayCarbonNode(children) =>
        children.foreach { child =>
          val notDecode = new util.HashSet[AttributeReferenceWrapper]
          updateDecoderInternal(child.asScala, notDecode)
          decoderNotDecode.addAll(notDecode)
        }
    }
  }

}

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


