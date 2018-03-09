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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.{ModularPlan, ModularRelation}
import org.apache.carbondata.mv.plans.modular.Flags._

case class CostBasedMVRecommendation(spark: SparkSession, conf: SQLConf) extends PredicateHelper {

  lazy val speedUpThreshold: Double =
    conf.getConfString("spark.mv.recommend.speedup.threshold").toDouble

  lazy val sizeReductionThreshold: Double =
    conf.getConfString("spark.mv.recommend.rowcount.threshold").toDouble

  lazy val frequencyThrehold: Int =
    conf.getConfString("spark.mv.recommend.frequency.threshold").toInt

  def findCostEffectiveCandidates(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {

    val sortedMVCandidates = batch.map { candidate =>
      (MVAccessCardinality(candidate._1, getMVAccessCardinality(candidate._1)), candidate._2)
    }.collect {
      case t @ (MVAccessCardinality(_, Some(_)), _) =>
        t
    }.sortBy(_._1.size)(implicitly[Ordering[Option[BigInt]]].reverse)

    val trivialCandidateBufR = mutable.ArrayBuffer[(MVAccessCardinality, Int)]()
    sortedMVCandidates.foreach { trivialCandidateBufR += _ }

    val trivialCandidateBufM = mutable.ArrayBuffer[(MVAccessCardinality, Int)]()
    val candidateSet = mutable.ArrayBuffer[(MVAccessCardinality, Int)]()

    while (trivialCandidateBufR.nonEmpty) {
      var candidateR = trivialCandidateBufR.remove(0)
      trivialCandidateBufM.clear
      sortedMVCandidates.foreach{trivialCandidateBufM += _}
      trivialCandidateBufM -= candidateR
      var isCandidate = false
      var done = false
      var isFirst = true
      while (trivialCandidateBufM.nonEmpty && !done) {
        computeCandidateWithMaximalBenefit(candidateR, trivialCandidateBufM) match {
          case Some(bestCandidateWithBenefit) =>
            if (bestCandidateWithBenefit._1 > speedUpThreshold) {
              val result = ModularPlanUtility.combinePlans(
                candidateR._1.plan, bestCandidateWithBenefit._2._1.plan, isFirst)
              isFirst = false
              (result._1, result._2) match {
                case (true, Some(combinedPlan)) =>
                  val candidateR1 =
                    (MVAccessCardinality(combinedPlan, getMVAccessCardinality(combinedPlan)),
                      candidateR._2 + bestCandidateWithBenefit._2._2)
                  if (notEnoughSpeedUp(candidateR1._1)) {
                    trivialCandidateBufM -= bestCandidateWithBenefit._2
                    done = true
                  } else {
                    isCandidate = true
                    trivialCandidateBufM -= bestCandidateWithBenefit._2
                    trivialCandidateBufR -= bestCandidateWithBenefit._2
                    candidateR = candidateR1
                  }
                case _ =>
                  trivialCandidateBufM -= bestCandidateWithBenefit._2
              }
            } else done = true
          case _ => done = true
        }
      }
      if (isCandidate) {
        candidateSet += candidateR
      } else if (candidateR._2 >= frequencyThrehold) {
        val result = ModularPlanUtility.combinePlans(
          candidateR._1.plan, candidateR._1.plan, isFirst = true)
        candidateSet += ((MVAccessCardinality(result._2.get, candidateR._1.size), candidateR._2))
      }
    }

    collection.immutable.Seq(candidateSet: _*).map { member => (member._1.plan, member._2) }
  }

  private def computeCandidateWithMaximalBenefit(
      rSubplan: (MVAccessCardinality, Int),
      mCandidates: ArrayBuffer[(MVAccessCardinality, Int)]
  ): Option[(Float, (MVAccessCardinality, Int))] = {
    val fNumOfRows = rSubplan._1.plan.asInstanceOf[modular.GroupBy].child.children.head
      .asInstanceOf[ModularRelation].stats(spark, conf).rowCount
    val rNumOfRows = rSubplan._1.size

    (fNumOfRows, rNumOfRows)  match {
      case (Some(f), Some(r)) =>
        var speedupFinal = 0.0
        var candidateFinal = None: Option[(MVAccessCardinality, Int)]
        for (candidate <- mCandidates) {
          val cNumOfRows = candidate._1.size

          cNumOfRows match {
            case Some(c) =>
              // speedup = percentage of I/O savings
              val speedupR = 1.0 - r.toDouble / f.toDouble
              val speedupC = 1.0 - c.toDouble / f.toDouble
              val speedupNew =
                (rSubplan._2 * speedupR + candidate._2 * speedupC) / (rSubplan._2 + candidate._2)
              if (speedupNew.toDouble > speedupFinal) {
                speedupFinal = speedupNew.toDouble
                candidateFinal = Some(candidate)
              }
            case _ =>
          }
        }

        if (speedupFinal > speedUpThreshold) {
          candidateFinal match {
            case Some(candidatefinal) => Some((speedupFinal.toFloat, candidatefinal))
            case _ => None
          }
        }
        else None

      case _ => None
    }
  }

  def notEnoughSpeedUp(candidate: MVAccessCardinality): Boolean = {
    val rNumOfRows = candidate.size
    val sNumOfRows = candidate.plan.asInstanceOf[modular.GroupBy].child.children.head
      .asInstanceOf[ModularRelation].stats(spark, conf).rowCount
    (rNumOfRows, sNumOfRows) match {
      case (Some(r), Some(s)) => if (r.toDouble >
                                     sizeReductionThreshold * s.toDouble) {
        true
      } else {
        false
      }
      case _ => false
    }
  }

  /**
   * Helper case class to hold (plan, rowCount) pairs.
   */
  private case class MVAccessCardinality(plan: ModularPlan, size: Option[BigInt])

  /**
   * Returns the cardinality of a base table access. A base table access represents
   * a LeafNode, or Project or Filter operators above a LeafNode.
   */
  private def getMVAccessCardinality(
      input: ModularPlan): Option[BigInt] = {
    input match {
      case g@modular.GroupBy(
      _, _, _, _,
      modular.Select(_, _, _, _, _, children, NoFlags, Nil, Nil), NoFlags, Nil) =>
        val statsList = children.map(_.stats(spark, conf))
        val ndvs = g.predicateList.flatMap {
          case subexp@Substring(col: AttributeReference, pos: Literal, len: Literal) if
          pos.dataType == IntegerType && len.dataType == IntegerType =>
            import scala.math.min
            statsList.find(_.attributeStats.contains(col))
              .flatMap(_.attributeStats.get(col))
              .map(colstats => BigInt((colstats.distinctCount.toDouble * min(
                (len.value.asInstanceOf[Integer].toFloat / colstats.maxLen.toFloat).toDouble, 1.0))
                .toLong))

          case col: AttributeReference =>
            statsList.find(_.attributeStats.contains(col))
              .flatMap(_.attributeStats.get(col))
              .map(_.distinctCount)
          case other => None
        }
        val rowCount = ndvs match {
          case Nil => None
          case xs =>
            Some(xs.max)
        }
        rowCount
      case _ => None
    }
  }

  /**
   * Helper object that has all related modular plan utilities
   */
  private object ModularPlanUtility extends ModularPlanUtility

  private class ModularPlanUtility extends CreateCandidateCSEs(spark, conf)

}
