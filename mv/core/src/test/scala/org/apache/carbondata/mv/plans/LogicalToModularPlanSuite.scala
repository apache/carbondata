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

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{LeftOuter, RightOuter, _}

import org.apache.carbondata.mv.dsl.Plans._
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.modular.{JoinEdge, ModularRelation}
import org.apache.carbondata.mv.testutil.ModularPlanTest

class LogicalToModularPlanSuite extends ModularPlanTest {

  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int)
  
  val testRelation1 = LocalRelation('d.int)
  
  val testRelation2 = LocalRelation('c.int, 'd.int)

  test("select only") {
    val originalQuery =
      testRelation0
        .select('a.attr)
        .analyze
    val modularized = originalQuery.modularize 
    val correctAnswer = originalQuery match {
      case logical.Project(proj,MatchLocalRelation(tbl,_)) =>
        ModularRelation(null,null,tbl,NoFlags,Seq.empty).select(proj:_*)(tbl:_*)()(Map.empty)()
    }
    comparePlans(modularized, correctAnswer)
  }
  
  test("select-project-groupby grouping without aggregate function") {
    val originalQuery =
      testRelation0
        .select('a)
        .groupBy('a)('a)
        .select('a).analyze
        
    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj1,logical.Aggregate(grp,agg,logical.Project(proj2,MatchLocalRelation(tbl,_)))) =>
        ModularRelation(null,null,tbl,NoFlags,Seq.empty).select(proj2:_*)(tbl:_*)()(Map.empty)().groupBy(agg:_*)(proj2:_*)(grp:_*).select(proj1:_*)(proj1:_*)()(Map.empty)() 
    }   
    comparePlans(modularized, correctAnswer)
  }
  
  test("select-project with filter") {
    val originalQuery =
      testRelation0
        .where('a + 'b === 1)
        .select('a + 'b as 'e)
        .analyze
        
    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj,logical.Filter(cond,MatchLocalRelation(tbl,_))) =>
        ModularRelation(null,null,tbl,NoFlags,Seq.empty).select(proj:_*)(tbl:_*)(cond)(Map.empty)()  
    }
    comparePlans(modularized, correctAnswer)
  }
  
  test("join") {
    val left = testRelation0.where('a === 1)
    val right = testRelation1
    val originalQuery =
      left.join(right, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze
      
    val modularized = originalQuery.modularize 
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,MatchLocalRelation(tbl1,_)),MatchLocalRelation(tbl2,_),Inner,Some(cond2)) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty)).select(tbl1++tbl2:_*)(tbl1++tbl2:_*)(Seq(cond1,cond2):_*)(Map.empty)(JoinEdge(0,1,Inner)) 
    } 
    comparePlans(modularized, correctAnswer)
  }
  
  test("left outer join") {
    val left = testRelation0.where('a === 1)
    val right = testRelation1
    val originalQuery =
      left.join(right, LeftOuter, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze
      
    val modularized = originalQuery.modularize 
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,MatchLocalRelation(tbl1,_)),MatchLocalRelation(tbl2,_),LeftOuter,Some(cond2)) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty)).select(tbl1++tbl2:_*)(tbl1++tbl2:_*)(Seq(cond1,cond2):_*)(Map.empty)(JoinEdge(0,1,LeftOuter)) 
    } 
    comparePlans(modularized, correctAnswer)
  }
  
  test("right outer join") {
    val left = testRelation0.where('a === 1)
    val right = testRelation1
    val originalQuery =
      left.join(right, RightOuter, condition = Some("d".attr === "b".attr || "d".attr === "c".attr)).analyze
      
    val modularized = originalQuery.modularize 
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,MatchLocalRelation(tbl1,_)),MatchLocalRelation(tbl2,_),RightOuter,Some(cond2)) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty)).select(tbl1++tbl2:_*)(tbl1++tbl2:_*)(Seq(cond1,cond2):_*)(Map.empty)(JoinEdge(0,1,RightOuter))  
    } 
    comparePlans(modularized, correctAnswer)
  }
  
  ignore("joins: conjunctive predicates #1 with alias") {
    val left = testRelation0.where('a === 1).subquery('x)
    val right = testRelation1.subquery('y)
    val originalQuery =
      left.join(right, condition = Some("x.b".attr === "y.d".attr)).analyze
    
    val modularized = analysis.EliminateSubqueryAliases(originalQuery).modularize
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,MatchLocalRelation(tbl1,_)),MatchLocalRelation(tbl2,_),Inner,Some(cond2)) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty)).select(tbl1++tbl2:_*)(tbl1++tbl2:_*)(Seq(cond1,cond2):_*)(Map.empty)(JoinEdge(0,1,Inner))  
    } 
    comparePlans(modularized, correctAnswer)
  }
  
  ignore("joins: conjunctive predicates #2 with alias") {
    val lleft = testRelation0.where('a >= 3).subquery('z)
    val left = testRelation0.where('a === 1).subquery('x)
    val right = testRelation0.subquery('y)
    val originalQuery =
      lleft.join(
        left.join(right, condition = Some("x.b".attr === "y.b".attr)),
          condition = Some("z.a".attr === "x.b".attr))
        .analyze
        
    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Join(logical.Filter(cond1,MatchLocalRelation(tbl1,_)),logical.Join(logical.Filter(cond2,MatchLocalRelation(tbl2,_)),MatchLocalRelation(tbl3,_),Inner,Some(cond3)),Inner,Some(cond4)) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty),ModularRelation(null,null,tbl3,NoFlags,Seq.empty)).select(tbl1++tbl2++tbl3:_*)(tbl1++tbl2++tbl3:_*)(Seq(cond1,cond2,cond3,cond4):_*)(Map.empty)(JoinEdge(0,1,Inner),JoinEdge(1,2,Inner))  
    } 
    comparePlans(modularized, correctAnswer)
  }
  
  ignore("SPJGH query") {
    val left = testRelation0.where('b >= 1).subquery('x)
    val right = testRelation2.where('d >= 2).subquery('y)
    
    val originalQuery =
      left.join(right, Inner, Option("x.c".attr ==="y.c".attr))
        .groupBy("x.a".attr)("x.a".attr as 'f, Count("x.b") as 'g)
        .select('f)
        .where('g > 1).analyze
        
    val modularized = originalQuery.modularize
    val correctAnswer = originalQuery match {
      case logical.Project(proj0, logical.Filter(cond1, logical.Project(proj1, logical.Aggregate(grp,agg,logical.Join(logical.Filter(cond2,MatchLocalRelation(tbl1,_)),logical.Filter(cond3,MatchLocalRelation(tbl2,_)),Inner,Some(cond4)))))) =>
        Seq(ModularRelation(null,null,tbl1,NoFlags,Seq.empty),ModularRelation(null,null,tbl2,NoFlags,Seq.empty)).select(tbl1++tbl2:_*)(tbl1++tbl2:_*)(Seq(cond2,cond3,cond4):_*)(Map.empty)(JoinEdge(0,1,Inner)).groupBy(agg:_*)(tbl1++tbl2:_*)(grp:_*).select(proj0:_*)(proj1:_*)(cond1)(Map.empty)()  
    }   
    comparePlans(modularized, correctAnswer)
  }
  
  ignore("non-SPJGH query") {
    val mqoAnswer = try testRelation0.where('b > 2).select('a).orderBy('a.asc).analyze.modularize catch {
      case e: Exception =>
        s"""
          |Exception thrown while modularizing query:
          |== Exception ==
          |$e
        """.stripMargin.trim
    }
    val correctAnswer =
      s"""
        |Exception thrown while modularizing query:
        |== Exception ==
        |java.lang.UnsupportedOperationException: unsupported operation: No modular plan for 
        |Sort [a#0 ASC NULLS FIRST], true
        |+- Project [a#0]
        |   +- Filter (b#1 > 2)
        |      +- LocalRelation <empty>, [a#0, b#1, c#2]  
      """.stripMargin.trim
    compareMessages(mqoAnswer.toString,correctAnswer)
  }
}