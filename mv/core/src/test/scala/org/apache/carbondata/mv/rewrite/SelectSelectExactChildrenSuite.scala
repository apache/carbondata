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

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

import org.apache.carbondata.mv.testutil.ModularPlanTest

class SelectSelectExactChildrenSuite extends ModularPlanTest { 
  
  object Match extends DefaultMatchMaker {
    val patterns = SelectSelectNoChildDelta :: Nil
  }
  
  val testRelation1 = LocalRelation('tid.int,'fpgid.int,'flid.int,'date.timestamp,'faid.int,'price.double,'qty.int,'disc.string)
  val testRelation2 = LocalRelation('lid.int,'city.string,'state.string,'country.string)
  
//  test("pro-typical lower select") {
//    val fact = testRelation1.subquery('fact)
//    val dim = testRelation2.subquery('dim)
//    
//    val lowerSTSelect =
//      fact
//        .select('faid,'flid,Year('date) as 'year)
//        .analyze
//    val lowerUQSelect =
//      fact.join(dim)
//          .where("fact.flid".attr === "dim.lid".attr && "dim.country".attr === "USA")
//          .select('faid,'flid,Year('date) as 'year, 'state)
//          .analyze
//          
//    val matched = Match.execute(lowerSTSelect.model,lowerUQSelect.model,None).next 
//    
//    val correctAnswer = 
//      lowerSTSelect.join(dim)
//          .where("fact.flid".attr === "dim.lid".attr && "dim.country".attr === "USA") 
//          .select('faid,'flid,Year('date) as 'year, 'state)
//          .analyze.model
//    
//    comparePlans(matched, correctAnswer)
//  }
  
//  val testSummaryDataset =
//    s"""
//       |SELECT faid, flid, year_proj(date) as year, count(*) as cnt
//       |FROM Fact
//       |GROUP BY faid, flid, year_proj(date)
//    """.stripMargin.trim
//      
//  val testUserQuery = 
//    s"""
//       |SELECT faid, state, year_proj(date) as year, count(*) as cnt
//       |FROM Fact
//       |  INNER JOIN Dim 
//       |  ON Fact.flid = Dim.lid AND Dim.country = "USA"
//       |GROUP BY Fact.faid,Fact.state,year_proj(Fact.date)
//       |HAVING count(*) > 2
//    """.stripMargin.trim
    
  
}