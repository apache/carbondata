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

package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan}
import org.apache.spark.util.{ScalaCompilerUtil, Utils}

private[sql] class CodeGenerateFactory(version: String) {

  val optimizerFactory = if (version.equals("1.6.2") || version.equals("1.6.3")) {
    ScalaCompilerUtil.compiledCode(CodeTemplates.spark1_6_OptimizerString)
      .asInstanceOf[AbstractCarbonOptimizerFactory]
  } else if (version.startsWith("1.6") || version.startsWith("1.5")) {
    ScalaCompilerUtil.compiledCode(CodeTemplates.defaultOptimizerString)
      .asInstanceOf[AbstractCarbonOptimizerFactory]
  } else {
    throw new UnsupportedOperationException(s"Spark version $version is not supported")
  }

  val expandFactory = if (version.startsWith("1.5")) {
    ScalaCompilerUtil.compiledCode(CodeTemplates.spark1_5ExpandString)
      .asInstanceOf[AbstractCarbonExpandFactory]
  } else if (version.startsWith("1.6")) {
    new AbstractCarbonExpandFactory {
      override def createExpand(expand: Expand, child: LogicalPlan): Expand = {
        val loader = Utils.getContextOrSparkClassLoader
        try {
          val cons = loader.loadClass("org.apache.spark.sql.catalyst.plans.logical.Expand")
            .getDeclaredConstructors
          cons.head.setAccessible(true)
          cons.head.newInstance(expand.projections, expand.output, child).asInstanceOf[Expand]
        } catch {
          case e: Exception => null
        }
      }
    }
  } else {
    throw new UnsupportedOperationException(s"Spark version $version is not supported")
  }

}

object CodeGenerateFactory {

  private var codeGenerateFactory: CodeGenerateFactory = _

  def init(version: String): Unit = {
    if (codeGenerateFactory == null) {
      codeGenerateFactory = new CodeGenerateFactory(version)
    }
  }

  def getInstance(): CodeGenerateFactory = {
    codeGenerateFactory
  }

  def createDefaultOptimizer(conf: CatalystConf, sc: SparkContext): Optimizer = {
    val name = "org.apache.spark.sql.catalyst.optimizer.DefaultOptimizer"
    val loader = Utils.getContextOrSparkClassLoader
    try {
      val cons = loader.loadClass(name + "$").getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance().asInstanceOf[Optimizer]
    } catch {
      case e: Exception =>
        loader.loadClass(name).getConstructor(classOf[CatalystConf])
          .newInstance(conf).asInstanceOf[Optimizer]
    }
  }

}

object CodeTemplates {

  val spark1_6_OptimizerString =
    s"""
       import org.apache.spark.sql._;
       import org.apache.spark.sql.optimizer._;
       import org.apache.spark.sql.catalyst.plans.logical._;
       import org.apache.spark.sql.catalyst._;
       import org.apache.spark.sql.catalyst.optimizer.Optimizer;

       new AbstractCarbonOptimizerFactory {
         override def createOptimizer(optimizer: Optimizer, conf: CarbonSQLConf): Optimizer = {
           class CarbonOptimizer1(optimizer: Optimizer, conf: CarbonSQLConf)
             extends Optimizer(conf) {
             override val batches = Nil;
             override def execute(plan: LogicalPlan): LogicalPlan = {
               CarbonOptimizer.execute(plan, optimizer);
             }
           }
           new CarbonOptimizer1(optimizer, conf);
         }
       }
    """

  val defaultOptimizerString =
    s"""
       import org.apache.spark.sql._;
       import org.apache.spark.sql.optimizer._;
       import org.apache.spark.sql.catalyst.plans.logical._;
       import org.apache.spark.sql.catalyst._;
       import org.apache.spark.sql.catalyst.optimizer.Optimizer;

       new AbstractCarbonOptimizerFactory {
         override def createOptimizer(optimizer: Optimizer, conf: CarbonSQLConf): Optimizer = {
           class CarbonOptimizer2(optimizer: Optimizer, conf: CarbonSQLConf) extends Optimizer {
             val batches = Nil;
             override def execute(plan: LogicalPlan): LogicalPlan = {
               CarbonOptimizer.execute(plan, optimizer);
             }
           }
           new CarbonOptimizer2(optimizer, conf);
         }
       }
    """

  val spark1_5ExpandString =
    s"""
       import org.apache.spark.sql._
       import org.apache.spark.sql.catalyst.plans.logical.{Expand, LogicalPlan}
       new AbstractCarbonExpandFactory {
         override def createExpand(expand: Expand, child: LogicalPlan): Expand = {
           Expand(expand.bitmasks, expand.groupByExprs, expand.gid, child)
         }
       }
    """
}

abstract class AbstractCarbonOptimizerFactory {
  def createOptimizer(optimizer: Optimizer, conf: CarbonSQLConf) : Optimizer
}

abstract class AbstractCarbonExpandFactory {
  def createExpand(expand: Expand, child: LogicalPlan) : Expand
}

