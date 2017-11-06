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
package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResourceLoader, GlobalTempViewManager, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation, PreWriteCheck, ResolveSQLOnFile, _}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy}
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSparkSqlParser

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier

/**
 * This class will have carbon catalog and refresh the relation from cache if the carbontable in
 * carbon catalog is not same as cached carbon relation's carbon table
 *
 * @param externalCatalog
 * @param globalTempViewManager
 * @param sparkSession
 * @param functionResourceLoader
 * @param functionRegistry
 * @param conf
 * @param hadoopConf
 */
class CarbonSessionCatalog(
    externalCatalog: HiveExternalCatalog,
    globalTempViewManager: GlobalTempViewManager,
    functionRegistry: FunctionRegistry,
    sparkSession: SparkSession,
    conf: SQLConf,
    hadoopConf: Configuration,
    parser: ParserInterface,
    functionResourceLoader: FunctionResourceLoader)
  extends HiveSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) {

  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }


  private def refreshRelationFromCache(identifier: TableIdentifier,
      alias: Option[String],
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation): Boolean = {
    var isRefreshed = false
    val storePath = CarbonEnv.getInstance(sparkSession).storePath
    carbonEnv.carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables(storePath)

    val tableMeta = carbonEnv.carbonMetastore
      .getTableFromMetadataCache(carbonDatasourceHadoopRelation.carbonTable.getDatabaseName,
        carbonDatasourceHadoopRelation.carbonTable.getFactTableName)
    if (tableMeta.isEmpty || (tableMeta.isDefined &&
                              tableMeta.get.carbonTable.getTableLastUpdatedTime !=
                              carbonDatasourceHadoopRelation.carbonTable.getTableLastUpdatedTime)) {
      refreshTable(identifier)
      DataMapStoreManager.getInstance().
        clearDataMap(AbsoluteTableIdentifier.from(storePath,
          identifier.database.getOrElse("default"), identifier.table))
      isRefreshed = true
      logInfo(s"Schema changes have been detected for table: $identifier")
    }
    isRefreshed
  }

  //  override def makeFunctionBuilder(funcName: String, className: String): FunctionBuilder = {
  //    makeFunctionBuilder(funcName, Utils.classForName(className))
  //  }
  //
  //  /**
  //   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
  //   */
  //  private def makeFunctionBuilder(name: String, clazz: Class[_]): FunctionBuilder = {
  //    // When we instantiate hive UDF wrapper class, we may throw exception if the input
  //    // expressions don't satisfy the hive UDF, such as type mismatch, input number
  //    // mismatch, etc. Here we catch the exception and throw AnalysisException instead.
  //    (children: Seq[Expression]) => {
  //      try {
  //        if (classOf[UDF].isAssignableFrom(clazz)) {
  //          val udf = HiveSimpleUDF(name, new HiveFunctionWrapper(clazz.getName), children)
  //          udf.dataType // Force it to check input data types.
  //          udf
  //        } else if (classOf[GenericUDF].isAssignableFrom(clazz)) {
  //          val udf = HiveGenericUDF(name, new HiveFunctionWrapper(clazz.getName), children)
  //          udf.dataType // Force it to check input data types.
  //          udf
  //        } else if (classOf[AbstractGenericUDAFResolver].isAssignableFrom(clazz)) {
  //          val udaf = HiveUDAFFunction(name, new HiveFunctionWrapper(clazz.getName), children)
  //          udaf.dataType // Force it to check input data types.
  //          udaf
  //        } else if (classOf[UDAF].isAssignableFrom(clazz)) {
  //          val udaf = HiveUDAFFunction(
  //            name,
  //            new HiveFunctionWrapper(clazz.getName),
  //            children,
  //            isUDAFBridgeRequired = true)
  //          udaf.dataType  // Force it to check input data types.
  //          udaf
  //        } else if (classOf[GenericUDTF].isAssignableFrom(clazz)) {
  //          val udtf = HiveGenericUDTF(name, new HiveFunctionWrapper(clazz.getName), children)
  //          udtf.elementSchema // Force it to check input data types.
  //          udtf
  //        } else {
  //          throw new AnalysisException(s"No handler for Hive UDF '${clazz.getCanonicalName}'")
  //        }
  //      } catch {
  //        case ae: AnalysisException =>
  //          throw ae
  //        case NonFatal(e) =>
  //          val analysisException =
  //            new AnalysisException(s"No handler for Hive UDF '${clazz.getCanonicalName}': $e")
  //          analysisException.setStackTrace(e.getStackTrace)
  //          throw analysisException
  //      }
  //    }
  //  }
  //
  //  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
  //    try {
  //      lookupFunction0(name, children)
  //    } catch {
  //      case NonFatal(_) =>
  //        // SPARK-16228 ExternalCatalog may recognize `double`-type only.
  //        val newChildren = children.map { child =>
  //          if (child.dataType.isInstanceOf[DecimalType]) Cast(child, DoubleType) else child
  //        }
  //        lookupFunction0(name, newChildren)
  //    }
  //  }
  //
  //  private def lookupFunction0(name: FunctionIdentifier, children: Seq[Expression]): Expression = {
  //    val database = name.database.map(formatDatabaseName)
  //    val funcName = name.copy(database = database)
  //    Try(super.lookupFunction(funcName, children)) match {
  //      case Success(expr) => expr
  //      case Failure(error) =>
  //        if (functionRegistry.functionExists(funcName.unquotedString)) {
  //          // If the function actually exists in functionRegistry, it means that there is an
  //          // error when we create the Expression using the given children.
  //          // We need to throw the original exception.
  //          throw error
  //        } else {
  //          // This function is not in functionRegistry, let's try to load it as a Hive's
  //          // built-in function.
  //          // Hive is case insensitive.
  //          val functionName = funcName.unquotedString.toLowerCase(Locale.ROOT)
  //          if (!hiveFunctions.contains(functionName)) {
  //            failFunctionLookup(funcName)
  //          }
  //
  //          // TODO: Remove this fallback path once we implement the list of fallback functions
  //          // defined below in hiveFunctions.
  //          val functionInfo = {
  //            try {
  //              Option(HiveFunctionRegistry.getFunctionInfo(functionName)).getOrElse(
  //                failFunctionLookup(funcName))
  //            } catch {
  //              // If HiveFunctionRegistry.getFunctionInfo throws an exception,
  //              // we are failing to load a Hive builtin function, which means that
  //              // the given function is not a Hive builtin function.
  //              case NonFatal(e) => failFunctionLookup(funcName)
  //            }
  //          }
  //          val className = functionInfo.getFunctionClass.getName
  //          val functionIdentifier =
  //            FunctionIdentifier(functionName.toLowerCase(Locale.ROOT), database)
  //          val func = CatalogFunction(functionIdentifier, className, Nil)
  //          // Put this Hive built-in function to our function registry.
  //          registerFunction(func, ignoreIfExists = false)
  //          // Now, we need to create the Expression.
  //          functionRegistry.lookupFunction(functionName, children)
  //        }
  //    }
  //  }
  //
  //  // TODO Removes this method after implementing Spark native "histogram_numeric".
  //  override def functionExists(name: FunctionIdentifier): Boolean = {
  //    super.functionExists(name) || hiveFunctions.contains(name.funcName)
  //  }
  //
  //  /** List of functions we pass over to Hive. Note that over time this list should go to 0. */
  //  // We have a list of Hive built-in functions that we do not support. So, we will check
  //  // Hive's function registry and lazily load needed functions into our own function registry.
  //  // List of functions we are explicitly not supporting are:
  //  // compute_stats, context_ngrams, create_union,
  //  // current_user, ewah_bitmap, ewah_bitmap_and, ewah_bitmap_empty, ewah_bitmap_or, field,
  //  // in_file, index, matchpath, ngrams, noop, noopstreaming, noopwithmap,
  //  // noopwithmapstreaming, parse_url_tuple, reflect2, windowingtablefunction.
  //  // Note: don't forget to update SessionCatalog.isTemporaryFunction
  //  private val hiveFunctions = Seq(
  //    "histogram_numeric"
  //  )
}

/**
 * Session state implementation to override sql parser and adding strategies
 *
 * @param sparkSession
 */
class CarbonSessionStateBuilder(sparkSession: SparkSession, parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies =
    Seq(new CarbonLateDecodeStrategy, new DDLStrategy(sparkSession))
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog : CarbonSessionCatalog= {
    new CarbonSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog],
      sparkSession.sharedState.globalTempViewManager,
      functionRegistry,
      sparkSession,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        CarbonIUDAnalysisRule(sparkSession) +:
        CarbonPreInsertionCasts


      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
        PreWriteCheck +:
        HiveOnlyCheck
    }
  }

  override protected def newBuilder: NewBuilder = new CarbonSessionStateBuilder(_, _)

}


class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    // In case scalar subquery add flag in relation to skip the decoder plan in optimizer rule, And
    // optimize whole plan at once.
    val transFormedPlan = plan.transform {
      case filter: Filter =>
        filter.transformExpressions {
          case s: ScalarSubquery =>
            val tPlan = s.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            ScalarSubquery(tPlan, s.children, s.exprId)
        }
    }
    super.execute(transFormedPlan)
  }

}
