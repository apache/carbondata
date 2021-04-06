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

package org.apache.carbondata.view

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonToSparkAdapter, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.parser.MVQueryParser

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier
import org.apache.carbondata.core.view.{MVCatalog, MVSchema}
import org.apache.carbondata.mv.plans.modular.{Flags, ModularPlan, ModularRelation, Select, SimpleModularizer}
import org.apache.carbondata.mv.plans.util.BirdcageOptimizer

case class MVCatalogInSpark(session: SparkSession)
  extends MVCatalog[MVSchemaWrapper] {

  @transient
  private val logger = MVCatalogInSpark.LOGGER

  @transient
  private val lock = new ReentrantReadWriteLock

  @transient
  private val viewSchemas = new scala.collection.mutable.ArrayBuffer[MVSchemaWrapper]

  @transient
  private val viewManager = MVManagerInSpark.get(session)

  /** Acquires a read lock on the catalog for the duration of `f`. */
  private def withReadLock[A](f: => A): A = {
    val readLock = lock.readLock()
    readLock.lock()
    try f finally {
      readLock.unlock()
    }
  }

  /** Acquires a write lock on the catalog for the duration of `f`. */
  private def withWriteLock[A](function: => A): A = {
    val writeLock = lock.writeLock()
    writeLock.lock()
    try function finally {
      writeLock.unlock()
    }
  }

  override def getValidSchemas: Array[MVSchemaWrapper] = {
    val enabledStatusDetails = viewManager.getEnabledStatusDetails.asScala
    // Only select the enabled mvs for the query.
    val enabledSchemas = viewSchemas.filter {
      enabledSchema =>
        enabledStatusDetails.exists(
          statusDetail =>
            statusDetail.getIdentifier.getDatabaseName.equalsIgnoreCase(
              enabledSchema.viewSchema.getIdentifier.getDatabaseName) &&
            statusDetail.getIdentifier.getTableName.equalsIgnoreCase(
              enabledSchema.viewSchema.getIdentifier.getTableName))
    }
    enabledSchemas.toArray
  }

  def getAllSchemas: Array[MVSchemaWrapper] = {
    viewSchemas.toArray
  }

  def isMVInSync(mvSchema: MVSchema): Boolean = {
    viewManager.isMVInSyncWithParentTables(mvSchema)
  }

  /**
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  def registerSchema(mvSchema: MVSchema): Unit = {
    withWriteLock {
      val currentDatabase = session.catalog.currentDatabase
      val logicalPlan = try {
        // This is required because mv schemas are across databases, so while loading the
        // catalog, if the mv is in database other than sparkSession.currentDataBase(), then it
        // fails to register, so set the database present in the mvSchema Object
        session.catalog.setCurrentDatabase(mvSchema.getIdentifier.getDatabaseName)
        MVHelper.dropDummyFunction(MVQueryParser.getQueryPlan(mvSchema.getQuery, session))
      } finally {
        // here setting back to current database of current session, because if the actual query
        // contains db name in query like, select db1.column1 from table and current database is
        // default and if we drop the db1, still the session has current db as db1.
        // So setting back to current database.
        session.catalog.setCurrentDatabase(currentDatabase)
      }
      val mvSignature = SimpleModularizer.modularize(
        BirdcageOptimizer.execute(logicalPlan)).next().semiHarmonized.signature
      val mvIdentifier = mvSchema.getIdentifier
      val mvTableIdentifier =
        TableIdentifier(mvIdentifier.getTableName, Some(mvIdentifier.getDatabaseName))
      val plan = new FindDataSourceTable(session)
        .apply(session.sessionState.catalog.lookupRelation(mvTableIdentifier))
      val output = plan match {
        case alias: SubqueryAlias =>
          CarbonToSparkAdapter.getOutput(alias)
        case _ =>
          plan.output
      }
      val relation = ModularRelation(mvIdentifier.getDatabaseName,
        mvIdentifier.getTableName,
        output,
        Flags.NoFlags,
        Seq.empty)
      val modularPlan = Select(relation.outputList,
        relation.outputList,
        Seq.empty,
        Seq((0, mvIdentifier.getTableName)).toMap,
        Seq.empty,
        Seq(relation),
        Flags.NoFlags,
        Seq.empty,
        Seq.empty,
        None)
      viewSchemas += MVSchemaWrapper(
        mvSignature,
        mvSchema,
        logicalPlan,
        MVPlanWrapper(modularPlan, mvSchema))
    }
  }

  /** Removes the given [[DataFrame]] from the catalog */
  def deregisterSchema(mvIdentifier: RelationIdentifier): Unit = {
    withWriteLock {
      val mvIndex = viewSchemas.indexWhere(_.viewSchema.getIdentifier.equals(mvIdentifier))
      require(mvIndex >= 0, s"MV $mvIdentifier is not registered.")
      viewSchemas.remove(mvIndex)
    }
  }

  /**
   * API for test only
   *
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  def registerSchema(query: DataFrame, tableName: Option[String] = None): Unit = {
    withWriteLock {
      val logicalPlan = query.queryExecution.analyzed
      if (lookupSchema(logicalPlan).nonEmpty) {
        logger.error(s"Asked to register already registered.")
      } else {
        val modularPlan = SimpleModularizer.modularize(
            BirdcageOptimizer.execute(logicalPlan)).next().semiHarmonized
        val signature = modularPlan.signature
        viewSchemas += MVSchemaWrapper(signature, null, logicalPlan, null)
      }
    }
  }

  /**
   * Check and return mv having same query already present in the catalog
   */
  def getMVWithSameQueryPresent(query: LogicalPlan): Option[MVSchemaWrapper] = {
    lookupSchema(query)
  }

  /** Returns feasible registered mv schemas for processing the given ModularPlan. */
  private def lookupSchema(plan: LogicalPlan): Option[MVSchemaWrapper] = {
    withReadLock {
      viewSchemas.find(mvSchema => plan.sameResult(mvSchema.logicalPlan))
    }
  }


  /** Returns feasible registered mv schemas for processing the given ModularPlan. */
  def lookupFeasibleSchemas(plan: ModularPlan): Seq[MVSchemaWrapper] = {
    withReadLock {
      val signature = plan.signature
      val enableStatusDetails = viewManager.getEnabledStatusDetails.asScala
      // Only select the enabled mvs for the query.
      val enabledSchemas = viewSchemas.filter {
        viewSchema =>
          enableStatusDetails.exists(
            _.getIdentifier.getTableName.equalsIgnoreCase(
              viewSchema.viewSchema.getIdentifier.getTableName))
      }
      // Heuristics: more tables involved in a mv schema set => higher query reduction factor
      enabledSchemas.filter {
        enabledSchema => (enabledSchema.viewSignature, signature) match {
          case (Some(signature1), Some(signature2)) =>
            if (signature1.groupby && signature2.groupby &&
                signature1.datasets.subsetOf(signature2.datasets)) {
              true
            } else if (!signature1.groupby && !signature2.groupby &&
                       signature1.datasets.subsetOf(signature2.datasets)) {
              true
            } else {
              false
            }
          case _ => false
        }
      }
      .sortWith(_.viewSignature.get.datasets.size > _.viewSignature.get.datasets.size)
    }
  }

}

object MVCatalogInSpark {

  val LOGGER: Logger =
    LogServiceFactory.getLogService(classOf[MVCatalogInSpark].getCanonicalName)

}
