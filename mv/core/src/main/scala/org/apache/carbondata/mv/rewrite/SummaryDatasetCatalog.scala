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

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.FindDataSourceTable

import org.apache.carbondata.core.datamap.DataMapCatalog
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.mv.extension.{MVHelper, MVParser}
import org.apache.carbondata.mv.plans.modular.{Flags, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.carbondata.mv.session.MVSession


/** Holds a summary logical plan */
private[mv] case class SummaryDataset(
    signature: Option[Signature],
    plan: LogicalPlan,
    dataMapSchema: DataMapSchema,
    relation: ModularPlan)

/**
 * It is wrapper on datamap relation along with schema.
 */
case class MVPlanWrapper(plan: ModularPlan, dataMapSchema: DataMapSchema) extends ModularPlan {
  override def output: Seq[Attribute] = plan.output

  override def children: Seq[ModularPlan] = plan.children
}

private[mv] class SummaryDatasetCatalog(sparkSession: SparkSession)
  extends DataMapCatalog[SummaryDataset] {

  @transient
  private val summaryDatasets = new scala.collection.mutable.ArrayBuffer[SummaryDataset]

  val mvSession = new MVSession(sparkSession, this)

  @transient
  private val registerLock = new ReentrantReadWriteLock


  /** Acquires a read lock on the catalog for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val lock = registerLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the catalog for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val lock = registerLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Clears all summary tables. */
  private[mv] def refresh(): Unit = {
    writeLock {
      summaryDatasets.clear()
    }
  }

  /** Checks if the catalog is empty. */
  private[mv] def isEmpty: Boolean = {
    readLock {
      summaryDatasets.isEmpty
    }
  }

  /**
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[mv] def registerSchema(dataMapSchema: DataMapSchema): Unit = {
    writeLock {
      val currentDatabase = sparkSession.catalog.currentDatabase

      // This is required because datamap schemas are across databases, so while loading the
      // catalog, if the datamap is in database other than sparkSession.currentDataBase(), then it
      // fails to register, so set the database present in the dataMapSchema Object
      setCurrentDataBase(dataMapSchema.getRelationIdentifier.getDatabaseName)
      val mvPlan = MVParser.getMVPlan(dataMapSchema.getCtasQuery, sparkSession)
      // here setting back to current database of current session, because if the actual query
      // contains db name in query like, select db1.column1 from table and current database is
      // default and if we drop the db1, still the session has current db as db1.
      // So setting back to current database.
      setCurrentDataBase(currentDatabase)
      val planToRegister = MVHelper.dropDummyFunc(mvPlan)
      val modularPlan =
        mvSession.sessionState.modularizer.modularize(
          mvSession.sessionState.optimizer.execute(planToRegister)).next().semiHarmonized
      val signature = modularPlan.signature
      val identifier = dataMapSchema.getRelationIdentifier
      val output = new FindDataSourceTable(sparkSession)
        .apply(sparkSession.sessionState.catalog
        .lookupRelation(TableIdentifier(identifier.getTableName, Some(identifier.getDatabaseName))))
        .output
      val relation = ModularRelation(identifier.getDatabaseName,
        identifier.getTableName,
        output,
        Flags.NoFlags,
        Seq.empty)
      val select = Select(relation.outputList,
        relation.outputList,
        Seq.empty,
        Seq((0, identifier.getTableName)).toMap,
        Seq.empty,
        Seq(relation),
        Flags.NoFlags,
        Seq.empty,
        Seq.empty,
        None)

      summaryDatasets += SummaryDataset(
        signature,
        planToRegister,
        dataMapSchema,
        MVPlanWrapper(select, dataMapSchema))
    }
  }

  private def setCurrentDataBase(dataBaseName: String): Unit = {
    sparkSession.catalog.setCurrentDatabase(dataBaseName)
  }

  /** Removes the given [[DataFrame]] from the catalog */
  private[mv] def unregisterSchema(dataMapName: String): Unit = {
    writeLock {
      val dataIndex = summaryDatasets
        .indexWhere(sd => sd.dataMapSchema.getDataMapName.equals(dataMapName))
      require(dataIndex >= 0, s"Datamap $dataMapName is not registered.")
      summaryDatasets.remove(dataIndex)
    }
  }


  override def listAllValidSchema(): Array[SummaryDataset] = {
    val statusDetails = DataMapStatusManager.getEnabledDataMapStatusDetails
    // Only select the enabled datamaps for the query.
    val enabledDataSets = summaryDatasets.filter { p =>
      statusDetails.exists(_.getDataMapName.equalsIgnoreCase(p.dataMapSchema.getDataMapName))
    }
    enabledDataSets.toArray
  }

  /**
   * API for test only
   *
   * Registers the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  private[mv] def registerSummaryDataset(
      query: DataFrame,
      tableName: Option[String] = None): Unit = {
    writeLock {
      val planToRegister = query.queryExecution.analyzed
      if (lookupSummaryDataset(planToRegister).nonEmpty) {
        sys.error(s"Asked to register already registered.")
      } else {
        val modularPlan =
          mvSession.sessionState.modularizer.modularize(
            mvSession.sessionState.optimizer.execute(planToRegister)).next().semiHarmonized
        val signature = modularPlan.signature
        summaryDatasets +=
        SummaryDataset(signature, planToRegister, null, null)
      }
    }
  }

  /** Removes the given [[DataFrame]] from the catalog */
  private[mv] def unregisterSummaryDataset(query: DataFrame): Unit = {
    writeLock {
      val planToRegister = query.queryExecution.analyzed
      val dataIndex = summaryDatasets.indexWhere(sd => planToRegister.sameResult(sd.plan))
      require(dataIndex >= 0, s"Table $query is not registered.")
      summaryDatasets.remove(dataIndex)
    }
  }

  /**
   * Check already with same query present in mv
   */
  private[mv] def isMVWithSameQueryPresent(query: LogicalPlan) : Boolean = {
    lookupSummaryDataset(query).nonEmpty
  }

  /**
   * API for test only
   * Tries to remove the data set for the given [[DataFrame]] from the catalog if it's registered
   */
  private[mv] def tryUnregisterSummaryDataset(
      query: DataFrame,
      blocking: Boolean = true): Boolean = {
    writeLock {
      val planToRegister = query.queryExecution.analyzed
      val dataIndex = summaryDatasets.indexWhere(sd => planToRegister.sameResult(sd.plan))
      val found = dataIndex >= 0
      if (found) {
        summaryDatasets.remove(dataIndex)
      }
      found
    }
  }

  /** Optionally returns registered data set for the given [[DataFrame]] */
  private[mv] def lookupSummaryDataset(query: DataFrame): Option[SummaryDataset] = {
    readLock {
      lookupSummaryDataset(query.queryExecution.analyzed)
    }
  }

  /** Returns feasible registered summary data sets for processing the given ModularPlan. */
  private[mv] def lookupSummaryDataset(plan: LogicalPlan): Option[SummaryDataset] = {
    readLock {
      summaryDatasets.find(sd => plan.sameResult(sd.plan))
    }
  }


  /** Returns feasible registered summary data sets for processing the given ModularPlan. */
  private[mv] def lookupFeasibleSummaryDatasets(plan: ModularPlan): Seq[SummaryDataset] = {
    readLock {
      val sig = plan.signature
      val statusDetails = DataMapStatusManager.getEnabledDataMapStatusDetails
      // Only select the enabled datamaps for the query.
      val enabledDataSets = summaryDatasets.filter { p =>
        statusDetails.exists(_.getDataMapName.equalsIgnoreCase(p.dataMapSchema.getDataMapName))
      }

      //  ****not sure what enabledDataSets is used for ****
      //  can enable/disable datamap move to other place ?
      //    val feasible = enabledDataSets.filter { x =>
      val feasible = enabledDataSets.filter { x =>
        (x.signature, sig) match {
          case (Some(sig1), Some(sig2)) =>
            if (sig1.groupby && sig2.groupby && sig1.datasets.subsetOf(sig2.datasets)) {
              true
            } else if (!sig1.groupby && !sig2.groupby && sig1.datasets.subsetOf(sig2.datasets)) {
              true
            } else {
              false
            }

          case _ => false
        }
      }
      // heuristics: more tables involved in a summary data set => higher query reduction factor
      feasible.sortWith(_.signature.get.datasets.size > _.signature.get.datasets.size)
    }
  }
}
