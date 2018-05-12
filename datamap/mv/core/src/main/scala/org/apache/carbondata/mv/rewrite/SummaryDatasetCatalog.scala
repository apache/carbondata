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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.FindDataSourceTable
import org.apache.spark.sql.parser.CarbonSpark2SqlParser

import org.apache.carbondata.core.datamap.DataMapCatalog
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.mv.datamap.{MVHelper, MVState}
import org.apache.carbondata.mv.plans.modular.{Flags, ModularPlan, ModularRelation, Select}
import org.apache.carbondata.mv.plans.util.Signature

/** Holds a summary logical plan */
private[mv] case class SummaryDataset(signature: Option[Signature],
    plan: LogicalPlan,
    dataMapSchema: DataMapSchema,
    relation: ModularPlan)

private[mv] class SummaryDatasetCatalog(sparkSession: SparkSession)
  extends DataMapCatalog[SummaryDataset] {

  @transient
  private val summaryDatasets = new scala.collection.mutable.ArrayBuffer[SummaryDataset]

  val mVState = new MVState(this)

  @transient
  private val registerLock = new ReentrantReadWriteLock

  /**
   * parser
   */
  lazy val parser = new CarbonSpark2SqlParser

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
      // TODO Add mvfunction here, don't use preagg function
      val updatedQuery = parser.addPreAggFunction(dataMapSchema.getCtasQuery)
      val query = sparkSession.sql(updatedQuery)
      val planToRegister = MVHelper.dropDummFuc(query.queryExecution.analyzed)
      val modularPlan = mVState.modularizer.modularize(mVState.optimizer.execute(planToRegister))
        .next()
        .harmonized
      val signature = modularPlan.signature
      val identifier = dataMapSchema.getRelationIdentifier
      val output = new FindDataSourceTable(sparkSession).apply(sparkSession.sessionState.catalog
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

      summaryDatasets += SummaryDataset(signature, planToRegister, dataMapSchema, select)
    }
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


  override def listAllSchema(): Array[SummaryDataset] = summaryDatasets.toArray

  /** Returns feasible registered summary data sets for processing the given ModularPlan. */
  private[mv] def lookupFeasibleSummaryDatasets(plan: ModularPlan): Seq[SummaryDataset] = {
    readLock {
      val sig = plan.signature
      val statusDetails = DataMapStatusManager.getEnabledDataMapStatusDetails
      // Only select the enabled datamaps for the query.
      val enabledDataSets = summaryDatasets.filter{p =>
        statusDetails.exists(_.getDataMapName.equalsIgnoreCase(p.dataMapSchema.getDataMapName))
      }
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
