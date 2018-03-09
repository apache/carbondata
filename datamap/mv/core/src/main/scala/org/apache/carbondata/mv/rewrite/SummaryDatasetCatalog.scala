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

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.plans.modular.{ModularPlan, SimpleModularizer}
import org.apache.carbondata.mv.plans.util.{BirdcageOptimizer, Signature}

/** Holds a summary logical plan */
private[mv] case class SummaryDataset(signature: Option[Signature], plan: LogicalPlan)

private[mv] class SummaryDatasetCatalog(saprkContext: SparkContext) {

  @transient
  private val summaryDatasets = new scala.collection.mutable.ArrayBuffer[SummaryDataset]

  @transient
  private val registerLock = new ReentrantReadWriteLock

  private val optimizer = BirdcageOptimizer

  private val modularizer = SimpleModularizer

  //  /** Returns true if the table is currently registered in-catalog. */
  //  def isRegistered(tableName: String): Boolean =
  //    lookupSummaryDataset(sqlContext.table(tableName)).nonEmpty
  //
  //  /** Registers the specified table in-memory catalog. */
  //  def registerTable(tableName: String): Unit =
  //    registerSummaryDataset(sqlContext.table(tableName), Some(tableName))
  //
  //  /** Removes the specified table from the in-memory catalog. */
  //  def unregisterTable(tableName: String): Unit =
  //   unregisterSummaryDataset(sqlContext.table(tableName))
  //
  //  override def unregisterAllTables(): Unit = {
  //    clearSummaryDatasetCatalog()
  //  }

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
  private[mv] def clearSummaryDatasetCatalog(): Unit = {
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
  private[mv] def registerSummaryDataset(
      query: DataFrame,
      tableName: Option[String] = None): Unit = {
    writeLock {
      val planToRegister = query.queryExecution.analyzed
      if (lookupSummaryDataset(planToRegister).nonEmpty) {
        sys.error(s"Asked to register already registered.")
      } else {
        val modularPlan = modularizer.modularize(optimizer.execute(planToRegister)).next()
          .harmonized
        val signature = modularPlan.signature
        summaryDatasets += SummaryDataset(signature, planToRegister)
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
      val feasible = summaryDatasets.filter { x =>
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
