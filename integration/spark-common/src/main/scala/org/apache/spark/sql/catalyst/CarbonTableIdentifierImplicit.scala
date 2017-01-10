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

package org.apache.spark.sql.catalyst

/**
 * Implicit functions for [TableIdentifier]
 */
object CarbonTableIdentifierImplicit {
  def apply(tableName: String): TableIdentifier = TableIdentifier(tableName)

  implicit def toTableIdentifier(tableIdentifier: Seq[String]): TableIdentifier = {
    tableIdentifier match {
      case Seq(dbName, tableName) => TableIdentifier(tableName, Some(dbName))
      case Seq(tableName) => TableIdentifier(tableName, None)
      case _ => throw new IllegalArgumentException("invalid table identifier: " + tableIdentifier)
    }
  }

  implicit def toSequence(tableIdentifier: TableIdentifier): Seq[String] = {
    tableIdentifier.database match {
      case Some(dbName) => Seq(dbName, tableIdentifier.table)
      case _ => Seq(tableIdentifier.table)
    }
  }

  implicit def toOptionalSequence(alias: Option[String]): Option[Seq[String]] = {
    alias match {
      case Some(alias) => Some(Seq(alias))
      case _ => None
    }
  }
}
