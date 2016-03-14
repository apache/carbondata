package org.apache.spark.sql.hive

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.hive.client.ClientInterface
import org.apache.spark.sql.catalyst.TableIdentifier

class CompositeMetastoreCatalog(conf: CatalystConf,
                                val children: ArrayBuffer[Catalog] = new ArrayBuffer[Catalog], client: ClientInterface,
                                hive: HiveContext) extends HiveMetastoreCatalog(client, hive) {

  //children += this

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] =
    super.getTables(databaseName) ++ children.foldLeft(Seq[(String, Boolean)]())(_ ++ _.getTables(databaseName))

  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan =
    if (super.tableExists(tableIdentifier))
      super.lookupRelation(tableIdentifier, alias)
    else
      children.find(_.tableExists(tableIdentifier))
        .getOrElse(sys.error(s"Table Not Found: $tableIdentifier"))
        .lookupRelation(tableIdentifier, alias)

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    val tableIdentifier = Seq(tableIdent.database.getOrElse(client.currentDatabase), tableIdent.table)
    if (super.tableExists(tableIdentifier))
      super.refreshTable(tableIdent)
    else
      children.find(_.tableExists(tableIdentifier))
        .foreach(_.refreshTable(tableIdent))
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    try {
      super.registerTable(tableIdentifier, plan)
    }
    catch {
      case x: NotImplementedError =>
        children.find(x =>
          try {
            x.registerTable(tableIdentifier, plan)
            true
          } catch {
            case x: NotImplementedError => false
          })
    }
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    if (super.tableExists(tableIdentifier))
      true
    else
      children.exists(_.tableExists(tableIdentifier))
  }

  override def unregisterAllTables(): Unit = {
    try {
      super.unregisterAllTables
    }
    catch {
      case x: NotImplementedError =>
        children.foreach(x =>
          try {
            x.unregisterAllTables()
          } catch {
            case x: NotImplementedError =>
          })
    }
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    var checkChildren = false;
    try {
      if (super.tableExists(tableIdentifier))
        super.unregisterTable(tableIdentifier)
      else
        checkChildren = true;
    }
    catch {
      case _: NotImplementedError =>
        checkChildren = true;
    }

    if (checkChildren) {
      children.find {
        _.tableExists(tableIdentifier)
      }
        .foreach(
          x =>
            try {
              x.unregisterTable(tableIdentifier)
            } catch {
              case _: NotImplementedError =>
            })
    }
  }

  def addMetastoreCatalog(catalog: Catalog) = children += catalog

  def getAllDatabases(schemaLike: Option[String]): Seq[String] = {

    client.runSqlHive("show databases") /*.map{c=>
      schemaLike match {
        case Some(name) => 
          if(c.contains(name)) c else null
        case _=> c      }
    }*/ .filter(f => f != null)

  }
}