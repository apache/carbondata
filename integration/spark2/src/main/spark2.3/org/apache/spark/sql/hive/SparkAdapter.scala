package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.catalog.{ExternalCatalog, GlobalTempViewManager}
import org.apache.spark.sql.hive.client.HiveClient

object SparkAdapter {
  def getExternalCatalogCatalog(catalog: HiveExternalCatalog) = catalog

  def getGlobalTempViewManager(manager: GlobalTempViewManager) = manager

  def getHiveClient(client: HiveClient) = client

  def getHiveExternalCatalog(catalog: ExternalCatalog) = catalog.asInstanceOf[HiveExternalCatalog]
}
