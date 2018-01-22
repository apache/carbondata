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

package org.apache.spark.sql.hive.test

import java.io.{DataInputStream, DataOutputStream, File, FileInputStream, IOException}
import java.util

import scala.collection.JavaConverters._

import mockit.{Invocation, Mock, MockUp}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataCommand, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalog

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.datastore.FileHolder
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory

class MockUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  class CarbonSessionCatalogMock extends MockUp[CarbonSessionCatalog] {

    LOGGER.audit("intializing mock CarbonSessionCatalog")

    @Mock def createDatabase(invocation: Invocation,
        dbDefinition: CatalogDatabase,
        ignoreIfExists: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(dbDefinition: CatalogDatabase, ignoreIfExists.asInstanceOf[AnyRef])
    }

    @Mock def dropDatabase(invocation: Invocation,
        db: String,
        ignoreIfNotExists: Boolean,
        cascade: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation
        .proceed(db: String, ignoreIfNotExists.asInstanceOf[AnyRef], cascade.asInstanceOf[AnyRef])
    }

    @Mock def alterDatabase(invocation: Invocation, dbDefinition: CatalogDatabase): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(dbDefinition)
    }

    @Mock def getDatabaseMetadata(invocation: Invocation, db: String): CatalogDatabase = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db)
    }

    @Mock def databaseExists(invocation: Invocation, db: String): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db)
    }

    @Mock def listDatabases(invocation: Invocation): Seq[String] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed()
    }

    @Mock def listDatabases(invocation: Invocation, pattern: String): Seq[String] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(pattern)

    }

    @Mock def getCurrentDatabase(invocation: Invocation): String = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed()
    }

    @Mock def setCurrentDatabase(invocation: Invocation, db: String): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db)
    }

    @Mock def getDefaultDBPath(invocation: Invocation, db: String): String = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db)
    }

    @Mock def createTable(invocation: Invocation,
        tableDefinition: CatalogTable,
        ignoreIfExists: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableDefinition: CatalogTable, ignoreIfExists.asInstanceOf[AnyRef])
    }

    @Mock def alterTable(invocation: Invocation, tableDefinition: CatalogTable): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableDefinition)
    }

    @Mock def tableExists(invocation: Invocation, name: TableIdentifier): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name)
    }

    @Mock def getTableMetadata(invocation: Invocation, name: TableIdentifier): CatalogTable = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name)
    }

    @Mock def getTableMetadataOption(invocation: Invocation,
        name: TableIdentifier): Option[CatalogTable] = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name)
    }

    @Mock def loadTable(invocation: Invocation,
        name: TableIdentifier,
        loadPath: String,
        isOverwrite: Boolean,
        holdDDLTime: Boolean): Unit = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name: TableIdentifier,
        loadPath: String,
        isOverwrite.asInstanceOf[AnyRef],
        holdDDLTime.asInstanceOf[AnyRef])
    }

    @Mock def loadPartition(invocation: Invocation,
        name: TableIdentifier,
        loadPath: String,
        partition: TablePartitionSpec,
        isOverwrite: Boolean,
        holdDDLTime: Boolean,
        inheritTableSpecs: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name: TableIdentifier,
        loadPath: String,
        partition: TablePartitionSpec,
        isOverwrite.asInstanceOf[AnyRef],
        holdDDLTime.asInstanceOf[AnyRef],
        inheritTableSpecs.asInstanceOf[AnyRef])
    }

    @Mock def defaultTablePath(invocation: Invocation, tableIdent: TableIdentifier): String = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableIdent)
    }

    @Mock def renameTable(invocation: Invocation,
        oldName: TableIdentifier,
        newName: TableIdentifier): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(oldName: TableIdentifier, newName: TableIdentifier)
    }

    @Mock def dropTable(invocation: Invocation,
        name: TableIdentifier,
        ignoreIfNotExists: Boolean,
        purge: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name: TableIdentifier,
        ignoreIfNotExists.asInstanceOf[AnyRef],
        purge.asInstanceOf[AnyRef])
    }

    @Mock def listTables(invocation: Invocation, db: String): Seq[TableIdentifier] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db)
    }

    @Mock def listTables(invocation: Invocation,
        db: String,
        pattern: String): Seq[TableIdentifier] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(db, pattern)
    }

    @Mock def dropPartitions(invocation: Invocation,
        tableName: TableIdentifier,
        specs: Seq[TablePartitionSpec],
        ignoreIfNotExists: Boolean,
        purge: Boolean,
        retainData: Boolean): Unit = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier,
        specs: Seq[TablePartitionSpec],
        ignoreIfNotExists.asInstanceOf[AnyRef],
        purge.asInstanceOf[AnyRef],
        retainData.asInstanceOf[AnyRef])
    }

    @Mock def renamePartitions(invocation: Invocation,
        tableName: TableIdentifier,
        specs: Seq[TablePartitionSpec],
        newSpecs: Seq[TablePartitionSpec]): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier,
        specs: Seq[TablePartitionSpec],
        newSpecs)

    }

    @Mock def alterPartitions(invocation: Invocation,
        tableName: TableIdentifier,
        parts: Seq[CatalogTablePartition]): Unit = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier, parts)
    }

    @Mock def getPartition(invocation: Invocation,
        tableName: TableIdentifier,
        spec: TablePartitionSpec): CatalogTablePartition = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier, spec)

    }

    @Mock def listPartitionNames(invocation: Invocation,
        tableName: TableIdentifier,
        partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {

      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier,
        partialSpec)
    }

    @Mock def listPartitions(invocation: Invocation,
        tableName: TableIdentifier,
        partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier,
        partialSpec)

    }

    @Mock def listPartitionsByFilter(invocation: Invocation,
        tableName: TableIdentifier,
        predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName: TableIdentifier,
        predicates)
    }


  }

  /**
   * Mock for file factory
   */
  class FileFactoryMock extends MockUp[FileFactory] {

    LOGGER.audit("intializing mock FileFactoryMock")

    @Mock def getFileHolder(invocation: Invocation, fileType: FileFactory.FileType): FileHolder = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(fileType)
    }

    @Mock def getCarbonFile(invocation: Invocation, path: String): CarbonFile = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path)
    }

    @Mock def getCarbonFile(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType): CarbonFile = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType)
    }

    @Mock def getCarbonFile(invocation: Invocation, path: String, fileType: FileFactory.FileType,
        hadoopConf: Configuration): CarbonFile = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType, hadoopConf)
    }

    @Mock def getDataInputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        bufferSize: Int,
        configuration: Configuration): DataInputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType, bufferSize.asInstanceOf[AnyRef], configuration)
    }

    @Mock def getDataInputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        bufferSize: Int,
        compressorName: String): DataInputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType, bufferSize.asInstanceOf[AnyRef], compressorName)
    }

    @Mock def getDataOutputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType): DataOutputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType)
    }

    @Mock def getDataOutputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        bufferSize: Int,
        append: Boolean): DataOutputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation
        .proceed(path, fileType, bufferSize.asInstanceOf[AnyRef], append.asInstanceOf[AnyRef])
    }

    @Mock def getDataOutputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        bufferSize: Int,
        blockSize: Long): DataOutputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation
        .proceed(path, fileType, bufferSize.asInstanceOf[AnyRef], blockSize.asInstanceOf[AnyRef])
    }

    @Mock def getDataOutputStream(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        bufferSize: Int,
        compressorName: String): DataOutputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType, bufferSize.asInstanceOf[AnyRef], compressorName)
    }

    @Mock def isFileExist(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType,
        performFileCheck: Boolean): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType, performFileCheck.asInstanceOf[AnyRef])
    }

    @Mock def isFileExist(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType)
    }

    @Mock def createNewFile(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType,
        doAs: Boolean,
        permission: FsPermission): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType, doAs.asInstanceOf[AnyRef], permission)
    }

    @Mock def deleteFile(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType)
    }

    @Mock def deleteAllFilesOfDir(invocation: Invocation, path: File): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path)
    }

    @Mock def deleteAllCarbonFilesOfDir(invocation: Invocation, path: CarbonFile): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path)
    }

    @Mock def mkdirs(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType)
    }

    @Mock def getDataOutputStreamUsingAppend(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType): DataOutputStream = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType)
    }

    @Mock def truncateFile(invocation: Invocation,
        path: String,
        fileType: FileFactory.FileType,
        newSize: Long): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(path, fileType, newSize.asInstanceOf[AnyRef])
    }

    @Mock def createNewLockFile(invocation: Invocation,
        filePath: String,
        fileType: FileFactory.FileType): Boolean = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath, fileType)
    }

    @Mock def getDirectorySize(invocation: Invocation, filePath: String): Long = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(filePath)
    }

    @Mock def createDirectoryAndSetPermission(invocation: Invocation,
        directoryPath: String,
        permission: FsPermission): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessMeta(trace)
      invocation.proceed(directoryPath, permission)
    }

  }

  new FileFactoryMock()
  new CarbonSessionCatalogMock()

  new MockUp[CarbonSessionCatalog]() {
    LOGGER.audit("intializing mock CarbonSessionCatalog")

    @Mock def createPartitions(
        invocation: Invocation,
        tableName: TableIdentifier,
        parts: Seq[CatalogTablePartition],
        ignoreIfExists: Boolean): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(tableName, parts, ignoreIfExists.asInstanceOf[AnyRef])
    }
  }

  new MockUp[CarbonSessionCatalog]() {
    LOGGER.audit("intializing mock CarbonSessionCatalog")

    @Mock def refreshTable(invocation: Invocation, name: TableIdentifier): Unit = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name)
    }
  }

  new MockUp[CarbonSessionCatalog]() {
    LOGGER.audit("intializing mock CarbonSessionCatalog")

    @Mock def lookupRelation(invocation: Invocation,
        name: TableIdentifier,
        alias: Option[String]): LogicalPlan = {
      val trace = Thread.currentThread().getStackTrace
      MockUtil.verifyProcessData(trace)
      invocation.proceed(name, alias)
    }
  }
}

object MockUtil {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val metacommand: util.List[String] =
    findClassesImpmenenting(
      classOf[MetadataCommand],
      classOf[MetadataCommand].getPackage)
  val datacommand = findClassesImpmenenting(classOf[DataCommand],
    classOf[MetadataCommand].getPackage)
  val autocommand = findClassesImpmenenting(classOf[AtomicRunnableCommand],
    classOf[MetadataCommand].getPackage)

  LOGGER.audit("Metacommand : " + metacommand)
  LOGGER.audit("Datacommand : " + datacommand)
  LOGGER.audit("Autocommand : " + autocommand)

  def findClassesImpmenenting(interfaceClass: Class[_],
      fromPackage: Package): util.List[String] = {
    if (interfaceClass == null) {
      LOGGER.audit("Unknown subclass.")
      return null
    }
    if (fromPackage == null) {
      LOGGER.audit("Unknown package.")
      return null
    }
    val rVal = new util.ArrayList[String]
    try {
      val targets = getAllClassesFromPackage(fromPackage.getName)
      if (targets != null) {
        for (aTarget <- targets) {
          if (aTarget != null && aTarget != interfaceClass &&
              interfaceClass.isAssignableFrom(aTarget)) {
            rVal.add(aTarget.getName)
          }
        }
      }
    } catch {
      case e: ClassNotFoundException =>
        LOGGER.audit("Error reading package name.")
        e.printStackTrace()
      case e: IOException =>
        LOGGER.audit("Error reading classes in package.")
        e.printStackTrace()
    }
    rVal
  }

  /**
   * Load all classes from a package.
   *
   * @param packageName
   * @return
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @throws[ClassNotFoundException]
  @throws[IOException]
  def getAllClassesFromPackage(packageName: String): Seq[Class[_]] = {
    val classLoader = Thread.currentThread.getContextClassLoader
    assert(classLoader != null)
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path)
    val dirs = new util.ArrayList[File]
    while ( { resources.hasMoreElements }) {
      val resource = resources.nextElement
      dirs.add(new File(resource.getFile))
    }
    val classes = new util.ArrayList[Class[_]]
    for (directory <- dirs.asScala) {
      classes.addAll(findClasses(directory, packageName))
      if (directory.getAbsolutePath.contains(".jar")) {
        readJar(directory.getPath.replace("file:", ""), classes)
      }
    }
    classes.asScala
  }

  def readJar(path: String, classes: util.List[Class[_]]): Unit = {
    LOGGER.audit("Reading jar with path : " + path)
    import java.util
    import java.util.zip.ZipInputStream
    val list = new util.ArrayList[String]
    val index = path.indexOf(".jar")
    val command = path.substring(index + 6, path.length)
    val src = new FileInputStream(path.substring(0, index + 4))
    if (src != null) {
      val zip = new ZipInputStream(src)
      var ze = zip.getNextEntry
      while (ze != null) {
        val entryName = ze.getName
        if (entryName.endsWith(".class") && entryName.contains(command)) {
          list.add(entryName)
          classes
            .add(Class
              .forName(entryName.substring(0, entryName.length - 6).replace("/", ".")))
        }
        ze = zip.getNextEntry
      }
    }
  }

  /**
   * Find file in package.
   *
   * @param directory
   * @param packageName
   * @return
   * @throws ClassNotFoundException
   */
  @throws[ClassNotFoundException]
  def findClasses(directory: File, packageName: String): util.List[Class[_]] = {
    val classes = new util.ArrayList[Class[_]]
    if (!directory.exists) {
      return classes
    }
    val files = directory.listFiles
    for (file <- files) {
      if (file.isDirectory) {
        assert(!file.getName.contains("."))
        classes.addAll(findClasses(file, packageName + "." + file.getName))
      }
      else if (file.getName.endsWith(".class")) {
        classes
          .add(Class
            .forName(packageName + '.' + file.getName.substring(0, file.getName.length - 6)))
      }
    }
    classes
  }

  def verifyProcessData(trace: Array[StackTraceElement]): Unit = {
    val exists = trace.exists { s =>
      if (MockUtil.datacommand.contains(s.getClassName) && s.getMethodName.equals("processData")) {
        true
      } else if (MockUtil.autocommand.contains(s.getClassName) &&
                 s.getMethodName.equals("processData")) {
        true
      } else {
        false
      }
    }
    if (exists) {
      throw new RuntimeException("Cannot use metastore operations from processData ")
    }
  }

  def verifyProcessMeta(trace: Array[StackTraceElement]): Unit = {
    var bypass = false
    val exists = trace.exists { s =>
      // Just bypass locking segment reading from verification
      // TODO Need to do something about it
      if (s.getClassName.contains("lock") ||
          (s.getClassName.equals("org.apache.carbondata.core.util.CarbonUtil") &&
           s.getMethodName.equals("calculateDataIndexSize")) ||
          s.getClassName.equals("org.apache.carbondata.core.statusmanager.SegmentStatusManager")) {
        bypass = true
      }
      if (MockUtil.metacommand.contains(s.getClassName) &&
          s.getMethodName.equals("processMetadata")) {
        true
      } else if (MockUtil.autocommand.contains(s.getClassName) &&
                 (s.getMethodName.equals("processMetadata") ||
                  s.getMethodName.equals("undoMetadata"))) {
        true
      } else {
        false
      }
    }
    if (!bypass && exists) {
      throw new RuntimeException("Cannot use file operations from processMetadata or  undoMetadata")
    }
  }

}
