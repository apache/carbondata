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
package org.apache.spark.sql.test

import java.io._
import java.net.URL
import java.util.zip.ZipFile

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.IOUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.HdfsFileLock
import org.apache.carbondata.core.util.CarbonUtil

/**
 * All the registered resources will be checked into hdfs and copies to it if not exists.
 */
object ResourceRegisterAndCopier {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val link = "https://raw.githubusercontent" +
             ".com/ravipesala/incubator-carbondata/sdv-test_data/integration/spark-common-test" +
             "/src/test/resources"

  def copyResourcesifNotExists(hdfsPath: String,
      resourcePath: String,
      dataFilesPath: String): Unit = {
    val file = FileFactory.getCarbonFile(hdfsPath)
    if (!file.exists()) {
      sys.error(s"""Provided path $hdfsPath does not exist""")
    }
    LOGGER.info("Try downloading resource data")
    val lock = new HdfsFileLock(hdfsPath, "/resource.lock")
    var bool = false
    try {
      bool = lockWithRetries(lock)
      if (bool) {
        val resources = readDataFiles(dataFilesPath)
        resources.foreach { file =>
          val hdfsDataPath = hdfsPath + "/" + file
          val rsFile = FileFactory.getCarbonFile(hdfsDataPath)
          val target = resourcePath + "/" + file
          if (!rsFile.exists()) {
            if (file.lastIndexOf("/") > -1) {
              new File(resourcePath + "/" + file.substring(0, file.lastIndexOf("/"))).mkdirs()
            }
            downloadFile(link, file, target)
            // copy it
            copyLocalFile(hdfsDataPath, target)
            // Unzip the zip file to local directory
            if (target.endsWith("zip")) {
              unzip(target, new File(resourcePath + "/" + file.substring(0, file.lastIndexOf("/")))
                .getAbsolutePath)
            }
            new File(target).delete()
          } else if (target.endsWith("zip")) {
            if (new File(target).exists()) {
              FileFactory.deleteAllFilesOfDir(new File(target))
            }
            if (file.lastIndexOf("/") > -1) {
              new File(resourcePath + "/" + file.substring(0, file.lastIndexOf("/"))).mkdirs()
            }
            downloadFile(link, file, target)
            unzip(target, new File(resourcePath + "/" + file.substring(0, file.lastIndexOf("/")))
              .getAbsolutePath)
          }
        }
      }
    } finally {
      if (bool) {
        lock.unlock()
      }
    }
  }

  def lockWithRetries(lock: HdfsFileLock): Boolean = {
    try {
      var i = 0
      while (i < 10) {
        if (lock.lock()) {
          return true
        } else {
          Thread.sleep(30 * 1000L)
        }
        i += 1
      }
    } catch {
      case _: InterruptedException =>
        return false
    }
    false
  }

  def readDataFiles(dataFilesPath: String): Seq[String] = {
    val buffer = new ArrayBuffer[String]()
    val reader = new BufferedReader(new FileReader(dataFilesPath))
    var line = reader.readLine()
    while (line != null) {
      buffer += line
      line = reader.readLine()
    }
    reader.close()
    buffer
  }

  def copyLocalFile(dst: String,
      src: String): Unit = {
    LOGGER.info(s"Copying file : $src to  $dst")
    if (FileFactory.isFileExist(src)) {
      val dataOutputStream = FileFactory.getDataOutputStream(dst)
      val dataInputStream = FileFactory.getDataInputStream(src)
      IOUtils.copyBytes(dataInputStream, dataOutputStream, 8 * 1024)
      CarbonUtil.closeStream(dataInputStream)
      CarbonUtil.closeStream(dataOutputStream)
    }
  }

  def downloadFile(relativeLink: String, fileToDownLoad: String, targetFile: String): Unit = {
    import java.io.FileOutputStream
    val link = relativeLink + "/" + fileToDownLoad
    LOGGER.info(s"Downloading file $link")
    val url = new URL(link)
    val c = url.openConnection
    c.setRequestProperty("User-Agent",
      "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; .NET CLR 1.0.3705; .NET CLR 1.1.4322;" +
      " .NET CLR 1.2.30703)")

    val input = c.getInputStream
    val buffer = new Array[Byte](4096)
    var n = input.read(buffer)

    val output = new FileOutputStream(new File(targetFile))
    while (n != -1) {
      output.write(buffer, 0, n)
      n = input.read(buffer)
    }
    output.close()
    input.close()
  }

  private def unzip(zipFilePath: String, destDir: String) = {
    LOGGER.info(s"Uncompressing $zipFilePath to the directory $destDir")
    try {
      val zipFile = new ZipFile(zipFilePath)
      val enu = zipFile.entries
      while ( { enu.hasMoreElements }) {
        val zipEntry = enu.nextElement
        val name = destDir + "/" + zipEntry.getName
        val file = new File(name)
        if (name.endsWith("/")) {
          file.mkdirs
        } else {
          val parent = file.getParentFile
          if (parent != null) {
            parent.mkdirs
          }
          val is = zipFile.getInputStream(zipEntry)
          val fos = new FileOutputStream(file)
          val bytes = new Array[Byte](1024)
          var length = is.read(bytes)
          while (length >= 0) {
            fos.write(bytes, 0, length)
            length = is.read(bytes)
          }
          is.close
          fos.close()
        }
      }
      zipFile.close
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

}
