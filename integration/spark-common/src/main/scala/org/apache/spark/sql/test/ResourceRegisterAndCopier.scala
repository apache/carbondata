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

import java.io.{BufferedReader, File, FileReader}
import java.net.URL

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.io.IOUtils

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil

/**
 * All the registered resources will be checked into hdfs and copies to it if not exists.
 */
object ResourceRegisterAndCopier {

  val link = "https://raw.githubusercontent.com/ravipesala/incubator-carbondata/sdv-test_data/integration/spark-common-test/src/test/resources"

  def copyResourcesifNotExists(hdfsPath: String, resourcePath: String, dataFilesPath: String): Unit = {
    val fileType = FileFactory.getFileType(hdfsPath)
    val file = FileFactory.getCarbonFile(hdfsPath, fileType)
    if (!file.exists()) {
      sys.error(s"""Provided path $hdfsPath does not exist""")
    }
    val resources = readDataFiles(dataFilesPath)
    resources.foreach {file =>
      val hdfsDataPath = hdfsPath + "/" + file
      val rsFile = FileFactory.getCarbonFile(hdfsDataPath, fileType)
      if (!rsFile.exists()) {
        val target = resourcePath + "/" + file
        new File(resourcePath + "/" + file.substring(0, file.lastIndexOf("/"))).mkdirs()
        downloadFile(link, file, target)
        // copy it
        copyLocalFile(hdfsDataPath, target)
        new File(target).delete()
      }
    }
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
    println(s"Copying file : $src to  $dst")
    val dataOutputStream = FileFactory.getDataOutputStream(dst,
        FileFactory.getFileType(dst))
    val dataInputStream = FileFactory.getDataInputStream(src,
      FileFactory.getFileType(src))
    IOUtils.copyBytes(dataInputStream, dataOutputStream, 8*1024)
    CarbonUtil.closeStream(dataInputStream)
    CarbonUtil.closeStream(dataOutputStream)
  }

  def downloadFile(relativeLink: String, fileToDownLoad: String, targetFile: String): Unit = {
    import java.io.FileOutputStream
    val link = relativeLink + "/" + fileToDownLoad
    println(s"Downloading file $link")
    val url = new URL(link)
    val c = url.openConnection
    c.setRequestProperty("User-Agent",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; .NET CLR 1.0.3705; .NET CLR 1.1.4322;" +
        " .NET CLR 1.2.30703)")

    var input = c.getInputStream
    val buffer = new Array[Byte](4096)
    var n = input.read(buffer)

    val output = new FileOutputStream(new File(targetFile))
    while ( n != -1)  {
      output.write(buffer, 0, n)
      n = input.read(buffer)
    }
    output.close()
    input.close()
  }

}
