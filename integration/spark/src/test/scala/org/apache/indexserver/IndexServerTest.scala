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

package org.apache.indexserver

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonUtil


class IndexServerTest extends FunSuite with BeforeAndAfterEach {
  val folderPath = CarbonUtil.getIndexServerTempPath

  override protected def beforeEach(): Unit = {
    if (FileFactory.isFileExist(folderPath)) {
      FileFactory.deleteFile(folderPath)
    }
    FileFactory.createDirectoryAndSetPermission(folderPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
  }

  override protected def afterEach(): Unit = {
    if (FileFactory.isFileExist(folderPath)) {
      FileFactory.deleteFile(folderPath)
    }
  }

  test("test clean tmp folder when restart") {
    val folderPath = CarbonUtil.getIndexServerTempPath
    assert(FileFactory.isFileExist(folderPath))
    CarbonUtil.cleanTempFolderForIndexServer()
    assert(!FileFactory.isFileExist(folderPath))
  }

  test("test age tmp folder as some period") {
    val folderPath = CarbonUtil.getIndexServerTempPath
    val tmpPath = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84"
    val newPath = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84/ip1"
    val newFile = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84/ip1/as1"
    val tmpPathII = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b8411"
    val tmpPathIII = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84121"
    val tmpPathV = "file:////tmp/indexservertmp/a885a111-439f-4b91-ad81-f0bd48164b84121V"
    FileFactory.createDirectoryAndSetPermission(tmpPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createDirectoryAndSetPermission(tmpPathII,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createDirectoryAndSetPermission(tmpPathIII,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createDirectoryAndSetPermission(newPath,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createNewFile(newFile)
    Thread.sleep(5000)
    val age = System.currentTimeMillis() - 3000
    FileFactory.createDirectoryAndSetPermission(tmpPathV,
      new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    CarbonUtil.agingTempFolderForIndexServer(age)
    assert(!FileFactory.isFileExist(tmpPath))
    // scalastyle:off println
    System.out.println(folderPath)
    // scalastyle:on println
    assert(FileFactory.isFileExist(folderPath))
    assert(FileFactory.isFileExist(tmpPathV))
  }
}
