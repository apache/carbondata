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

package org.apache.carbondata.alluxio.util

import alluxio.cli.fs.FileSystemShell
import alluxio.security.LoginUserTestUtils
import alluxio.security.authentication.AuthenticatedClientUser
import alluxio.{Configuration, Constants, PropertyKey}
import java.util.{HashMap, Map}

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class AlluxioUtilTest extends QueryTest with BeforeAndAfterAll {

    val localAlluxioCluster = {
        AuthenticatedClientUser.remove()
        LoginUserTestUtils.resetLoginUser()
        val localAlluxioCluster = new alluxio.master.LocalAlluxioCluster(2);
        localAlluxioCluster.initConfiguration()
        import scala.collection.JavaConversions._
        val mConfiguration: Map[PropertyKey, String] = new HashMap[PropertyKey, String]
        val SIZE_BYTES: Int = Constants.MB * 16
        mConfiguration.put(PropertyKey.WORKER_MEMORY_SIZE, SIZE_BYTES.toString)
        mConfiguration.put(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES.toString)
        mConfiguration.put(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.MAX_VALUE.toString)
        // TODO: remove the webapp, but need Alluxio community support it in the future.
        mConfiguration.put(PropertyKey.WEB_RESOURCES, resourcesPath + "/webapp")
        for (entry <- mConfiguration.entrySet()) {
            Configuration.set(entry.getKey, entry.getValue)
        }
        Configuration.validate()
        localAlluxioCluster.start()
        localAlluxioCluster
    }

    var fileSystemShell: FileSystemShell = null

    override protected def beforeAll(): Unit = {
        fileSystemShell = new FileSystemShell()
    }

    override protected def afterAll(): Unit = {
        if (null != fileSystemShell) {
            fileSystemShell.close()
        }
    }
}
