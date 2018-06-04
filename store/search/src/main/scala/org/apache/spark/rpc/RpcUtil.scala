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

package org.apache.spark.rpc

import org.apache.spark.{SPARK_VERSION, SecurityManager, SparkConf}
import org.apache.spark.util.Utils

object RpcUtil {

  def getRpcEnvConfig(conf: SparkConf,
                      name: String,
                      bindAddress: String,
                      advertiseAddress: String,
                      port: Int,
                      securityManager: SecurityManager,
                      clientMode: Boolean): RpcEnvConfig = {
    val className = "org.apache.spark.rpc.RpcEnvConfig"
    if (SPARK_VERSION.startsWith("2.1") || SPARK_VERSION.startsWith("2.2")) {
      createObject(className, name, bindAddress,
        advertiseAddress, port.asInstanceOf[Object],
        securityManager, clientMode.asInstanceOf[Object])._1.asInstanceOf[RpcEnvConfig]
    } else if (SPARK_VERSION.startsWith("2.3")) {
      // numUsableCores if it is 0 then spark will consider the available CPUs on the host.
      val numUsableCores: Int = 0
      createObject(className, name, bindAddress,
        advertiseAddress, port.asInstanceOf[Object],
        securityManager, numUsableCores.asInstanceOf[Object],
        clientMode.asInstanceOf[Object])._1.asInstanceOf[RpcEnvConfig]
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

}
